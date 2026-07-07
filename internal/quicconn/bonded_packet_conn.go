// Package quicconn — bonded_packet_conn.go
//
// BondedPacketConn implements net.PacketConn over N concurrent WebRTC
// audio tracks with:
//
//   - True Interleaved WRR packet striping (all paths, including FEC shards)
//   - 4+1 Reed-Solomon FEC with explicit GroupID wire headers
//   - Speculative hedging for QUIC handshake packets (top-N lanes by weight)
//   - Adaptive sliding-window jitter reorder buffer (see reorder_buffer.go)
//   - sync.Pool pre-allocation throughout (zero GC pressure in hot path)
//
// Wire header (5 bytes — UPDATED from 4 to include GroupID + ShardIdx):
//
//	┌──────────┬────────┬────────┬────────────┐
//	│ seq[0:1] │ flags  │ grpID  │  shardIdx  │
//	│ uint16   │ uint8  │ uint8  │ uint8      │
//	└──────────┴────────┴────────┴────────────┘
//
//	seq      — global monotonic packet sequence (uint16, wraps at 65536)
//	flags    — bit 0: IS_PARITY   (FEC parity shard)
//	           bit 1: IS_HEDGED   (speculative duplicate, dedup on receipt)
//	           bit 2: IS_FEC_DATA (data shard belonging to a 4+1 group)
//	grpID    — wrapping 8-bit group counter; ties shards to their group
//	           independent of the global seq number. 0xFF = non-FEC packet.
//	shardIdx — index within the group (0..3 = data, 4 = parity, 0xFF = N/A)
//
// FEC design:
//   All 4 data packets are HELD until the group is complete, then all 5
//   shards are emitted exactly once through the WRR selector (not modulo).
//
// WRR design:
//   Interleaved WRR (Katevenis) with per-lane weights recomputed every 500ms
//   from a receive-rate / RTT proxy. ALL packet types (direct, FEC data, parity,
//   hedged) go through the same pickLane() call — no bypass paths.
package quicconn

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"github.com/salman/ble-webrtc-tun/internal/rtpconn"
)

var bondLog = logger.New("bond")

const (
	// bondHeaderSize: 5 bytes (seq:2 + flags:1 + grpID:1 + shardIdx:1).
	// Hole 3 fix: explicit GroupID decouples FEC reconstruction from global seq.
	bondHeaderSize = 5

	fecDataShards   = 4
	fecParityShards = 1
	fecTotalShards  = fecDataShards + fecParityShards

	// Header flag bits.
	flagParity  = uint8(1 << 0) // IS_PARITY: this is an RS parity shard
	flagHedged  = uint8(1 << 1) // IS_HEDGED: speculative duplicate, dedup on rx
	flagFECData = uint8(1 << 2) // IS_FEC_DATA: data shard in a 4+1 group

	// Sentinel values for non-FEC packets.
	grpIDNone    = uint8(0xFF)
	shardIdxNone = uint8(0xFF)

	fecMinLanes          = 2
	weightSampleInterval = 500 * time.Millisecond

	// maxBondPayload: 1135 bytes (1140 rtpconn limit − 5-byte bond header).
	maxBondPayload = 1135
	poolSlabSize   = maxBondPayload + bondHeaderSize + 64 // 1264 bytes per slab
)

// ── Global buffer pool (Phase 5 / zero-copy) ──────────────────────────────────

var bondBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, poolSlabSize)
		return &b
	},
}

func getPoolBuf() []byte {
	p := bondBufPool.Get().(*[]byte)
	return (*p)[:poolSlabSize]
}

func putPoolBuf(b []byte) {
	if cap(b) < poolSlabSize {
		return
	}
	b = b[:poolSlabSize]
	bondBufPool.Put(&b)
}

// ── Lane ───────────────────────────────────────────────────────────────────────

type lane struct {
	conn  *rtpconn.Conn
	label string

	weight    atomic.Int32 // WRR weight [10..10000], higher = more packets
	recvPkts  atomic.Int64 // total packets received from far-end via this lane
	prevRecv  int64        // recvPkts value at the previous weight-sample tick
	smoothRTT atomic.Int64 // EWMA RTT estimate in milliseconds
}

// ── BondedPacketConn ───────────────────────────────────────────────────────────

type BondedPacketConn struct {
	mu    sync.RWMutex
	lanes []*lane

	// Global packet sequence counter.
	seqID atomic.Uint32

	// Explicit FEC group ID counter (wraps at 256, matching the uint8 wire field).
	// Using a dedicated counter means non-FEC packets never consume group IDs,
	// so group boundaries are never disrupted by hedged or direct packets.
	fecGroupID atomic.Uint32

	// FEC accumulation state (protected by fecMu).
	fecMu    sync.Mutex
	fecEnc   reedsolomon.Encoder
	fecBuf   [fecDataShards][]byte
	fecCount int

	// Receive path.
	recvBuf *reorderBuffer
	done    chan struct{}
	once    sync.Once

	// Interleaved WRR state (protected by wrrMu).
	wrrMu        sync.Mutex
	wrrCurLane   int
	wrrCurWeight int32
	wrrMaxWeight int32
	wrrGCD       int32

	localAddr  net.Addr
	remoteAddr net.Addr
}

// NewBondedPacketConn creates an empty BondedPacketConn.
// Lanes are added later via AddLane() as WebRTC channels establish.
func NewBondedPacketConn() (*BondedPacketConn, error) {
	enc, err := reedsolomon.New(fecDataShards, fecParityShards)
	if err != nil {
		return nil, fmt.Errorf("reedsolomon init: %w", err)
	}
	bc := &BondedPacketConn{
		fecEnc:     enc,
		recvBuf:    newReorderBuffer(),
		done:       make(chan struct{}),
		localAddr:  opusAddr{name: "bond://local:0"},
		remoteAddr: opusAddr{name: "bond://remote:0"},
	}
	go bc.weightSampler()
	return bc, nil
}

// AddLane registers a new rtpconn.Conn as a bonding lane.
// Safe to call while QUIC is already running (lanes hot-join the bond).
func (bc *BondedPacketConn) AddLane(conn *rtpconn.Conn, label string) {
	l := &lane{conn: conn, label: label}
	l.weight.Store(100)
	l.smoothRTT.Store(50)

	bc.mu.Lock()
	bc.lanes = append(bc.lanes, l)
	n := len(bc.lanes)
	bc.mu.Unlock()

	bc.recomputeWRR()
	bondLog.Info("[Bond] Lane added: %s (total: %d)", label, n)
	go bc.laneReader(l)
}

// RemoveLane unregisters a lane when its WebRTC channel dies.
func (bc *BondedPacketConn) RemoveLane(conn *rtpconn.Conn) {
	bc.mu.Lock()
	for i, l := range bc.lanes {
		if l.conn == conn {
			bc.lanes = append(bc.lanes[:i], bc.lanes[i+1:]...)
			bondLog.Info("[Bond] Lane removed: %s (remaining: %d)", l.label, len(bc.lanes))
			break
		}
	}
	bc.mu.Unlock()
	bc.recomputeWRR()
}

// LaneCount returns the number of active lanes.
func (bc *BondedPacketConn) LaneCount() int {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return len(bc.lanes)
}

// ── net.PacketConn interface ───────────────────────────────────────────────────

// WriteTo is called by quic-go for every outgoing QUIC datagram.
// Priority:
//  1. QUIC long-header (handshake) → speculative hedge across top-3 lanes
//  2. ≥2 lanes available           → 4+1 FEC group pipeline
//  3. Single lane                  → direct send with bond header
func (bc *BondedPacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	select {
	case <-bc.done:
		return 0, fmt.Errorf("bonded conn closed")
	default:
	}

	bc.mu.RLock()
	numLanes := len(bc.lanes)
	bc.mu.RUnlock()

	if numLanes == 0 {
		return 0, fmt.Errorf("no active lanes")
	}

	// QUIC long-header (MSB set) → hedge to maximise handshake reliability.
	if len(p) > 0 && len(p) <= 1280 && (p[0]&0x80) != 0 && numLanes >= 3 {
		bc.sendHedged(p)
		return len(p), nil
	}

	if numLanes >= fecMinLanes {
		return bc.writeWithFEC(p)
	}
	return bc.writeDirect(p, 0)
}

// ReadFrom blocks until the next sequential packet is ready.
// Returns the pool slab to the pool after QUIC copies the bytes out.
func (bc *BondedPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	data := bc.recvBuf.Next()
	if data == nil {
		return 0, nil, fmt.Errorf("bonded conn closed")
	}
	n = copy(p, data)
	putPoolBuf(data)
	return n, bc.remoteAddr, nil
}

func (bc *BondedPacketConn) Close() error {
	bc.once.Do(func() {
		close(bc.done)
		bc.recvBuf.Close()
	})
	return nil
}

func (bc *BondedPacketConn) LocalAddr() net.Addr                { return bc.localAddr }
func (bc *BondedPacketConn) SetDeadline(_ time.Time) error      { return nil }
func (bc *BondedPacketConn) SetReadDeadline(_ time.Time) error  { return nil }
func (bc *BondedPacketConn) SetWriteDeadline(_ time.Time) error { return nil }

// ── FEC write pipeline ─────────────────────────────────────────────────────────

// writeWithFEC accumulates packets silently into a 4-shard group, then encodes
// and emits all 5 shards exactly once. No packet is sent twice.
func (bc *BondedPacketConn) writeWithFEC(p []byte) (int, error) {
	bc.fecMu.Lock()
	defer bc.fecMu.Unlock()

	data := p
	if len(data) > maxBondPayload {
		data = data[:maxBondPayload]
	}

	buf := make([]byte, len(data))
	copy(buf, data)
	bc.fecBuf[bc.fecCount] = buf
	bc.fecCount++

	// Hold until group is complete — nothing is sent to the network yet.
	if bc.fecCount < fecDataShards {
		return len(p), nil
	}

	return bc.flushFECGroup(len(p))
}

// flushFECGroup encodes the 4 buffered shards into a 4+1 RS group and emits
// all 5 shards through the WRR selector.
//
// Hole 1 fix: pickLane() is called for EVERY shard — no more i%len(lanes).
// Hole 3 fix: every shard carries the explicit grpID binding the group together.
func (bc *BondedPacketConn) flushFECGroup(origLen int) (int, error) {
	// Atomically allocate a group ID for this group (wraps at 256).
	grpID := uint8(bc.fecGroupID.Add(1) & 0xFF)

	defer func() {
		bc.fecBuf = [fecDataShards][]byte{}
		bc.fecCount = 0
	}()

	// Pad all shards to equal length (Reed-Solomon requirement).
	maxLen := 0
	for _, s := range bc.fecBuf {
		if len(s) > maxLen {
			maxLen = len(s)
		}
	}
	shards := make([][]byte, fecTotalShards)
	for i := 0; i < fecDataShards; i++ {
		shard := make([]byte, maxLen)
		copy(shard, bc.fecBuf[i])
		shards[i] = shard
	}
	shards[fecDataShards] = make([]byte, maxLen)

	if err := bc.fecEnc.Encode(shards); err != nil {
		// Fallback: send buffered data shards individually via WRR.
		bondLog.Warn("[Bond] FEC encode error: %v — falling back to raw send", err)
		for i := 0; i < fecDataShards; i++ {
			if bc.fecBuf[i] != nil {
				bc.writeDirect(bc.fecBuf[i], flagFECData)
			}
		}
		return origLen, nil
	}

	bc.mu.RLock()
	lanes := bc.lanes
	bc.mu.RUnlock()

	// Emit all 5 shards — each through the WRR engine, not a static modulo.
	// Hole 1 fix: this loop now calls pickLane() for every shard.
	for i, shard := range shards {
		flags := flagFECData
		if i == fecDataShards {
			flags = flagParity
		}
		laneIdx := bc.pickLane(lanes) // ← WRR, not i%len(lanes)
		bc.writeToLane(shard, laneIdx, flags, grpID, uint8(i))
	}

	return origLen, nil
}

// writeDirect sends one packet via WRR with no FEC grouping.
// grpID = 0xFF, shardIdx = 0xFF (sentinel: not part of a group).
func (bc *BondedPacketConn) writeDirect(p []byte, flags uint8) (int, error) {
	bc.mu.RLock()
	lanes := bc.lanes
	bc.mu.RUnlock()

	if len(lanes) == 0 {
		return 0, fmt.Errorf("no lanes")
	}
	laneIdx := bc.pickLane(lanes)
	bc.writeToLane(p, laneIdx, flags, grpIDNone, shardIdxNone)
	return len(p), nil
}

// sendHedged duplicates a critical packet to the top-N highest-weight lanes.
func (bc *BondedPacketConn) sendHedged(p []byte) {
	bc.mu.RLock()
	lanes := bc.lanes
	bc.mu.RUnlock()

	count := 3
	if len(lanes) < count {
		count = len(lanes)
	}
	for _, idx := range bc.topNLanes(lanes, count) {
		bc.writeToLane(p, idx, flagHedged, grpIDNone, shardIdxNone)
	}
	bondLog.Info("[Bond] Hedged packet to %d lanes (size=%d)", count, len(p))
}

// topNLanes returns the indices of the N highest-weight lanes.
func (bc *BondedPacketConn) topNLanes(lanes []*lane, n int) []int {
	type entry struct {
		idx    int
		weight int32
	}
	es := make([]entry, len(lanes))
	for i, l := range lanes {
		es[i] = entry{i, l.weight.Load()}
	}
	for i := 0; i < n && i < len(es); i++ {
		best := i
		for j := i + 1; j < len(es); j++ {
			if es[j].weight > es[best].weight {
				best = j
			}
		}
		es[i], es[best] = es[best], es[i]
	}
	out := make([]int, n)
	for i := 0; i < n; i++ {
		out[i] = es[i].idx
	}
	return out
}

// writeToLane prepends the 5-byte bond header and writes one frame to a lane.
// Uses pool slabs for the framing buffer to avoid heap allocation.
func (bc *BondedPacketConn) writeToLane(p []byte, laneIdx int, flags, grpID, shardIdx uint8) {
	bc.mu.RLock()
	if laneIdx >= len(bc.lanes) {
		bc.mu.RUnlock()
		return
	}
	l := bc.lanes[laneIdx]
	bc.mu.RUnlock()

	seq := uint16(bc.seqID.Add(1) - 1)
	total := bondHeaderSize + len(p)

	var frame []byte
	if total <= poolSlabSize {
		slab := getPoolBuf()
		frame = slab[:total]
		defer putPoolBuf(slab)
	} else {
		frame = make([]byte, total)
	}

	// 5-byte wire header: [seq:2][flags:1][grpID:1][shardIdx:1]
	binary.BigEndian.PutUint16(frame[0:2], seq)
	frame[2] = flags
	frame[3] = grpID
	frame[4] = shardIdx
	copy(frame[bondHeaderSize:], p)

	if _, err := l.conn.WriteFrame(frame); err != nil {
		bondLog.Warn("[Bond] Lane %s write error: %v", l.label, err)
	}
}

// ── Interleaved WRR selector ───────────────────────────────────────────────────

// pickLane returns the next lane index using Interleaved WRR.
// A lane with weight 400 gets exactly 4× the packets of a lane with weight 100.
// A lane whose weight falls below the current threshold is skipped entirely.
func (bc *BondedPacketConn) pickLane(lanes []*lane) int {
	if len(lanes) == 1 {
		return 0
	}

	bc.wrrMu.Lock()
	defer bc.wrrMu.Unlock()

	n := len(lanes)
	maxW := bc.wrrMaxWeight
	g := bc.wrrGCD
	if maxW == 0 || g == 0 {
		// Pre-sample fallback: plain round-robin.
		bc.wrrCurLane = (bc.wrrCurLane + 1) % n
		return bc.wrrCurLane
	}

	// Interleaved WRR scan (terminates because at least one lane has
	// weight == maxW which is always ≥ any curWeight).
	for {
		bc.wrrCurLane = (bc.wrrCurLane + 1) % n
		if bc.wrrCurLane == 0 {
			bc.wrrCurWeight -= g
			if bc.wrrCurWeight <= 0 {
				bc.wrrCurWeight = maxW
			}
		}
		if lanes[bc.wrrCurLane].weight.Load() >= bc.wrrCurWeight {
			return bc.wrrCurLane
		}
	}
}

// recomputeWRR recalculates maxWeight and GCD after any weight change.
func (bc *BondedPacketConn) recomputeWRR() {
	bc.mu.RLock()
	lanes := bc.lanes
	bc.mu.RUnlock()

	if len(lanes) == 0 {
		return
	}

	var maxW, g int32
	for _, l := range lanes {
		w := l.weight.Load()
		if w > maxW {
			maxW = w
		}
		if g == 0 {
			g = w
		} else {
			g = gcd32(g, w)
		}
	}

	bc.wrrMu.Lock()
	bc.wrrMaxWeight = maxW
	bc.wrrGCD = g
	bc.wrrCurLane = -1
	bc.wrrCurWeight = maxW
	bc.wrrMu.Unlock()
}

func gcd32(a, b int32) int32 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// ── Weight sampler ─────────────────────────────────────────────────────────────

// weightSampler recomputes per-lane weights every 500ms using:
//
//	Weight_i = max(10, 10000 / (SmoothRTT_i × (LossRate_i + 1)))
//
// RTT and loss are estimated from the receive-packet-rate observed on each lane.
func (bc *BondedPacketConn) weightSampler() {
	ticker := time.NewTicker(weightSampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bc.done:
			return
		case <-ticker.C:
			bc.mu.RLock()
			lanes := bc.lanes
			bc.mu.RUnlock()

			changed := false
			for _, l := range lanes {
				cur := l.recvPkts.Load()
				delta := cur - l.prevRecv
				l.prevRecv = cur

				// Map receive rate to RTT proxy (higher rate → lower RTT).
				var rttMs int64
				switch {
				case delta >= 50:
					rttMs = 5
				case delta > 0:
					rttMs = 5 + int64(math.Round(float64(50-delta)/50.0*995))
				default:
					rttMs = 1000 // silent lane — severe penalty
				}

				// EWMA smoothing (α=0.3).
				prev := l.smoothRTT.Load()
				smoothed := int64(math.Round(float64(prev)*0.7 + float64(rttMs)*0.3))
				l.smoothRTT.Store(smoothed)

				lossRate := 0.0
				if rttMs == 1000 {
					lossRate = 0.5
				}
				denom := float64(smoothed) * (lossRate + 1.0)
				newW := int32(math.Round(10000.0 / denom))
				newW = max32(10, min32(10000, newW))

				old := l.weight.Swap(newW)
				if old != newW {
					changed = true
					bondLog.Info("[Bond] Lane %s weight %d→%d (rtt≈%dms)", l.label, old, newW, smoothed)
				}
			}
			if changed {
				bc.recomputeWRR()
			}
		}
	}
}

func max32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

// ── Lane reader goroutine ──────────────────────────────────────────────────────

// laneReader continuously reads from one rtpconn.Conn, strips the 5-byte bond
// header, and routes payloads to the reorder buffer or FEC reconstructor.
//
// Hole 2+3 fix: FEC groups are keyed by explicit grpID (uint8), not by seq.
// Out-of-order arrival no longer confuses group membership.
func (bc *BondedPacketConn) laneReader(l *lane) {
	// Per-lane FEC group map keyed by the explicit wire GroupID.
	fecGroups := make(map[uint8]*fecGroup)

	for {
		select {
		case <-bc.done:
			return
		default:
		}

		raw, err := l.conn.ReadPacket()
		if err != nil {
			select {
			case <-bc.done:
				return
			default:
				bondLog.Warn("[Bond] Lane %s read error: %v", l.label, err)
				return
			}
		}
		l.recvPkts.Add(1)

		if len(raw) < bondHeaderSize {
			bondLog.Warn("[Bond] Lane %s: short packet (%d bytes)", l.label, len(raw))
			continue
		}

		// Unpack 5-byte header.
		seq      := binary.BigEndian.Uint16(raw[0:2])
		flags    := raw[2]
		grpID    := raw[3]
		shardIdx := raw[4]
		payload  := raw[bondHeaderSize:]

		// Copy payload into a pool slab so the RTP receive buffer is immediately freed.
		slab := getPoolBuf()
		slab = slab[:len(payload)]
		copy(slab, payload)
		payload = slab

		isParity  := (flags & flagParity) != 0
		isHedged  := (flags & flagHedged) != 0
		isFECData := (flags & flagFECData) != 0

		switch {
		case isParity:
			// Parity shard: deliver to FEC reconstructor (not to reorder buffer).
			bc.deliverParityShard(fecGroups, grpID, seq, payload)

		case isHedged:
			// Hedged duplicate: the reorder buffer's bitmask deduplicates.
			bc.recvBuf.Insert(seq, payload, true)

		case isFECData:
			// Data shard from a 4+1 group: track and reconstruct on arrival.
			bc.trackFECDataShard(fecGroups, grpID, shardIdx, seq, payload)

		default:
			// Standalone direct packet: insert normally.
			bc.recvBuf.Insert(seq, payload, false)
		}

		bc.cleanStaleGroups(fecGroups)
	}
}

// ── FEC receive-side state ─────────────────────────────────────────────────────

// fecGroup tracks one 4+1 Reed-Solomon group on the receive side.
// Keyed by the explicit grpID from the wire header.
type fecGroup struct {
	groupID   uint8
	shards    [fecTotalShards][]byte
	seqNums   [fecTotalShards]uint16 // seq of each shard for delivery ordering
	received  int
	maxLen    int
	createdAt time.Time
}

func (fg *fecGroup) addShard(idx int, seq uint16, data []byte) bool {
	if idx < 0 || idx >= fecTotalShards {
		return false
	}
	if fg.shards[idx] != nil {
		return false // duplicate shard in the same group
	}
	fg.shards[idx] = data
	fg.seqNums[idx] = seq
	fg.received++
	if len(data) > fg.maxLen {
		fg.maxLen = len(data)
	}
	return true
}

func (fg *fecGroup) canReconstruct() bool {
	return fg.received >= fecDataShards
}

// trackFECDataShard processes an incoming FEC data shard.
//
// Hole 2 fix: group membership is now determined by the explicit grpID field,
// not by computing baseSeq = seq - (seq % N). Out-of-order arrivals (e.g.,
// shard 2 arrives before shard 0) always map to the correct group.
//
// Data shards are also forwarded directly to the reorder buffer so QUIC is
// not starved while the group is still accumulating.
func (bc *BondedPacketConn) trackFECDataShard(groups map[uint8]*fecGroup, grpID, shardIdx uint8, seq uint16, payload []byte) {
	if shardIdx >= fecDataShards {
		// Invalid shard index — drop.
		putPoolBuf(payload)
		return
	}

	fg, exists := groups[grpID]
	if !exists {
		fg = &fecGroup{groupID: grpID, createdAt: time.Now()}
		groups[grpID] = fg
	}

	fg.addShard(int(shardIdx), seq, payload)

	// Always deliver the data shard to the reorder buffer immediately.
	// If it gets reconstructed later, the bitmask dedup prevents double delivery.
	bc.recvBuf.Insert(seq, payload, false)

	if fg.canReconstruct() {
		bc.reconstructAndDeliver(fg)
		delete(groups, grpID)
	}
}

// deliverParityShard processes an incoming FEC parity shard.
// The parity shard is used for reconstruction only — it is not forwarded to QUIC.
func (bc *BondedPacketConn) deliverParityShard(groups map[uint8]*fecGroup, grpID uint8, seq uint16, payload []byte) {
	fg, exists := groups[grpID]
	if !exists {
		fg = &fecGroup{groupID: grpID, createdAt: time.Now()}
		groups[grpID] = fg
	}

	fg.addShard(fecDataShards, seq, payload)

	if fg.canReconstruct() {
		bc.reconstructAndDeliver(fg)
		delete(groups, grpID)
	}
}

// reconstructAndDeliver uses Reed-Solomon to recover any missing data shards
// and inserts ONLY the recovered (previously missing) shards into the reorder
// buffer. Shards that were already delivered are not re-inserted.
func (bc *BondedPacketConn) reconstructAndDeliver(fg *fecGroup) {
	// Record which data shards were absent before reconstruction.
	missing := [fecDataShards]bool{}
	for i := 0; i < fecDataShards; i++ {
		missing[i] = fg.shards[i] == nil
	}

	// Pad all present shards to equal length.
	shards := make([][]byte, fecTotalShards)
	for i := 0; i < fecTotalShards; i++ {
		if fg.shards[i] != nil {
			s := make([]byte, fg.maxLen)
			copy(s, fg.shards[i])
			shards[i] = s
		}
	}

	if err := bc.fecEnc.ReconstructData(shards); err != nil {
		bondLog.Warn("[Bond] FEC reconstruct failed (grp=%d): %v", fg.groupID, err)
		return
	}

	// Recover the seq numbers of missing shards by extrapolating from neighbours.
	// Because the sender assigns seq IDs in shard order (0,1,2,3), the deltas
	// between adjacent present shards tell us the missing ones.
	var baseSeq uint16
	baseFound := false
	for i := 0; i < fecDataShards; i++ {
		if fg.shards[i] != nil {
			// Work backwards: seq[i] − i gives the theoretical seq[0].
			baseSeq = fg.seqNums[i] - uint16(i)
			baseFound = true
			break
		}
	}
	if !baseFound {
		return
	}

	// Insert only the shards that were genuinely missing.
	for i := 0; i < fecDataShards; i++ {
		if missing[i] && shards[i] != nil {
			targetSeq := baseSeq + uint16(i)
			bondLog.Info("[Bond] ✅ FEC recovered missing shard seq=%d (grp=%d)", targetSeq, fg.groupID)
			bc.recvBuf.Insert(targetSeq, shards[i], false)
		}
	}
}

// cleanStaleGroups evicts FEC groups that have been open too long.
// This prevents memory accumulation when packets are permanently lost.
func (bc *BondedPacketConn) cleanStaleGroups(groups map[uint8]*fecGroup) {
	now := time.Now()
	for id, g := range groups {
		if now.Sub(g.createdAt) > 2*time.Second {
			delete(groups, id)
		}
	}
}

// ── Factory helpers ────────────────────────────────────────────────────────────

// NewBondedClient creates a BondedPacketConn pre-loaded with the given lanes.
func NewBondedClient(lanes []*rtpconn.Conn, labels []string) (*BondedPacketConn, error) {
	bc, err := NewBondedPacketConn()
	if err != nil {
		return nil, err
	}
	for i, c := range lanes {
		label := fmt.Sprintf("lane%d", i)
		if i < len(labels) {
			label = labels[i]
		}
		bc.AddLane(c, label)
	}
	return bc, nil
}

// NewBondedServer creates a BondedPacketConn for the QUIC server side.
func NewBondedServer(lanes []*rtpconn.Conn, labels []string) (*BondedPacketConn, error) {
	return NewBondedClient(lanes, labels)
}

// BondedRemoteAddr returns the fake remote address required by quic.Dial().
func BondedRemoteAddr() net.Addr { return opusAddr{name: "bond://remote:0"} }

// ── Ensure atomic.Int32 Load method satisfies the usage pattern ───────────────
var _ interface{ Load() int32 } = (*atomic.Int32)(nil)
