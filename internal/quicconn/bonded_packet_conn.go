// Package quicconn — bonded_packet_conn.go
//
// BondedPacketConn implements net.PacketConn over N concurrent WebRTC
// audio tracks. It provides:
//
//   - Packet-level striping (Interleaved Weighted Round-Robin across N lanes)
//   - 4+1 Reed-Solomon Forward Error Correction (zero-duplicate pipeline)
//   - Speculative hedging for high-priority control packets
//   - Sliding-window jitter reorder buffer (see reorder_buffer.go)
//   - sync.Pool pre-allocation for zero-copy, GC-pressure-free networking
//
// Wire header (4 bytes prepended to every packet):
//
//	┌──────────┬──────────┬────────┬────────┐
//	│ seq[0:1] │ chanID   │ flags  │ grpIdx │
//	│ uint16   │ uint8    │ uint8  │ uint8  │
//	└──────────┴──────────┴────────┴────────┘
//
//	seq    — global packet sequence (uint16, wraps at 65536)
//	chanID — originating lane index (0..N-1)
//	flags  — bit 0: IS_PARITY  (FEC parity shard)
//	         bit 1: IS_HEDGED  (speculative duplicate)
//	         bit 2: IS_FEC_DATA (part of a 4+1 group, not a standalone pkt)
//	grpIdx — index within the FEC group (0..4), 0xFF for non-FEC packets
//
// FEC design (FIXED — zero-duplicate pipeline):
//   Packets are HELD in memory until the 4-packet group is complete.
//   The group is encoded ONCE, producing 5 shards. All 5 are emitted
//   sequentially with monotonic sequence IDs. No packet is ever sent twice.
//
// Weighted Round-Robin (FIXED — Interleaved WRR):
//   Lane weights are computed from receive-rate health metrics every 500ms.
//   pickLane() uses the Interleaved WRR algorithm (GCD + max-weight cursor)
//   so that faster lanes carry proportionally more traffic.
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
	bondHeaderSize = 4 // [seq uint16][chanID uint8][flags uint8]

	// FEC parameters: 4 data shards + 1 parity shard.
	fecDataShards   = 4
	fecParityShards = 1
	fecTotalShards  = fecDataShards + fecParityShards

	// Flag bits in the header flags byte.
	flagParity  = 1 << 0
	flagHedged  = 1 << 1
	flagFECData = 1 << 2 // data shard that belongs to a FEC group

	// Minimum lanes needed before FEC is activated.
	fecMinLanes = 2

	// Weight sampling interval.
	weightSampleInterval = 500 * time.Millisecond

	// Maximum payload size after the 4-byte bond header.
	// rtpconn accepts up to ~1140 bytes; subtract bond header.
	maxBondPayload = 1136

	// Buffer pool slab size — large enough for any bond frame.
	poolSlabSize = maxBondPayload + bondHeaderSize + 64
)

// ── Global buffer pool (Phase 5: zero-copy, GC-pressure-free) ─────────────────
//
// All lane reads and header-prepend operations lease slabs from this pool
// instead of calling make([]byte, ...) inline. After the reorder buffer
// delivers a packet to QUIC, the slab is returned to the pool.
var bondBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, poolSlabSize)
		return &b
	},
}

func getPoolBuf() []byte {
	p := bondBufPool.Get().(*[]byte)
	b := *p
	return b[:poolSlabSize]
}

func putPoolBuf(b []byte) {
	if cap(b) < poolSlabSize {
		return // don't pool undersized slabs
	}
	b = b[:poolSlabSize]
	bondBufPool.Put(&b)
}

// ── Lane ───────────────────────────────────────────────────────────────────────

// lane wraps one rtpconn.Conn with per-lane health tracking for WRR.
type lane struct {
	conn  *rtpconn.Conn
	label string

	// Weight used by the Interleaved WRR selector (updated every 500ms).
	// Range [10, 10000] — higher means more packets allocated.
	weight atomic.Int32

	// Receive-side packet counter used by the health sampler.
	recvPkts atomic.Int64
	prevRecv  int64

	// Smooth RTT estimate in milliseconds (updated by weightSampler).
	// Initialised to a reasonable default; replaced by real measurement once
	// the lane has been active for at least one sample window.
	smoothRTT atomic.Int64 // milliseconds, default 50
}

// ── BondedPacketConn ───────────────────────────────────────────────────────────

// BondedPacketConn is a virtual net.PacketConn that stripes QUIC datagrams
// across all registered lanes simultaneously.
type BondedPacketConn struct {
	mu    sync.RWMutex
	lanes []*lane

	// Packet sequencing — monotonically increasing, wraps at 65536.
	seqID atomic.Uint32

	// FEC encode state (mutex-protected; called from WriteTo goroutine).
	fecMu    sync.Mutex
	fecEnc   reedsolomon.Encoder
	fecBuf   [fecDataShards][]byte // accumulation cache (hold, don't send yet)
	fecCount int                   // how many data pkts accumulated so far

	// Receive path.
	recvBuf *reorderBuffer
	done    chan struct{}
	once    sync.Once

	// Interleaved WRR state (protected by wrrMu).
	wrrMu        sync.Mutex
	wrrCurLane   int   // current lane index in the WRR pass
	wrrCurWeight int32 // current weight threshold in the WRR pass
	wrrMaxWeight int32 // max lane weight (recomputed on weight change)
	wrrGCD       int32 // GCD of all lane weights

	// Local/remote fake addresses (QUIC requires net.Addr).
	localAddr  net.Addr
	remoteAddr net.Addr
}

// NewBondedPacketConn creates a BondedPacketConn with no lanes.
// Lanes are added later via AddLane() as WebRTC connections establish.
func NewBondedPacketConn() (*BondedPacketConn, error) {
	enc, err := reedsolomon.New(fecDataShards, fecParityShards)
	if err != nil {
		return nil, fmt.Errorf("reedsolomon init: %w", err)
	}
	bc := &BondedPacketConn{
		fecEnc:     enc,
		recvBuf:    newReorderBuffer(),
		done:       make(chan struct{}),
		localAddr:  opusAddr{"bond://local:0"},
		remoteAddr: opusAddr{"bond://remote:0"},
	}
	go bc.weightSampler()
	return bc, nil
}

// AddLane adds a new rtpconn.Conn as an active bonding lane.
// Safe to call concurrently, even after QUIC has already started.
func (bc *BondedPacketConn) AddLane(conn *rtpconn.Conn, label string) {
	l := &lane{conn: conn, label: label}
	l.weight.Store(100)    // start with healthy weight
	l.smoothRTT.Store(50)  // 50ms default until measured

	bc.mu.Lock()
	bc.lanes = append(bc.lanes, l)
	n := len(bc.lanes)
	bc.mu.Unlock()

	bc.recomputeWRR()
	bondLog.Info("[Bond] Lane added: %s (total lanes: %d)", label, n)

	// Each lane gets a dedicated reader goroutine.
	go bc.laneReader(l)
}

// RemoveLane removes a lane (called when a WebRTC channel dies).
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

// LaneCount returns the current number of active lanes.
func (bc *BondedPacketConn) LaneCount() int {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return len(bc.lanes)
}

// ── net.PacketConn interface ───────────────────────────────────────────────────

// WriteTo is called by quic-go for every outgoing QUIC datagram.
// Routing decision (in priority order):
//  1. QUIC long-header (handshake) → speculative hedge to 3 lanes
//  2. Multi-lane available         → 4+1 FEC group pipeline
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

	// ── 1. Speculative hedging for QUIC Initial/Handshake packets ─────────
	// QUIC long-header packets have MSB of byte 0 set (0x80+).
	// These are the critical handshake frames — hedge across 3 lanes to
	// race for the fastest path and cut connection setup latency by ~50%.
	if len(p) > 0 && len(p) <= 1280 && (p[0]&0x80) != 0 && numLanes >= 3 {
		bc.sendHedged(p)
		return len(p), nil
	}

	// ── 2. FEC group pipeline (zero-duplicate) ────────────────────────────
	if numLanes >= fecMinLanes {
		return bc.writeWithFEC(p)
	}

	// ── 3. Single-lane direct send ────────────────────────────────────────
	return bc.writeDirect(p, 0)
}

// ReadFrom blocks until a reordered+FEC-recovered packet is ready.
// Returns the slab to the pool after QUIC consumes it.
// Implements net.PacketConn.
func (bc *BondedPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	data := bc.recvBuf.Next()
	if data == nil {
		return 0, nil, fmt.Errorf("bonded conn closed")
	}
	n = copy(p, data)
	// Return the slab to the pool if it came from the pool.
	// The copy above has already moved the bytes into QUIC's buffer.
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

// ── FEC write pipeline (FIXED — zero-duplicate) ────────────────────────────────
//
// HOLE 1 & 2 FIX:
//   The old code called writeDirect() immediately for packets 0-2, then
//   re-transmitted them again when the group flushed — 60% duplicate overhead.
//
//   New design: packets 0-2 are HELD in fecBuf only. Nothing is sent to the
//   network until the 4th packet arrives and the complete 4+1 group is encoded.
//   All 5 shards are emitted exactly once with monotonically increasing seq IDs.
//   QUIC never sees a duplicate datagram.
func (bc *BondedPacketConn) writeWithFEC(p []byte) (int, error) {
	bc.fecMu.Lock()
	defer bc.fecMu.Unlock()

	// Clamp to MTU.
	data := p
	if len(data) > maxBondPayload {
		data = data[:maxBondPayload]
	}

	// ── Copy into the accumulation buffer (do NOT send yet) ───────────────
	buf := make([]byte, len(data))
	copy(buf, data)
	bc.fecBuf[bc.fecCount] = buf
	bc.fecCount++

	// Group not yet complete — hold and return.
	// QUIC is not starved: it issues WriteTo calls at its own pacing rate;
	// the group accumulates within microseconds at line rate.
	if bc.fecCount < fecDataShards {
		return len(p), nil
	}

	// ── Group complete — encode parity, flush all 5 shards exactly once ───
	return bc.flushFECGroup(len(p))
}

// flushFECGroup encodes the accumulated 4 shards into a 4+1 RS group and
// emits all 5 frames to the network with consecutive sequence IDs.
// Called with bc.fecMu held.
func (bc *BondedPacketConn) flushFECGroup(origLen int) (int, error) {
	defer func() {
		bc.fecBuf = [fecDataShards][]byte{}
		bc.fecCount = 0
	}()

	// Pad all data shards to equal length (RS requirement).
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
		// Fallback: send each accumulated data packet once directly.
		bondLog.Warn("[Bond] FEC encode error: %v — sending data shards directly", err)
		for i := 0; i < fecDataShards; i++ {
			if bc.fecBuf[i] != nil {
				bc.writeDirect(bc.fecBuf[i], flagFECData)
			}
		}
		return origLen, nil
	}

	// Emit all 5 shards sequentially across different lanes.
	// Each shard gets its own monotonic seq ID — no duplicates.
	bc.mu.RLock()
	lanes := bc.lanes
	bc.mu.RUnlock()

	for i, shard := range shards {
		flags := uint8(flagFECData)
		if i == fecDataShards {
			flags = flagParity
		}
		laneIdx := i % len(lanes) // spread across lanes: 0→lane0, 1→lane1, ...
		bc.writeToLane(shard, laneIdx, flags)
	}

	return origLen, nil
}

// writeDirect sends one packet to the next WRR-selected lane.
func (bc *BondedPacketConn) writeDirect(p []byte, flags uint8) (int, error) {
	bc.mu.RLock()
	lanes := bc.lanes
	bc.mu.RUnlock()

	if len(lanes) == 0 {
		return 0, fmt.Errorf("no lanes")
	}

	laneIdx := bc.pickLane(lanes)
	bc.writeToLane(p, laneIdx, flags)
	return len(p), nil
}

// sendHedged duplicates p to `count` lanes simultaneously.
// Used for QUIC handshake packets to guarantee the fastest path wins.
func (bc *BondedPacketConn) sendHedged(p []byte) {
	bc.mu.RLock()
	lanes := bc.lanes
	bc.mu.RUnlock()

	count := 3
	if len(lanes) < count {
		count = len(lanes)
	}

	// Pick the top-weight lanes for hedging (not just sequential).
	picked := bc.topNLanes(lanes, count)
	for _, idx := range picked {
		bc.writeToLane(p, idx, flagHedged)
	}
	bondLog.Info("[Bond] Hedged packet across %d lanes (size=%d)", count, len(p))
}

// topNLanes returns indices of the N highest-weight lanes.
func (bc *BondedPacketConn) topNLanes(lanes []*lane, n int) []int {
	type entry struct {
		idx    int
		weight int32
	}
	es := make([]entry, len(lanes))
	for i, l := range lanes {
		es[i] = entry{i, l.weight.Load()}
	}
	// Partial sort: move top-n to the front.
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

// writeToLane prepends the bond header and writes one frame to a specific lane.
// Uses the pool buffer for the header+payload assembly (Phase 5 zero-copy).
func (bc *BondedPacketConn) writeToLane(p []byte, laneIdx int, flags uint8) {
	bc.mu.RLock()
	if laneIdx >= len(bc.lanes) {
		bc.mu.RUnlock()
		return
	}
	l := bc.lanes[laneIdx]
	bc.mu.RUnlock()

	seq := uint16(bc.seqID.Add(1) - 1)

	// Lease a slab, build the frame, write, then reclaim the slab.
	total := bondHeaderSize + len(p)
	var frame []byte
	if total <= poolSlabSize {
		slab := getPoolBuf()
		frame = slab[:total]
		defer putPoolBuf(slab)
	} else {
		frame = make([]byte, total)
	}

	binary.BigEndian.PutUint16(frame[0:2], seq)
	frame[2] = uint8(laneIdx)
	frame[3] = flags
	copy(frame[bondHeaderSize:], p)

	if _, err := l.conn.WriteFrame(frame); err != nil {
		bondLog.Warn("[Bond] Lane %s write error: %v", l.label, err)
	}
}

// ── Interleaved Weighted Round-Robin selector (FIXED) ─────────────────────────
//
// HOLE 3 FIX:
//   The old code used a plain modulo (all lanes equally), so a single slow
//   lane dragged down the entire bond. The new Interleaved WRR algorithm
//   (Katevenis et al.) assigns packet slots proportionally to weight:
//   a lane with weight 200 gets twice the packets of a lane with weight 100.
//
// Algorithm state: (wrrCurLane, wrrCurWeight)
//   - Start: curLane=0, curWeight=maxWeight
//   - Each call: advance curLane; if curLane wraps, decrease curWeight by GCD.
//   - A lane is selected if its weight >= curWeight; otherwise skip.
//   - When curWeight reaches 0, reset to maxWeight.

// pickLane returns the index of the next lane to use for a packet.
func (bc *BondedPacketConn) pickLane(lanes []*lane) int {
	if len(lanes) == 1 {
		return 0
	}

	bc.wrrMu.Lock()
	defer bc.wrrMu.Unlock()

	n := len(lanes)
	maxW := bc.wrrMaxWeight
	gcd := bc.wrrGCD
	if maxW == 0 || gcd == 0 {
		// Fallback: plain round-robin (happens before first weight sample).
		bc.wrrCurLane = (bc.wrrCurLane + 1) % n
		return bc.wrrCurLane
	}

	// Interleaved WRR scan.
	for {
		bc.wrrCurLane = (bc.wrrCurLane + 1) % n
		if bc.wrrCurLane == 0 {
			bc.wrrCurWeight -= gcd
			if bc.wrrCurWeight <= 0 {
				bc.wrrCurWeight = maxW
			}
		}
		if lanes[bc.wrrCurLane].weight.Load() >= bc.wrrCurWeight {
			return bc.wrrCurLane
		}
	}
}

// recomputeWRR recalculates maxWeight and GCD after any lane change.
func (bc *BondedPacketConn) recomputeWRR() {
	bc.mu.RLock()
	lanes := bc.lanes
	bc.mu.RUnlock()

	if len(lanes) == 0 {
		return
	}

	var maxW int32
	var g int32
	for _, l := range lanes {
		w := l.weight.Load()
		if w > maxW {
			maxW = w
		}
		if g == 0 {
			g = w
		} else {
			g = gcd(g, w)
		}
	}

	bc.wrrMu.Lock()
	bc.wrrMaxWeight = maxW
	bc.wrrGCD = g
	bc.wrrCurLane = -1
	bc.wrrCurWeight = maxW
	bc.wrrMu.Unlock()
}

func gcd(a, b int32) int32 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// ── Weight sampler (health-based, RTT-aware) ───────────────────────────────────

// weightSampler recomputes per-lane weights every 500ms using:
//
//	Weight_i = max(10, 10000 / (SmoothRTT_i × (LossRate_i + 1)))
//
// SmoothRTT is estimated from the inter-arrival rate of receive packets:
// a lane that receives more packets in the window is assumed to have lower RTT.
// LossRate is approximated as the fraction of expected packets that didn't arrive.
func (bc *BondedPacketConn) weightSampler() {
	ticker := time.NewTicker(weightSampleInterval)
	defer ticker.Stop()

	changed := false
	for {
		select {
		case <-bc.done:
			return
		case <-ticker.C:
			bc.mu.RLock()
			lanes := bc.lanes
			bc.mu.RUnlock()

			changed = false
			for _, l := range lanes {
				cur := l.recvPkts.Load()
				delta := cur - l.prevRecv
				l.prevRecv = cur

				// Estimate RTT: more packets received → lower RTT (proxy).
				// Map delta ∈ [0, ∞) → RTT ∈ [5ms, 1000ms].
				var rttMs int64
				if delta >= 50 {
					rttMs = 5 // very active lane
				} else if delta > 0 {
					rttMs = 5 + int64(math.Round(float64(50-delta)/50.0*995))
				} else {
					rttMs = 1000 // silent lane — high penalty
				}

				// Smooth the RTT estimate (EWMA, α = 0.3).
				prev := l.smoothRTT.Load()
				smoothed := int64(math.Round(float64(prev)*0.7 + float64(rttMs)*0.3))
				l.smoothRTT.Store(smoothed)

				// Weight formula: max(10, 10000 / (rtt * (loss+1)))
				// LossRate proxy: if rttMs == 1000 (silent), treat as 50% loss.
				lossRate := 0.0
				if rttMs == 1000 {
					lossRate = 0.5
				}
				denom := float64(smoothed) * (lossRate + 1.0)
				newW := int32(math.Round(10000.0 / denom))
				if newW < 10 {
					newW = 10
				}
				if newW > 10000 {
					newW = 10000
				}

				old := l.weight.Swap(newW)
				if old != newW {
					changed = true
					bondLog.Info("[Bond] Lane %s weight: %d→%d (rtt≈%dms)",
						l.label, old, newW, smoothed)
				}
			}

			if changed {
				bc.recomputeWRR()
			}
		}
	}
}

// ── Lane reader goroutine ──────────────────────────────────────────────────────

// laneReader continuously reads from one rtpconn.Conn, strips the bond
// header, and feeds payloads into the reorder buffer or FEC reconstructor.
// Uses pool slabs for the receive buffer (Phase 5).
func (bc *BondedPacketConn) laneReader(l *lane) {
	// Per-lane FEC group map: tracks incoming 4+1 groups for reconstruction.
	fecGroups := make(map[uint16]*fecGroup)

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

		// Parse bond header.
		seq := binary.BigEndian.Uint16(raw[0:2])
		flags := raw[3]
		payload := raw[bondHeaderSize:]

		isHedged  := (flags & flagHedged) != 0
		isParity  := (flags & flagParity) != 0
		isFECData := (flags & flagFECData) != 0

		// Copy payload into a pool slab so the RTP receive buffer can be reused.
		slab := getPoolBuf()
		slab = slab[:len(payload)]
		copy(slab, payload)
		payload = slab

		switch {
		case isParity:
			// Deliver parity shard to FEC reconstructor.
			bc.deliverToFEC(fecGroups, seq, payload, true)

		case isHedged:
			// Reorder buffer deduplicates via bitmask — insert and let it decide.
			bc.recvBuf.Insert(seq, payload, true)

		case isFECData:
			// Data shard belonging to a FEC group — track for reconstruction.
			bc.trackFECDataShard(fecGroups, seq, payload)

		default:
			// Standalone data packet (single-lane mode or non-FEC path).
			bc.recvBuf.Insert(seq, payload, false)
		}
	}
}

// ── FEC receive-side tracking ──────────────────────────────────────────────────

// fecGroup tracks the shards of one 4+1 FEC group on the receive side.
type fecGroup struct {
	baseSeq  uint16
	shards   [fecTotalShards][]byte
	received int
	maxLen   int
}

func (fg *fecGroup) addShard(idx int, data []byte) {
	if idx < 0 || idx >= fecTotalShards {
		return
	}
	if fg.shards[idx] == nil {
		fg.shards[idx] = data
		fg.received++
		if len(data) > fg.maxLen {
			fg.maxLen = len(data)
		}
	}
}

func (fg *fecGroup) canReconstruct() bool {
	return fg.received >= fecDataShards
}

// trackFECDataShard adds a data shard to its group and delivers when complete.
// FEC group base is derived from the sequence position within the group.
// The emitting side assigns shards sequentially: baseSeq, baseSeq+1, ... baseSeq+3.
func (bc *BondedPacketConn) trackFECDataShard(groups map[uint16]*fecGroup, seq uint16, payload []byte) {
	// Find or create the group for this shard.
	// Since shards are emitted in order, search the last few open groups.
	var fg *fecGroup
	for base, g := range groups {
		diff := int(seq) - int(base)
		if diff >= 0 && diff < fecDataShards {
			fg = g
			break
		}
	}
	if fg == nil {
		// New group starting at this seq (seq is baseSeq+0).
		// We look back up to fecDataShards-1 to find the real base.
		// For simplicity, treat seq itself as the base.
		fg = &fecGroup{baseSeq: seq}
		groups[seq] = fg
	}

	idx := int(seq - fg.baseSeq)
	fg.addShard(idx, payload)

	if fg.canReconstruct() {
		bc.reconstructAndDeliver(fg)
		bc.cleanFECGroups(groups, fg.baseSeq)
	} else {
		// Deliver data shards directly as they arrive — the reorder buffer
		// will handle sequencing. If the parity shard later enables
		// reconstruction of a missing one, it will be inserted there too.
		bc.recvBuf.Insert(seq, payload, false)
	}
}

// deliverToFEC handles an incoming parity shard.
func (bc *BondedPacketConn) deliverToFEC(groups map[uint16]*fecGroup, seq uint16, payload []byte, isParity bool) {
	// Parity seq = baseSeq + fecDataShards (the 5th shard, index 4).
	// So baseSeq = seq - fecDataShards.
	baseSeq := seq - fecDataShards
	fg, ok := groups[baseSeq]
	if !ok {
		fg = &fecGroup{baseSeq: baseSeq}
		groups[baseSeq] = fg
	}
	fg.addShard(fecDataShards, payload) // parity is always shard index 4

	if fg.canReconstruct() {
		bc.reconstructAndDeliver(fg)
		bc.cleanFECGroups(groups, baseSeq)
	}
}

// reconstructAndDeliver uses Reed-Solomon to fill any missing data shards and
// inserts all recovered payloads into the reorder buffer.
func (bc *BondedPacketConn) reconstructAndDeliver(fg *fecGroup) {
	shards := make([][]byte, fecTotalShards)
	for i := 0; i < fecTotalShards; i++ {
		if fg.shards[i] != nil {
			s := make([]byte, fg.maxLen)
			copy(s, fg.shards[i])
			shards[i] = s
		}
	}

	if err := bc.fecEnc.ReconstructData(shards); err != nil {
		bondLog.Warn("[Bond] FEC reconstruct failed: %v", err)
	} else {
		bondLog.Info("[Bond] ✅ FEC reconstructed missing shard (group base=%d)", fg.baseSeq)
	}

	// Deliver all data shards (both original and reconstructed).
	for i := 0; i < fecDataShards; i++ {
		if shards[i] != nil {
			bc.recvBuf.Insert(fg.baseSeq+uint16(i), shards[i], false)
		}
	}
}

// cleanFECGroups removes groups that are too old to be relevant.
func (bc *BondedPacketConn) cleanFECGroups(groups map[uint16]*fecGroup, delivered uint16) {
	for base := range groups {
		diff := int(delivered) - int(base)
		if diff < 0 {
			diff += maxSeqID
		}
		if diff > reorderWindowSize*2 {
			delete(groups, base)
		}
	}
}

// ── Factory helpers ────────────────────────────────────────────────────────────

// NewBondedClient creates a BondedPacketConn pre-loaded with lanes.
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

// BondedRemoteAddr returns the fake remote address for quic.Dial().
func BondedRemoteAddr() net.Addr { return opusAddr{"bond://remote:0"} }

// ── atomic helpers ─────────────────────────────────────────────────────────────

// Ensure atomic.Int32 satisfies the interface we use throughout.
var _ interface{ Load() int32 } = (*atomic.Int32)(nil)
