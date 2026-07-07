// Package quicconn — bonded_packet_conn.go
//
// All three critical production bugs fixed:
//
//   Hole 1 — Distributed FEC Fragmentation:
//     fecGroups moved from per-lane local map → shared BondedPacketConn field
//     guarded by fecRegistryMu. WRR stripes shards across different lane
//     goroutines; a shared registry is the only way shards from all lanes can
//     accumulate toward canReconstruct().
//
//   Hole 2 — Slab Pool Memory Recycle Race:
//     When a data shard is stored in fecGroup.shards[], a deep copy is made so
//     the FEC group's lifetime is decoupled from the pool slab. The pool slab
//     itself is handed to the reorder buffer (and recycled by ReadFrom after
//     QUIC copies it). The FEC group never touches pool memory again.
//
//   Hole 3 — WRR GCD=1 CPU Starvation → Deficit Round Robin:
//     Interleaved WRR replaced with O(N) worst-case DRR (Shreedhar & Varghese).
//     DRR adds a fixed quantum (= lane weight) to the lane's deficit counter on
//     each visit. Selection happens when deficit >= packetSize. No tight loop;
//     the inner scan visits each lane exactly once per call in the normal case.
//
// Wire header (5 bytes):
//   [seq:2][flags:1][grpID:1][shardIdx:1]
//
//   grpID and shardIdx decouple FEC group membership from the global seq counter
//   so interleaved non-FEC packets never disrupt group reconstruction.
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
	bondHeaderSize = 5 // [seq:2][flags:1][grpID:1][shardIdx:1]

	fecDataShards   = 4
	fecParityShards = 1
	fecTotalShards  = fecDataShards + fecParityShards

	flagParity  = uint8(1 << 0)
	flagHedged  = uint8(1 << 1)
	flagFECData = uint8(1 << 2)

	grpIDNone    = uint8(0xFF)
	shardIdxNone = uint8(0xFF)

	fecMinLanes          = 2
	weightSampleInterval = 500 * time.Millisecond

	// fecFlushInterval bounds how long a partially-accumulated FEC group is
	// held before being flushed as direct packets. Must be small enough that
	// sparse QUIC traffic (ACKs, keepalives) is never stalled.
	fecFlushInterval = 2 * time.Millisecond

	maxBondPayload = 1135                               // 1140 − 5-byte header
	poolSlabSize   = maxBondPayload + bondHeaderSize + 64 // 1264 bytes per slab
)

// ── Global buffer pool ─────────────────────────────────────────────────────────

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
		return // don't pool undersized / heap-allocated slabs
	}
	b = b[:poolSlabSize]
	bondBufPool.Put(&b)
}

// ── Lane ───────────────────────────────────────────────────────────────────────

type lane struct {
	conn  *rtpconn.Conn
	label string

	// DRR state.
	weight  atomic.Int32 // quantum [10..10000], updated by weightSampler
	deficit int32        // accumulated credit; protected by BondedPacketConn.drrMu

	// Health tracking for weightSampler.
	recvPkts  atomic.Int64
	prevRecv  int64
	smoothRTT atomic.Int64 // EWMA RTT estimate in milliseconds
}

// ── BondedPacketConn ───────────────────────────────────────────────────────────

type BondedPacketConn struct {
	mu    sync.RWMutex
	lanes []*lane

	seqID      atomic.Uint32 // global monotonic packet sequence
	fecGroupID atomic.Uint32 // wrapping 8-bit FEC group counter

	// ── Transmit-side FEC accumulation (protected by fecMu) ───────────────
	fecMu    sync.Mutex
	fecEnc   reedsolomon.Encoder
	fecBuf   [fecDataShards][]byte
	fecCount int

	// ── Hole 1 fix: CENTRALIZED receive-side FEC registry ─────────────────
	// All laneReader goroutines share this map so shards arriving on
	// different lanes can merge into the same fecGroup.
	fecRegistryMu sync.Mutex
	fecGroups     map[uint8]*fecGroup

	// Receive path.
	recvBuf *reorderBuffer
	done    chan struct{}
	once    sync.Once

	// ── Hole 3 fix: Deficit Round Robin state ─────────────────────────────
	drrMu  sync.Mutex
	drrIdx int // index of the last selected lane (cursor)

	localAddr  net.Addr
	remoteAddr net.Addr
}

func NewBondedPacketConn() (*BondedPacketConn, error) {
	enc, err := reedsolomon.New(fecDataShards, fecParityShards)
	if err != nil {
		return nil, fmt.Errorf("reedsolomon init: %w", err)
	}
	bc := &BondedPacketConn{
		fecEnc:     enc,
		fecGroups:  make(map[uint8]*fecGroup),
		recvBuf:    newReorderBuffer(),
		done:       make(chan struct{}),
		localAddr:  opusAddr{name: "bond://local:0"},
		remoteAddr: opusAddr{name: "bond://remote:0"},
	}
	go bc.weightSampler()
	go bc.fecFlushLoop()
	return bc, nil
}

// AddLane registers a new rtpconn.Conn as a bonding lane and starts its reader.
func (bc *BondedPacketConn) AddLane(conn *rtpconn.Conn, label string) {
	l := &lane{conn: conn, label: label}
	l.weight.Store(100)
	l.smoothRTT.Store(50)

	bc.mu.Lock()
	bc.lanes = append(bc.lanes, l)
	n := len(bc.lanes)
	bc.mu.Unlock()

	bondLog.Info("[Bond] Lane added: %s (total: %d)", label, n)
	// Pass *lane so laneReader can update recvPkts for weightSampler.
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
}

func (bc *BondedPacketConn) LaneCount() int {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return len(bc.lanes)
}

// ── net.PacketConn interface ───────────────────────────────────────────────────

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

	// QUIC long-header → hedge to top-3 lanes for reliable handshake.
	if len(p) > 0 && len(p) <= 1280 && (p[0]&0x80) != 0 && numLanes >= 3 {
		bc.sendHedged(p)
		return len(p), nil
	}

	if numLanes >= fecMinLanes {
		return bc.writeWithFEC(p)
	}
	return bc.writeDirect(p, 0)
}

// ReadFrom is called by quic-go to receive the next QUIC datagram.
// Returns the pool slab to the pool after QUIC copies the bytes out.
func (bc *BondedPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	data := bc.recvBuf.Next()
	if data == nil {
		return 0, nil, fmt.Errorf("bonded conn closed")
	}
	n = copy(p, data)
	putPoolBuf(data) // safe: FEC group holds its own deep copy (Hole 2 fix)
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

// ── Transmit-side FEC pipeline ─────────────────────────────────────────────────

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

	if bc.fecCount < fecDataShards {
		return len(p), nil // hold — nothing sent yet (flush timer bounds the wait)
	}
	return bc.flushFECGroup(len(p))
}

// fecFlushLoop drains a partially-accumulated FEC group as direct (non-FEC)
// packets if 4 data shards have not arrived within fecFlushInterval. Without
// this, sparse traffic (e.g. a lone ACK) would be buffered forever, stalling
// the QUIC connection. Under bursty traffic the group fills long before the
// timer fires, so normal FEC encoding is unaffected.
func (bc *BondedPacketConn) fecFlushLoop() {
	ticker := time.NewTicker(fecFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-bc.done:
			return
		case <-ticker.C:
			bc.flushPartialFEC()
		}
	}
}

func (bc *BondedPacketConn) flushPartialFEC() {
	bc.fecMu.Lock()
	if bc.fecCount == 0 || bc.fecCount >= fecDataShards {
		bc.fecMu.Unlock()
		return
	}
	pending := make([][]byte, bc.fecCount)
	copy(pending, bc.fecBuf[:bc.fecCount])
	bc.fecBuf = [fecDataShards][]byte{}
	bc.fecCount = 0
	bc.fecMu.Unlock()

	for _, pkt := range pending {
		if pkt != nil {
			bc.writeDirect(pkt, 0)
		}
	}
}

func (bc *BondedPacketConn) flushFECGroup(origLen int) (int, error) {
	grpID := uint8(bc.fecGroupID.Add(1) & 0xFF)

	defer func() {
		bc.fecBuf = [fecDataShards][]byte{}
		bc.fecCount = 0
	}()

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
		bondLog.Warn("[Bond] FEC encode error: %v — raw fallback", err)
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

	for i, shard := range shards {
		flags := flagFECData
		if i == fecDataShards {
			flags = flagParity
		}
		// DRR-based lane selection for every shard.
		laneIdx := bc.pickLaneDRR(lanes, int32(len(shard)+bondHeaderSize))
		seq := uint16(bc.seqID.Add(1) - 1)
		bc.writeToLane(shard, laneIdx, seq, flags, grpID, uint8(i))
	}
	return origLen, nil
}

func (bc *BondedPacketConn) writeDirect(p []byte, flags uint8) (int, error) {
	bc.mu.RLock()
	lanes := bc.lanes
	bc.mu.RUnlock()
	if len(lanes) == 0 {
		return 0, fmt.Errorf("no lanes")
	}
	laneIdx := bc.pickLaneDRR(lanes, int32(len(p)+bondHeaderSize))
	seq := uint16(bc.seqID.Add(1) - 1)
	bc.writeToLane(p, laneIdx, seq, flags, grpIDNone, shardIdxNone)
	return len(p), nil
}

func (bc *BondedPacketConn) sendHedged(p []byte) {
	bc.mu.RLock()
	lanes := bc.lanes
	bc.mu.RUnlock()
	count := 3
	if len(lanes) < count {
		count = len(lanes)
	}
	// All hedged copies share ONE sequence id so the receiver's reorder
	// buffer deduplicates them (the first copy wins; the rest are dropped).
	seq := uint16(bc.seqID.Add(1) - 1)
	for _, idx := range bc.topNLanes(lanes, count) {
		bc.writeToLane(p, idx, seq, flagHedged, grpIDNone, shardIdxNone)
	}
	bondLog.Info("[Bond] Hedged packet to %d lanes (size=%d)", count, len(p))
}

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

func (bc *BondedPacketConn) writeToLane(p []byte, laneIdx int, seq uint16, flags, grpID, shardIdx uint8) {
	bc.mu.RLock()
	if laneIdx >= len(bc.lanes) {
		bc.mu.RUnlock()
		return
	}
	l := bc.lanes[laneIdx]
	bc.mu.RUnlock()

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
	frame[2] = flags
	frame[3] = grpID
	frame[4] = shardIdx
	copy(frame[bondHeaderSize:], p)

	if _, err := l.conn.WriteFrame(frame); err != nil {
		bondLog.Warn("[Bond] Lane %s write error: %v", l.label, err)
	}
}

// ── Hole 3 fix: Deficit Round Robin selector ───────────────────────────────────
//
// DRR (Shreedhar & Varghese 1995):
//   Each lane maintains a deficit credit counter.
//   On each selection call we scan lanes in circular order (one full pass).
//   For each lane visited: deficit += quantum (= lane weight).
//   If deficit >= packetSize: select this lane, subtract packetSize, return.
//
// Worst-case complexity: O(N) lanes visited per call — completely independent
// of the weight magnitude, so weight=9999 vs weight=10000 is no worse than
// weight=1 vs weight=2.
//
// If no lane has enough deficit after one full pass (very large packets), we
// pick the lane with the highest accumulated deficit to avoid starvation.
func (bc *BondedPacketConn) pickLaneDRR(lanes []*lane, packetSize int32) int {
	if len(lanes) == 1 {
		return 0
	}

	bc.drrMu.Lock()
	defer bc.drrMu.Unlock()

	n := len(lanes)

	// One full circular pass: add quantum to each lane, select first that can send.
	for i := 0; i < n; i++ {
		bc.drrIdx = (bc.drrIdx + 1) % n
		l := lanes[bc.drrIdx]
		l.deficit += l.weight.Load()
		if l.deficit >= packetSize {
			l.deficit -= packetSize
			return bc.drrIdx
		}
	}

	// No lane had enough deficit (e.g. all weights < packetSize). Pick the
	// lane with the highest deficit to make forward progress and avoid starvation.
	bestIdx := 0
	bestDef := lanes[0].deficit
	for i := 1; i < n; i++ {
		if lanes[i].deficit > bestDef {
			bestDef = lanes[i].deficit
			bestIdx = i
		}
	}
	if lanes[bestIdx].deficit >= packetSize {
		lanes[bestIdx].deficit -= packetSize
	} else {
		lanes[bestIdx].deficit = 0
	}
	bc.drrIdx = bestIdx
	return bestIdx
}

// ── Weight sampler (RTT-proxy health model) ────────────────────────────────────

// weightSampler recomputes per-lane DRR quantums every 500ms.
// Higher weight = higher quantum = more packets scheduled per round.
//
//	Weight_i = max(10, 10000 / (SmoothRTT_i × (LossRate_i + 1)))
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

			for _, l := range lanes {
				cur := l.recvPkts.Load()
				delta := cur - l.prevRecv
				l.prevRecv = cur

				var rttMs int64
				switch {
				case delta >= 50:
					rttMs = 5
				case delta > 0:
					rttMs = 5 + int64(math.Round(float64(50-delta)/50.0*995))
				default:
					rttMs = 1000
				}

				prev := l.smoothRTT.Load()
				smoothed := int64(math.Round(float64(prev)*0.7 + float64(rttMs)*0.3))
				l.smoothRTT.Store(smoothed)

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
					bondLog.Info("[Bond] Lane %s weight %d→%d (rtt≈%dms)", l.label, old, newW, smoothed)
				}
			}
		}
	}
}

// ── Hole 1 fix: Lane reader with SHARED FEC registry ──────────────────────────
//
// Every lane goroutine routes FEC shards to the centralized bc.fecGroups map
// (guarded by bc.fecRegistryMu) so shards arriving on different lanes can
// accumulate toward the same group and trigger reconstruction.

func (bc *BondedPacketConn) laneReader(l *lane) {
	// Periodic stale-group cleanup ticker (runs inside the read loop).
	cleanTicker := time.NewTicker(1 * time.Second)
	defer cleanTicker.Stop()

	for {
		select {
		case <-bc.done:
			return
		default:
		}

		// Non-blocking stale group cleanup.
		select {
		case <-cleanTicker.C:
			bc.cleanStaleGroups()
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
			// fecDataShards (4) is the index of the parity shard.
			bc.processSharedFECShard(grpID, uint8(fecDataShards), seq, payload)

		case isHedged:
			bc.recvBuf.Insert(seq, payload, true)

		case isFECData:
			bc.processSharedFECShard(grpID, shardIdx, seq, payload)

		default:
			bc.recvBuf.Insert(seq, payload, false)
		}
	}
}

// ── Hole 1 + 2 fix: Centralized FEC shard processor ──────────────────────────
//
// Called by all laneReader goroutines. The shared fecRegistryMu ensures that
// shards arriving on different lanes merge into the same fecGroup.
//
// Memory safety (Hole 2):
//   The payload slab is a pool buffer. We store a DEEP COPY in the fecGroup
//   so its lifetime is independent of the pool. The original slab is passed to
//   the reorder buffer (for data shards) or recycled immediately (for parity).
//   When ReadFrom returns the slab to the pool, the fecGroup is unaffected.
func (bc *BondedPacketConn) processSharedFECShard(grpID, shardIdx uint8, seq uint16, payload []byte) {
	isParity := shardIdx == uint8(fecDataShards)

	// Validate shard index.
	if !isParity && shardIdx >= uint8(fecDataShards) {
		putPoolBuf(payload)
		return
	}

	bc.fecRegistryMu.Lock()
	defer bc.fecRegistryMu.Unlock()

	fg, exists := bc.fecGroups[grpID]
	if !exists {
		fg = &fecGroup{groupID: grpID, createdAt: time.Now()}
		bc.fecGroups[grpID] = fg
	}

	// ── Hole 2 fix: deep copy into fecGroup ───────────────────────────────
	// The pool slab (payload) goes to the reorder buffer; the fecGroup gets
	// its own independent allocation so putPoolBuf in ReadFrom is safe.
	if fg.shards[shardIdx] == nil {
		cloned := make([]byte, len(payload))
		copy(cloned, payload)
		fg.shards[shardIdx] = cloned
		fg.seqNums[shardIdx] = seq
		fg.received++
		if len(payload) > fg.maxLen {
			fg.maxLen = len(payload)
		}
	}

	if isParity {
		// Parity is only used for reconstruction — never forwarded to QUIC.
		putPoolBuf(payload) // recycle slab immediately; fecGroup has its deep copy
	} else {
		// Data shards are delivered immediately for latency; reconstruction
		// fills in any that were dropped.
		bc.recvBuf.Insert(seq, payload, false)
	}

	if fg.received >= fecDataShards {
		bc.executeSharedReconstruction(fg)
		delete(bc.fecGroups, grpID)
	}
}

// executeSharedReconstruction runs Reed-Solomon on a complete (or reconstructible)
// group and inserts ONLY the missing data shards into the reorder buffer.
// Called with bc.fecRegistryMu held.
func (bc *BondedPacketConn) executeSharedReconstruction(fg *fecGroup) {
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

	// Extrapolate seq of missing shards from the first present shard.
	var baseSeq uint16
	baseFound := false
	for i := 0; i < fecDataShards; i++ {
		if fg.shards[i] != nil {
			baseSeq = fg.seqNums[i] - uint16(i)
			baseFound = true
			break
		}
	}
	if !baseFound {
		return
	}

	// Insert only genuinely missing shards (delivered ones are already in reorder buf).
	for i := 0; i < fecDataShards; i++ {
		if missing[i] && shards[i] != nil {
			targetSeq := baseSeq + uint16(i)
			bondLog.Info("[Bond] ✅ FEC recovered seq=%d (grp=%d)", targetSeq, fg.groupID)
			bc.recvBuf.Insert(targetSeq, shards[i], false)
		}
	}
}

// cleanStaleGroups evicts FEC groups older than 2s (permanent packet loss).
// Called periodically from laneReader without holding fecRegistryMu.
func (bc *BondedPacketConn) cleanStaleGroups() {
	now := time.Now()
	bc.fecRegistryMu.Lock()
	for id, g := range bc.fecGroups {
		if now.Sub(g.createdAt) > 2*time.Second {
			delete(bc.fecGroups, id)
		}
	}
	bc.fecRegistryMu.Unlock()
}

// ── FEC group (receive-side) ───────────────────────────────────────────────────

type fecGroup struct {
	groupID   uint8
	shards    [fecTotalShards][]byte // deep copies, lifetime-independent of pool
	seqNums   [fecTotalShards]uint16
	received  int
	maxLen    int
	createdAt time.Time
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

// BondedRemoteAddr returns the fake remote address required by quic.Dial().
func BondedRemoteAddr() net.Addr { return opusAddr{name: "bond://remote:0"} }

// ── Compile-time interface checks ─────────────────────────────────────────────
var _ net.PacketConn = (*BondedPacketConn)(nil)
var _ interface{ Load() int32 } = (*atomic.Int32)(nil)
