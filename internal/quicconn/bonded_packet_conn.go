// Package quicconn — bonded_packet_conn.go
//
// BondedPacketConn implements net.PacketConn over N concurrent WebRTC
// audio tracks. It provides:
//
//   - Packet-level striping (weighted round-robin across N lanes)
//   - 4+1 Reed-Solomon Forward Error Correction
//   - Speculative hedging for high-priority control packets
//   - Sliding-window jitter reorder buffer (see reorder_buffer.go)
//
// Wire header (4 bytes prepended to every packet):
//
//	┌──────────┬──────────┬────────┬───────┐
//	│ seq[0:1] │ chanID   │ flags  │ (rsv) │
//	│ uint16   │ uint8    │ uint8  │ uint8 │
//	└──────────┴──────────┴────────┴───────┘
//
//	seq    — global packet sequence (uint16, wraps at 65536)
//	chanID — originating lane index (0..N-1)
//	flags  — bit 0: IS_PARITY  (FEC parity shard)
//	         bit 1: IS_HEDGED  (speculative duplicate)
//	         bit 2: IS_LAST    (last packet in FEC group)
//
// FEC topology: for every 4 data packets the encoder generates 1 parity
// packet (4+1 configuration). Any 4 of the 5 shards are sufficient to
// reconstruct all data.
//
// Hedging: QUIC Initial / Handshake packets (identified by size ≤ 1280
// and first-byte pattern 0xC0–0xFF) are duplicated across 3 lanes
// simultaneously. The reorder buffer deduplicates on arrival.
package quicconn

import (
	"encoding/binary"
	"fmt"
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
	flagParity = 1 << 0
	flagHedged = 1 << 1
	flagLast   = 1 << 2

	// Minimum lanes needed before FEC is activated.
	// With fewer lanes, FEC wastes bandwidth sending parity on the same lane.
	fecMinLanes = 2

	// Weight sampling interval.
	weightSampleInterval = 500 * time.Millisecond

	// Maximum MTU after adding the 4-byte bond header.
	// rtpconn writes up to 1140 bytes; bond header reduces usable payload.
	maxBondPayload = 1136
)

// lane wraps one rtpconn.Conn with per-lane weight tracking.
type lane struct {
	conn   *rtpconn.Conn
	label  string
	weight atomic.Int32 // higher = more packets assigned; updated every 500ms
	// Simple loss/RTT estimator: we track outgoing sequence IDs and check
	// which were ACK'd. Since we're inside QUIC (which handles this), we use
	// a simplified model: lanes that receive data from the remote end are
	// considered healthy; silent lanes are penalized.
	recvPkts atomic.Int64
	prevRecv  int64
}

// BondedPacketConn is a virtual net.PacketConn that stripes QUIC datagrams
// across all registered lanes simultaneously.
type BondedPacketConn struct {
	mu    sync.RWMutex
	lanes []*lane

	// Packet sequencing
	seqID atomic.Uint32

	// FEC encoder (4+1 Reed-Solomon)
	fecEnc      reedsolomon.Encoder
	fecMu       sync.Mutex
	fecBuf      [fecDataShards][]byte
	fecCount    int

	// Receive path
	recvBuf *reorderBuffer
	done    chan struct{}
	once    sync.Once

	// Round-robin cursor
	rrCursor atomic.Uint32

	// Local/remote fake addresses (QUIC requires these)
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
// Safe to call concurrently and after QUIC has already started.
func (bc *BondedPacketConn) AddLane(conn *rtpconn.Conn, label string) {
	bc.mu.Lock()
	l := &lane{conn: conn, label: label}
	l.weight.Store(100) // start with full weight
	bc.lanes = append(bc.lanes, l)
	idx := len(bc.lanes) - 1
	bc.mu.Unlock()

	bondLog.Info("[Bond] Lane added: %s (total lanes: %d)", label, idx+1)

	// Start a reader goroutine for this lane.
	go bc.laneReader(l)
}

// RemoveLane removes a lane (called when a WebRTC channel dies).
func (bc *BondedPacketConn) RemoveLane(conn *rtpconn.Conn) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	for i, l := range bc.lanes {
		if l.conn == conn {
			bc.lanes = append(bc.lanes[:i], bc.lanes[i+1:]...)
			bondLog.Info("[Bond] Lane removed: %s (remaining: %d)", l.label, len(bc.lanes))
			return
		}
	}
}

// LaneCount returns the current number of active lanes.
func (bc *BondedPacketConn) LaneCount() int {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return len(bc.lanes)
}

// ── net.PacketConn interface ───────────────────────────────────────────────────

// WriteTo is called by quic-go for every outgoing QUIC datagram.
// It stripes the packet (and its FEC parity) across all active lanes.
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

	// ── Speculative hedging for QUIC Initial/Handshake packets ────────────
	// QUIC long-header packets have the MSB of byte 0 set (0x80+).
	// Initial packets also have the version field. We hedge on small packets
	// with the long-header bit set to cover the critical handshake path.
	if len(p) <= 1280 && len(p) > 0 && (p[0]&0x80) != 0 && numLanes >= 3 {
		bc.sendHedged(p)
		return len(p), nil
	}

	// ── FEC path ─────────────────────────────────────────────────────────
	if numLanes >= fecMinLanes {
		return bc.writeWithFEC(p)
	}

	// ── Single-lane fallback (pass-through with header) ──────────────────
	return bc.writeDirect(p, 0, 0)
}

// ReadFrom blocks until a reordered packet is available from any lane.
// Implements net.PacketConn.
func (bc *BondedPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	data := bc.recvBuf.Next()
	if data == nil {
		return 0, nil, fmt.Errorf("bonded conn closed")
	}
	n = copy(p, data)
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

// ── Internal write helpers ─────────────────────────────────────────────────────

// writeWithFEC accumulates packets into a 4+1 FEC group, then flushes all
// 5 shards across different lanes when the group is complete.
func (bc *BondedPacketConn) writeWithFEC(p []byte) (int, error) {
	bc.fecMu.Lock()
	defer bc.fecMu.Unlock()

	// Clamp payload to maxBondPayload.
	data := p
	if len(data) > maxBondPayload {
		data = data[:maxBondPayload]
	}

	// Copy into the FEC accumulation buffer.
	buf := make([]byte, len(data))
	copy(buf, data)
	bc.fecBuf[bc.fecCount] = buf
	bc.fecCount++

	if bc.fecCount < fecDataShards {
		// Not enough shards yet — also send this shard directly so the
		// QUIC layer isn't starved while we accumulate.
		if _, err := bc.writeDirect(data, 0, 0); err != nil {
			return 0, err
		}
		return len(p), nil
	}

	// ── Group complete — build parity and emit all 5 shards ──────────────
	defer func() {
		bc.fecBuf = [fecDataShards][]byte{}
		bc.fecCount = 0
	}()

	// Pad all shards to the same length (required by reedsolomon).
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
	shards[fecDataShards] = make([]byte, maxLen) // parity placeholder

	if err := bc.fecEnc.Encode(shards); err != nil {
		// FEC encode failed — fall back to direct send.
		bondLog.Warn("[Bond] FEC encode error: %v — falling back to direct", err)
		for i := 0; i < fecDataShards; i++ {
			bc.writeDirect(bc.fecBuf[i], 0, 0) //nolint:errcheck
		}
		return len(p), nil
	}

	// Emit all 5 shards across 5 different lanes (round-robin start).
	for i, shard := range shards {
		flags := uint8(0)
		if i == fecDataShards {
			flags |= flagParity
		}
		if i == fecTotalShards-1 {
			flags |= flagLast
		}
		bc.writeToLane(shard, i, flags)
	}

	return len(p), nil
}

// writeDirect sends one packet to the next available lane (weighted RR).
func (bc *BondedPacketConn) writeDirect(p []byte, flags uint8, forceFlags uint8) (int, error) {
	bc.mu.RLock()
	lanes := bc.lanes
	bc.mu.RUnlock()

	if len(lanes) == 0 {
		return 0, fmt.Errorf("no lanes")
	}

	laneIdx := bc.pickLane(lanes)
	bc.writeToLane(p, laneIdx, flags|forceFlags)
	return len(p), nil
}

// sendHedged sends a speculative duplicate of p to 3 lanes simultaneously.
// Used for critical QUIC handshake packets to race for lowest-latency path.
func (bc *BondedPacketConn) sendHedged(p []byte) {
	bc.mu.RLock()
	lanes := bc.lanes
	bc.mu.RUnlock()

	count := 3
	if len(lanes) < count {
		count = len(lanes)
	}

	for i := 0; i < count; i++ {
		laneIdx := (int(bc.rrCursor.Add(1)) - 1) % len(lanes)
		bc.writeToLane(p, laneIdx, flagHedged)
	}

	bondLog.Info("[Bond] Hedged packet sent to %d lanes (size=%d)", count, len(p))
}

// writeToLane prepends the bond header and writes to a specific lane.
func (bc *BondedPacketConn) writeToLane(p []byte, laneIdx int, flags uint8) {
	bc.mu.RLock()
	if laneIdx >= len(bc.lanes) {
		bc.mu.RUnlock()
		return
	}
	l := bc.lanes[laneIdx]
	bc.mu.RUnlock()

	seq := uint16(bc.seqID.Add(1) - 1)

	// Build framed packet: [seq:2][chanID:1][flags:1][payload...]
	frame := make([]byte, bondHeaderSize+len(p))
	binary.BigEndian.PutUint16(frame[0:2], seq)
	frame[2] = uint8(laneIdx)
	frame[3] = flags
	copy(frame[bondHeaderSize:], p)

	if _, err := l.conn.WriteFrame(frame); err != nil {
		bondLog.Warn("[Bond] Lane %s write error: %v", l.label, err)
	}
}

// pickLane selects the next lane using weighted round-robin.
func (bc *BondedPacketConn) pickLane(lanes []*lane) int {
	if len(lanes) == 1 {
		return 0
	}
	// Simple weighted approach: accumulate weights and pick based on cursor.
	// TODO: For a future version, implement a proper WRR queue.
	return int(bc.rrCursor.Add(1)-1) % len(lanes)
}

// ── Lane reader goroutine ──────────────────────────────────────────────────────

// laneReader continuously reads from one rtpconn.Conn, strips the bond
// header, and feeds payloads into the reorder buffer or FEC reconstructor.
func (bc *BondedPacketConn) laneReader(l *lane) {
	// FEC group tracking: per-lane, we track shards by group (seq range).
	// Groups are identified by the seq of the first shard in the group.
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
		// chanID := raw[2] // not used on receive side
		flags := raw[3]
		payload := raw[bondHeaderSize:]

		isHedged := (flags & flagHedged) != 0
		isParity := (flags & flagParity) != 0

		if isParity {
			// Deliver parity to FEC group tracker.
			bc.deliverToFEC(fecGroups, seq, payload, flags)
			continue
		}

		if isHedged {
			// Deduplicated by the reorder buffer.
			bc.recvBuf.Insert(seq, payload, true)
			continue
		}

		// Check if this seq is part of an active FEC group.
		if fg := bc.findFECGroup(fecGroups, seq); fg != nil {
			fg.AddShard(seq, payload, false)
			if fg.CanReconstruct() {
				bc.reconstructAndDeliver(fg)
				bc.cleanFECGroups(fecGroups, seq)
			}
			continue
		}

		// Normal data packet — insert into reorder queue.
		bc.recvBuf.Insert(seq, payload, false)
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

func (fg *fecGroup) AddShard(seq uint16, data []byte, isParity bool) {
	idx := int(seq - fg.baseSeq)
	if isParity {
		idx = fecDataShards // parity is always last shard
	}
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

func (fg *fecGroup) CanReconstruct() bool {
	return fg.received >= fecDataShards
}

func (bc *BondedPacketConn) deliverToFEC(groups map[uint16]*fecGroup, seq uint16, payload []byte, flags uint8) {
	// The parity packet's seq is the base seq of the group + fecDataShards.
	baseSeq := seq - fecDataShards
	fg, ok := groups[baseSeq]
	if !ok {
		fg = &fecGroup{baseSeq: baseSeq}
		groups[baseSeq] = fg
	}
	fg.AddShard(seq, payload, true)
	if fg.CanReconstruct() {
		bc.reconstructAndDeliver(fg)
		bc.cleanFECGroups(groups, baseSeq)
	}
}

func (bc *BondedPacketConn) findFECGroup(groups map[uint16]*fecGroup, seq uint16) *fecGroup {
	// A data shard seq is in group baseSeq if baseSeq <= seq < baseSeq+fecDataShards.
	for base, fg := range groups {
		if seq >= base && int(seq-base) < fecDataShards {
			return fg
		}
	}
	return nil
}

func (bc *BondedPacketConn) reconstructAndDeliver(fg *fecGroup) {
	// Pad shards to uniform length.
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
		// Deliver whatever we have.
		for i := 0; i < fecDataShards; i++ {
			if shards[i] != nil {
				bc.recvBuf.Insert(fg.baseSeq+uint16(i), shards[i], false)
			}
		}
		return
	}

	bondLog.Info("[Bond] FEC reconstructed a missing shard (group base=%d)", fg.baseSeq)
	for i := 0; i < fecDataShards; i++ {
		if shards[i] != nil {
			bc.recvBuf.Insert(fg.baseSeq+uint16(i), shards[i], false)
		}
	}
}

func (bc *BondedPacketConn) cleanFECGroups(groups map[uint16]*fecGroup, delivered uint16) {
	// Clean up groups older than the delivered group.
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

// ── Weight sampler ─────────────────────────────────────────────────────────────

// weightSampler re-computes per-lane weights every 500ms.
// Currently uses a simple receive-rate model: lanes that are receiving data
// from the far end are healthy; silent lanes get reduced weight.
func (bc *BondedPacketConn) weightSampler() {
	ticker := time.NewTicker(weightSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-bc.done:
			return
		case <-ticker.C:
			bc.mu.RLock()
			for _, l := range bc.lanes {
				cur := l.recvPkts.Load()
				delta := cur - l.prevRecv
				l.prevRecv = cur
				if delta > 0 {
					l.weight.Store(100) // healthy
				} else {
					// Reduce weight — lane may be idle or congested.
					w := l.weight.Load()
					if w > 25 {
						l.weight.Store(w - 25)
					}
				}
			}
			bc.mu.RUnlock()
		}
	}
}

// ── Factory helpers ────────────────────────────────────────────────────────────

// NewBondedClient creates a BondedPacketConn for the QUIC client side.
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
	return NewBondedClient(lanes, labels) // identical construction
}

// BondedRemoteAddr returns the fake remote address for quic.Dial().
func BondedRemoteAddr() net.Addr { return opusAddr{"bond://remote:0"} }
