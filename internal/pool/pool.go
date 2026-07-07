// Package pool manages the bonded multi-lane QUIC tunnel.
//
// Architecture (post-bonding update):
//
//   SOCKS5/HTTP proxy → pool.OpenStream()
//                           │
//                           ▼
//                   [ Single Master QUIC Connection ]
//                           │
//                           ▼
//                   [ BondedPacketConn ]
//                           │
//       ┌───────────┬───────┴───────┬───────────┐
//       ▼           ▼               ▼           ▼
//   [Lane 0]    [Lane 1]       [Lane 2]    [Lane N-1]
//   (rtpconn)   (rtpconn)      (rtpconn)   (rtpconn)
//
// Previously: N independent QUIC connections, one per WebRTC channel.
// Now:        1 QUIC connection backed by a BondedPacketConn that
//             stripes individual UDP datagrams across all N lanes.
//
// The proxy-facing API (OpenStream, CloseAll, ActiveCount, Sessions) is
// unchanged so that no proxy code needs modification.
//
// Circuit Breaker logic (retained for stream-level fault detection):
//   - Streams with ≥3 consecutive open failures trip the breaker.
//   - After 5 failures the master QUIC connection is force-closed so
//     the TunnelManager can re-establish the bond.
//   - Success resets the failure counter.
package pool

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"github.com/salman/ble-webrtc-tun/internal/quicconn"
	"github.com/salman/ble-webrtc-tun/internal/rtpconn"
)

var poolLog = logger.New("pool")

// circuitBreakerThreshold is the number of consecutive stream open failures
// after which a channel is excluded from load balancing.
const circuitBreakerThreshold = int32(3)

// circuitBreakerKill is the number of consecutive failures that triggers a
// force-close of the QUIC connection, causing the TunnelManager to re-dial.
const circuitBreakerKill = int32(5)

// streamTimeout is how long OpenStreamSync waits before giving up.
const streamTimeout = 2 * time.Second

// TunnelPool manages the bonded multi-lane tunnel.
// Externally it looks identical to the old pool — OpenStream() still returns
// a net.Conn wrapping a QUIC stream. Internally, all streams run over the
// single master QUIC connection backed by the BondedPacketConn.
type TunnelPool struct {
	mu      sync.RWMutex
	entries []*PoolEntry     // legacy: still holds pool entries for health/status
	done    chan struct{}
	once    sync.Once

	// Bonding layer
	bondConn   *quicconn.BondedPacketConn // the virtual multi-lane transport
	masterConn quic.Connection             // single QUIC conn over bondConn
	masterMu   sync.RWMutex
	masterFail atomic.Int32 // consecutive stream failures on master conn
}

// PoolEntry wraps metadata for one WebRTC lane (not one QUIC connection).
// Kept for status reporting and health monitoring compatibility.
type PoolEntry struct {
	Conn         quic.Connection // nil in bonded mode (shared via masterConn)
	Label        string
	ActiveStreams atomic.Int32 // streams currently attributed to this lane
	FailCount    atomic.Int32 // consecutive stream failures (circuit breaker)
	addedAt      time.Time
}

// New creates a new empty TunnelPool with a background health monitor.
func New() *TunnelPool {
	p := &TunnelPool{done: make(chan struct{})}
	go p.healthMonitorLoop()
	return p
}

// NewBonded creates a TunnelPool pre-wired with a BondedPacketConn.
// Call SetMasterConn after QUIC Dial/Listen has established the connection.
func NewBonded(bc *quicconn.BondedPacketConn) *TunnelPool {
	p := &TunnelPool{
		done:     make(chan struct{}),
		bondConn: bc,
	}
	go p.healthMonitorLoop()
	return p
}

// SetBondedConn registers the BondedPacketConn (can be set after New()).
func (p *TunnelPool) SetBondedConn(bc *quicconn.BondedPacketConn) {
	p.masterMu.Lock()
	p.bondConn = bc
	p.masterMu.Unlock()
}

// SetMasterConn registers the single master QUIC connection established
// over the BondedPacketConn. All OpenStream() calls use this connection.
func (p *TunnelPool) SetMasterConn(conn quic.Connection) {
	p.masterMu.Lock()
	p.masterConn = conn
	p.masterFail.Store(0)
	p.masterMu.Unlock()
	poolLog.Info("[BondedPool] Master QUIC connection registered")
}

// AddLane registers a new rtpconn.Conn lane into the BondedPacketConn and
// adds a metadata entry for health tracking.
func (p *TunnelPool) AddLane(conn *rtpconn.Conn, label string) {
	p.masterMu.RLock()
	bc := p.bondConn
	p.masterMu.RUnlock()

	if bc != nil {
		bc.AddLane(conn, label)
	}

	p.mu.Lock()
	p.entries = append(p.entries, &PoolEntry{
		Label:   label,
		addedAt: time.Now(),
	})
	p.mu.Unlock()
	poolLog.Info("[BondedPool] Lane %s registered (total lanes: %d)", label, p.LaneCount())
}

// RemoveLane removes an rtpconn.Conn lane from the bond.
func (p *TunnelPool) RemoveLane(conn *rtpconn.Conn, label string) {
	p.masterMu.RLock()
	bc := p.bondConn
	p.masterMu.RUnlock()

	if bc != nil {
		bc.RemoveLane(conn)
	}
	poolLog.Info("[BondedPool] Lane %s removed (remaining: %d)", label, p.LaneCount())
}

// LaneCount returns the current number of active lanes in the bond.
func (p *TunnelPool) LaneCount() int {
	p.masterMu.RLock()
	bc := p.bondConn
	p.masterMu.RUnlock()
	if bc == nil {
		return 0
	}
	return bc.LaneCount()
}

// Add is kept for backward-compatibility with the TunnelManager refresh paths.
// In bonded mode it is a no-op (lanes are added via AddLane).
func (p *TunnelPool) Add(conn quic.Connection, label string) {
	poolLog.Info("[BondedPool] Add() called (bonded mode — no-op, use AddLane): %s", label)
}

// Remove is kept for backward-compatibility.
func (p *TunnelPool) Remove(conn quic.Connection) {
	// In bonded mode there is only one master QUIC connection; removing it
	// means the entire bond needs to be re-established by TunnelManager.
	// We log and let the master conn death detection handle it.
	poolLog.Info("[BondedPool] Remove() called — bond will be rebuilt by TunnelManager")
}

// OpenStream opens a QUIC stream on the master bonded connection.
//
// Circuit breaker: after circuitBreakerThreshold consecutive failures the
// caller receives an error. After circuitBreakerKill failures the master
// QUIC connection is force-closed to trigger the TunnelManager's reconnect.
func (p *TunnelPool) OpenStream() (net.Conn, error) {
	p.masterMu.RLock()
	master := p.masterConn
	p.masterMu.RUnlock()

	// ── Fallback to legacy per-entry mode if no master conn yet ──────────
	if master == nil {
		return p.openStreamLegacy()
	}

	// ── Circuit breaker check ─────────────────────────────────────────────
	fails := p.masterFail.Load()
	if fails >= circuitBreakerThreshold {
		poolLog.Warn("[BondedPool] Master connection circuit-breaker open (fails=%d)", fails)
		if fails >= circuitBreakerKill {
			poolLog.Warn("[BondedPool] Circuit-breaker KILL — force-closing master conn")
			master.CloseWithError(1, "circuit breaker: stream exhaustion")
		}
		return nil, fmt.Errorf("master tunnel circuit-breaker open (fails=%d)", fails)
	}

	// ── Open stream ───────────────────────────────────────────────────────
	ctx, cancel := context.WithTimeout(context.Background(), streamTimeout)
	defer cancel()

	stream, err := master.OpenStreamSync(ctx)
	if err != nil {
		newFails := p.masterFail.Add(1)
		poolLog.Warn("[BondedPool] Stream open failed (fails=%d): %v", newFails, err)
		if newFails >= circuitBreakerKill {
			poolLog.Warn("[BondedPool] Circuit-breaker KILL — force-closing master conn after %d failures", newFails)
			master.CloseWithError(1, "circuit breaker: stream exhaustion")
		}
		return nil, fmt.Errorf("open stream on bonded conn: %w", err)
	}

	// Success — reset circuit breaker.
	if old := p.masterFail.Swap(0); old > 0 {
		poolLog.Info("[BondedPool] Master conn recovered (was fails=%d)", old)
	}

	// Track stream against first non-nil entry (for stats).
	p.mu.RLock()
	var trackEntry *PoolEntry
	if len(p.entries) > 0 {
		trackEntry = p.entries[0]
	}
	p.mu.RUnlock()
	if trackEntry != nil {
		trackEntry.ActiveStreams.Add(1)
	}

	poolLog.Info("[BondedPool] Opened stream (active lanes: %d)", p.LaneCount())
	return &trackedStream{
		Conn:  wrapQUICStream(stream, master),
		entry: trackEntry,
	}, nil
}

// openStreamLegacy is the pre-bonding implementation used when no master
// QUIC connection has been registered yet (startup race window).
func (p *TunnelPool) openStreamLegacy() (net.Conn, error) {
	p.mu.RLock()
	entries := make([]*PoolEntry, len(p.entries))
	copy(entries, p.entries)
	p.mu.RUnlock()

	if len(entries) == 0 {
		return nil, fmt.Errorf("no active tunnels")
	}

	var best *PoolEntry
	bestScore := int32(math.MaxInt32)

	for _, e := range entries {
		if e.Conn == nil {
			continue
		}
		select {
		case <-e.Conn.Context().Done():
			continue
		default:
		}

		fails := e.FailCount.Load()
		if fails >= circuitBreakerThreshold {
			continue
		}
		score := e.ActiveStreams.Load() + fails*100
		if best == nil || score < bestScore {
			bestScore = score
			best = e
		}
	}

	if best == nil {
		return nil, fmt.Errorf("no healthy tunnels available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), streamTimeout)
	defer cancel()

	stream, err := best.Conn.OpenStreamSync(ctx)
	if err != nil {
		newFails := best.FailCount.Add(1)
		if newFails >= circuitBreakerKill {
			best.Conn.CloseWithError(1, "circuit breaker: stream exhaustion")
		}
		return nil, fmt.Errorf("open stream on %s: %w", best.Label, err)
	}

	if old := best.FailCount.Swap(0); old > 0 {
		poolLog.Info("[CB] %s recovered (was fails=%d)", best.Label, old)
	}
	best.ActiveStreams.Add(1)

	return &trackedStream{
		Conn:  wrapQUICStream(stream, best.Conn),
		entry: best,
	}, nil
}

// ActiveCount returns the number of active lanes in bonded mode,
// or the number of alive legacy QUIC connections in compatibility mode.
func (p *TunnelPool) ActiveCount() int {
	p.masterMu.RLock()
	master := p.masterConn
	bc := p.bondConn
	p.masterMu.RUnlock()

	if master != nil {
		select {
		case <-master.Context().Done():
			return 0
		default:
			if bc != nil {
				return bc.LaneCount()
			}
			return 1
		}
	}

	// Legacy mode: count alive QUIC connections.
	p.mu.RLock()
	defer p.mu.RUnlock()
	count := 0
	for _, e := range p.entries {
		if e.Conn != nil {
			select {
			case <-e.Conn.Context().Done():
			default:
				count++
			}
		}
	}
	return count
}

// CloseAll closes the master QUIC connection and the BondedPacketConn.
func (p *TunnelPool) CloseAll() {
	p.once.Do(func() { close(p.done) })

	p.masterMu.Lock()
	master := p.masterConn
	bc := p.bondConn
	p.masterConn = nil
	p.bondConn = nil
	p.masterMu.Unlock()

	if master != nil {
		master.CloseWithError(0, "tunnel stopped")
	}
	if bc != nil {
		bc.Close()
	}

	// Legacy cleanup.
	p.mu.Lock()
	for _, e := range p.entries {
		if e.Conn != nil {
			e.Conn.CloseWithError(0, "tunnel stopped")
		}
	}
	p.entries = nil
	p.mu.Unlock()

	poolLog.Info("[BondedPool] All connections closed")
}

// healthMonitorLoop logs pool status every 5s and evicts dead legacy entries.
func (p *TunnelPool) healthMonitorLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			p.masterMu.RLock()
			master := p.masterConn
			bc := p.bondConn
			p.masterMu.RUnlock()

			if master != nil {
				select {
				case <-master.Context().Done():
					poolLog.Warn("[BondedPool] Master QUIC connection is dead")
				default:
					lanes := 0
					if bc != nil {
						lanes = bc.LaneCount()
					}
					poolLog.Info("[BondedPool] Master conn alive (lanes=%d, masterFails=%d)",
						lanes, p.masterFail.Load())
				}
				continue
			}

			// Legacy mode health check.
			p.mu.Lock()
			live := p.entries[:0]
			for _, e := range p.entries {
				if e.Conn == nil {
					live = append(live, e)
					continue
				}
				select {
				case <-e.Conn.Context().Done():
					if e.ActiveStreams.Load() <= 0 {
						poolLog.Warn("[Health] Evicting dead entry %s", e.Label)
						continue
					}
				default:
				}
				poolLog.Info("[Health] %s: alive (fails=%d, streams=%d)",
					e.Label, e.FailCount.Load(), e.ActiveStreams.Load())
				live = append(live, e)
			}
			p.entries = live
			p.mu.Unlock()
		}
	}
}

// ─── trackedStream ─────────────────────────────────────────────────────────────

type trackedStream struct {
	net.Conn
	entry *PoolEntry
	once  sync.Once
}

func (s *trackedStream) Close() error {
	s.once.Do(func() {
		if s.entry != nil {
			if s.entry.ActiveStreams.Add(-1) < 0 {
				s.entry.ActiveStreams.Store(0)
			}
		}
	})
	return s.Conn.Close()
}

// ─── quicStreamConn ────────────────────────────────────────────────────────────

type quicStreamConn struct {
	quic.Stream
	local  net.Addr
	remote net.Addr
}

func wrapQUICStream(s quic.Stream, conn quic.Connection) net.Conn {
	return &quicStreamConn{
		Stream: s,
		local:  conn.LocalAddr(),
		remote: conn.RemoteAddr(),
	}
}

func (c *quicStreamConn) LocalAddr() net.Addr  { return c.local }
func (c *quicStreamConn) RemoteAddr() net.Addr { return c.remote }

// ─── Compatibility shims ───────────────────────────────────────────────────────

func (p *TunnelPool) Sessions() []*PoolEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*PoolEntry, len(p.entries))
	copy(out, p.entries)
	return out
}

// LatencyMs returns a placeholder.
func (e *PoolEntry) LatencyMs() int64 { return 0 }

// next is used by legacy callers; no-op in bonded pool.
var _next uint32

func init() { atomic.StoreUint32(&_next, 0) }
