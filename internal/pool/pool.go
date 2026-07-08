// Package pool manages the multi-QUIC connection pool.
//
// Architecture (connection-level multiplexing):
//
//   SOCKS5/HTTP proxy → pool.OpenStream()
//                           │
//                           ▼
//               [ Lockless Round-Robin Selector ]
//                           │
//       ┌───────────┬───────┴───────┬───────────┐
//       ▼           ▼               ▼           ▼
//   [Lane 0]    [Lane 1]       [Lane 2]    [Lane N-1]
//   (QUIC conn) (QUIC conn)   (QUIC conn) (QUIC conn)
//
// Each WebRTC channel operates its own independent QUIC connection.
// The pool distributes incoming proxy stream requests across all live
// connections using an atomic round-robin cursor — no lock contention
// on the hot path.
//
// Circuit Breaker logic (per-connection fault isolation):
//   - A connection with ≥3 consecutive stream failures is excluded.
//   - After 5 failures the connection is force-closed so the
//     TunnelManager can re-establish that specific lane.
//   - Success resets the failure counter.
package pool

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/salman/ble-webrtc-tun/internal/logger"
)

var poolLog = logger.New("pool")

// circuitBreakerThreshold is the number of consecutive stream open failures
// after which a connection is excluded from load balancing.
const circuitBreakerThreshold = int32(3)

// circuitBreakerKill is the number of consecutive failures that triggers a
// force-close of the QUIC connection, causing the TunnelManager to re-dial.
const circuitBreakerKill = int32(5)

// streamTimeout is how long OpenStreamSync waits before giving up.
const streamTimeout = 2 * time.Second

// TunnelPool manages independent QUIC connections — one per WebRTC lane.
// OpenStream() selects a healthy connection via round-robin and opens a
// multiplexed QUIC stream directly over that connection.
type TunnelPool struct {
	mu    sync.RWMutex
	conns map[string]*connEntry // Maps account labels to isolated QUIC sessions
	done  chan struct{}
	once  sync.Once

	// Atomic round-robin cursor — lockless on the hot path.
	cursor uint32
}

// connEntry wraps a QUIC connection with health and stats metadata.
type connEntry struct {
	Conn          quic.Connection
	Label         string
	ActiveStreams  atomic.Int32
	FailCount     atomic.Int32
	addedAt       time.Time
}

// New creates a new empty TunnelPool with a background health monitor.
func New() *TunnelPool {
	p := &TunnelPool{
		conns: make(map[string]*connEntry),
		done:  make(chan struct{}),
	}
	go p.healthMonitorLoop()
	return p
}

// Register adds a fully-established QUIC connection to the pool under
// the given label. The connection immediately starts carrying proxy traffic.
func (p *TunnelPool) Register(label string, qconn quic.Connection) {
	p.mu.Lock()
	p.conns[label] = &connEntry{
		Conn:    qconn,
		Label:   label,
		addedAt: time.Now(),
	}
	n := len(p.conns)
	p.mu.Unlock()
	poolLog.Info("[Pool] Registered %s (total: %d)", label, n)
}

// Unregister removes a connection from the pool. Active proxy traffic
// is immediately steered to remaining connections by the round-robin cursor.
func (p *TunnelPool) Unregister(label string) {
	p.mu.Lock()
	delete(p.conns, label)
	n := len(p.conns)
	p.mu.Unlock()
	poolLog.Info("[Pool] Unregistered %s (remaining: %d)", label, n)
}

// OpenStream opens a QUIC stream on a healthy connection selected by
// atomic round-robin. Thread-safe and lockless on the cursor increment.
func (p *TunnelPool) OpenStream() (net.Conn, error) {
	p.mu.RLock()
	n := len(p.conns)
	if n == 0 {
		p.mu.RUnlock()
		return nil, fmt.Errorf("zero active multiplex lines available in the pool map")
	}

	// Extract active keys under read-lock safety limits.
	keys := make([]string, 0, n)
	for k := range p.conns {
		keys = append(keys, k)
	}

	// Try each connection in round-robin order, skipping unhealthy ones.
	start := atomic.AddUint32(&p.cursor, 1)
	var selected *connEntry
	var selectedLabel string

	for i := uint32(0); i < uint32(n); i++ {
		idx := (start + i) % uint32(len(keys))
		label := keys[idx]
		entry := p.conns[label]

		// Skip dead connections.
		select {
		case <-entry.Conn.Context().Done():
			continue
		default:
		}

		// Skip circuit-broken connections.
		if entry.FailCount.Load() >= circuitBreakerThreshold {
			continue
		}

		selected = entry
		selectedLabel = label
		break
	}
	p.mu.RUnlock()

	if selected == nil {
		return nil, fmt.Errorf("no healthy connections available")
	}

	// Open a multiplexed stream on the selected connection.
	ctx, cancel := context.WithTimeout(context.Background(), streamTimeout)
	defer cancel()

	stream, err := selected.Conn.OpenStreamSync(ctx)
	if err != nil {
		newFails := selected.FailCount.Add(1)
		poolLog.Warn("[Pool] Stream open failed on %s (fails=%d): %v", selectedLabel, newFails, err)
		if newFails >= circuitBreakerKill {
			poolLog.Warn("[Pool] Circuit-breaker KILL on %s — force-closing", selectedLabel)
			selected.Conn.CloseWithError(1, "circuit breaker: stream exhaustion")
		}
		return nil, fmt.Errorf("open stream on %s: %w", selectedLabel, err)
	}

	// Success — reset circuit breaker.
	if old := selected.FailCount.Swap(0); old > 0 {
		poolLog.Info("[Pool] %s recovered (was fails=%d)", selectedLabel, old)
	}
	selected.ActiveStreams.Add(1)

	return &trackedStream{
		Conn:  wrapQUICStream(stream, selected.Conn),
		entry: selected,
	}, nil
}

// ActiveCount returns the number of healthy (alive) QUIC connections.
func (p *TunnelPool) ActiveCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	count := 0
	for _, e := range p.conns {
		select {
		case <-e.Conn.Context().Done():
		default:
			count++
		}
	}
	return count
}

// CloseAll closes all QUIC connections in the pool.
func (p *TunnelPool) CloseAll() {
	p.once.Do(func() { close(p.done) })

	p.mu.Lock()
	for label, e := range p.conns {
		poolLog.Info("[Pool] Closing %s", label)
		e.Conn.CloseWithError(0, "tunnel stopped")
	}
	p.conns = make(map[string]*connEntry)
	p.mu.Unlock()

	poolLog.Info("[Pool] All connections closed")
}

// GetConnection returns the QUIC connection for a specific label, or nil.
func (p *TunnelPool) GetConnection(label string) quic.Connection {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if e, ok := p.conns[label]; ok {
		return e.Conn
	}
	return nil
}

// healthMonitorLoop logs pool status every 5s and evicts dead entries.
func (p *TunnelPool) healthMonitorLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			p.mu.Lock()
			for label, e := range p.conns {
				select {
				case <-e.Conn.Context().Done():
					poolLog.Warn("[Health] Evicting dead connection %s", label)
					delete(p.conns, label)
				default:
					poolLog.Info("[Health] %s: alive (fails=%d, streams=%d)",
						label, e.FailCount.Load(), e.ActiveStreams.Load())
				}
			}
			p.mu.Unlock()
		}
	}
}

// ── trackedStream ─────────────────────────────────────────────────────────────

type trackedStream struct {
	net.Conn
	entry *connEntry
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

// ── quicStreamConn ────────────────────────────────────────────────────────────

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

// ── Compatibility shims ───────────────────────────────────────────────────────

// PoolEntry is kept for backward-compatibility with status reporting APIs.
type PoolEntry struct {
	Conn         quic.Connection
	Label        string
	ActiveStreams atomic.Int32
	FailCount    atomic.Int32
	addedAt      time.Time
}

// LatencyMs returns a placeholder.
func (e *PoolEntry) LatencyMs() int64 { return 0 }

// Sessions returns a list of all registered connections as PoolEntry slices.
func (p *TunnelPool) Sessions() []*PoolEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*PoolEntry, 0, len(p.conns))
	for _, e := range p.conns {
		out = append(out, &PoolEntry{
			Conn:    e.Conn,
			Label:   e.Label,
			addedAt: e.addedAt,
		})
	}
	return out
}
