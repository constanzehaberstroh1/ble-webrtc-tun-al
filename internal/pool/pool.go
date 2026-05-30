// Package pool manages a set of QUIC connections (one per WebRTC channel)
// and provides flow-pinned load balancing for proxy streams.
//
// QUIC Regulation Engine:
//   - Each channel = one quic.Connection over an OpusPacketConn
//   - Load balancing: "least loaded" by active stream count (no latency heuristics)
//   - Flow pinning: each proxy TCP connection is locked to one QUIC connection
//   - No HoL blocking: a dropped RTP packet pauses only the affected QUIC stream,
//     not the entire tunnel
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

// TunnelPool manages multiple QUIC connections and distributes proxy streams
// using a "least active streams" load balancer.
type TunnelPool struct {
	mu       sync.RWMutex
	entries  []*PoolEntry
	done     chan struct{}
	once     sync.Once
}

// PoolEntry wraps one QUIC connection with stream-count tracking.
type PoolEntry struct {
	Conn         quic.Connection
	Label        string
	ActiveStreams atomic.Int32 // streams currently open on this connection
	FailCount    atomic.Int32
	addedAt      time.Time
}

// New creates a new empty TunnelPool with a background health monitor.
func New() *TunnelPool {
	p := &TunnelPool{done: make(chan struct{})}
	go p.healthMonitorLoop()
	return p
}

// Add registers a new QUIC connection in the pool.
func (p *TunnelPool) Add(conn quic.Connection, label string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.entries = append(p.entries, &PoolEntry{
		Conn:    conn,
		Label:   label,
		addedAt: time.Now(),
	})
	poolLog.Info("Added QUIC connection %s (total: %d)", label, len(p.entries))
}

// Remove removes a specific QUIC connection from the pool.
func (p *TunnelPool) Remove(conn quic.Connection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, e := range p.entries {
		if e.Conn == conn {
			p.entries = append(p.entries[:i], p.entries[i+1:]...)
			poolLog.Info("Removed QUIC connection %s (remaining: %d)", e.Label, len(p.entries))
			return
		}
	}
}

// OpenStream opens a QUIC stream on the least-loaded connection.
//
// "Least loaded" = fewest active streams. This ensures new TCP connections
// spread evenly across channels. Flow pinning guarantees each proxy request
// stays on one QUIC connection so QUIC's per-stream ordering works correctly.
//
// Returns a trackedStream that decrements ActiveStreams on Close().
func (p *TunnelPool) OpenStream() (net.Conn, error) {
	p.mu.RLock()
	entries := make([]*PoolEntry, len(p.entries))
	copy(entries, p.entries)
	p.mu.RUnlock()

	if len(entries) == 0 {
		return nil, fmt.Errorf("no active tunnels")
	}

	// Find healthy entries and pick the one with fewest active streams
	var best *PoolEntry
	for _, e := range entries {
		if e.Conn == nil {
			continue
		}
		// Check if connection is still alive
		select {
		case <-e.Conn.Context().Done():
			continue // connection closed
		default:
		}
		if best == nil || e.ActiveStreams.Load() < best.ActiveStreams.Load() {
			best = e
		}
	}

	if best == nil {
		return nil, fmt.Errorf("all %d tunnels are closed", len(entries))
	}

	// Open stream with a short timeout to avoid hanging on a dead connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := best.Conn.OpenStreamSync(ctx)
	if err != nil {
		best.FailCount.Add(1)
		poolLog.Warn("Stream open failed on %s (fails=%d): %v",
			best.Label, best.FailCount.Load(), err)
		return nil, fmt.Errorf("open stream on %s: %w", best.Label, err)
	}

	if best.FailCount.Load() > 0 {
		best.FailCount.Store(0)
	}
	best.ActiveStreams.Add(1)
	poolLog.Info("Opened stream on %s (active: %d)", best.Label, best.ActiveStreams.Load())

	return &trackedStream{
		Conn:  wrapQUICStream(stream, best.Conn),
		entry: best,
	}, nil
}

// ActiveCount returns the number of QUIC connections that are still alive.
func (p *TunnelPool) ActiveCount() int {
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

// CloseAll closes all QUIC connections and stops the health monitor.
func (p *TunnelPool) CloseAll() {
	p.once.Do(func() { close(p.done) })
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.entries {
		if e.Conn != nil {
			e.Conn.CloseWithError(0, "tunnel stopped")
		}
	}
	p.entries = nil
	poolLog.Info("All QUIC connections closed")
}

// healthMonitorLoop logs pool status every 3 seconds.
func (p *TunnelPool) healthMonitorLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			p.mu.RLock()
			for _, e := range p.entries {
				select {
				case <-e.Conn.Context().Done():
					poolLog.Warn("[%s] QUIC connection dead (active streams: %d)", e.Label, e.ActiveStreams.Load())
				default:
					poolLog.Info("[%s] QUIC alive (active streams: %d)", e.Label, e.ActiveStreams.Load())
				}
			}
			p.mu.RUnlock()
		}
	}
}

// ─── trackedStream ────────────────────────────────────────────────────────────

// trackedStream wraps a net.Conn and decrements the parent entry's
// ActiveStreams counter exactly once on Close().
type trackedStream struct {
	net.Conn
	entry *PoolEntry
	once  sync.Once
}

func (s *trackedStream) Close() error {
	s.once.Do(func() {
		if s.entry.ActiveStreams.Add(-1) < 0 {
			s.entry.ActiveStreams.Store(0)
		}
		poolLog.Info("Stream closed on %s (active: %d)", s.entry.Label, s.entry.ActiveStreams.Load())
	})
	return s.Conn.Close()
}

// ─── quicStreamConn ───────────────────────────────────────────────────────────

// quicStreamConn adds LocalAddr/RemoteAddr to quic.Stream so it satisfies net.Conn.
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

// ─── Compatibility shim (legacy yamux callers) ────────────────────────────────
// These stubs allow old code that hasn't been migrated yet to compile.

// Sessions returns nil (QUIC pool has no yamux sessions).
func (p *TunnelPool) Sessions() []*PoolEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*PoolEntry, len(p.entries))
	copy(out, p.entries)
	return out
}

// LatencyMs returns a placeholder (QUIC pool does not measure latency separately).
func (e *PoolEntry) LatencyMs() int64 { return 0 }
