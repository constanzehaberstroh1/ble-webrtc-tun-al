package pool

import (
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/salman/ble-webrtc-tun/internal/logger"
)

var poolLog = logger.New("pool")

// TunnelPool manages multiple yamux sessions and provides
// stream-count aware load balancing across them.
//
// REGULATION ENGINE improvements:
//   - Stream-count load balancing: new connections are routed to the session
//     with the fewest active streams (flow pinning), not just lowest latency.
//     This prevents one saturated channel from absorbing all new connections.
//   - trackedStream: wraps yamux streams to automatically decrement the
//     per-session ActiveStreams counter when the stream is closed.
//   - Faster health checks: 2s ping interval (was 5s) and 5s reconnect
//     ping (was 10s) for faster dead-session detection.
type TunnelPool struct {
	mu       sync.RWMutex
	sessions []*PoolEntry
	next     uint32
	done     chan struct{}
	once     sync.Once
}

// PoolEntry wraps a yamux session with metadata and performance tracking.
type PoolEntry struct {
	Session      *yamux.Session
	Index        int
	Label        string       // e.g. "ch1", "ch2"
	LatencyMs    atomic.Int64 // last measured RTT in milliseconds
	FailCount    atomic.Int32 // consecutive stream open failures
	LastPingAt   atomic.Int64 // unix millis of last successful ping
	ActiveStreams atomic.Int32 // number of currently open streams (REGULATION ENGINE)
}

// New creates a new empty TunnelPool and starts the background health monitor.
func New() *TunnelPool {
	p := &TunnelPool{
		done: make(chan struct{}),
	}
	go p.healthMonitorLoop()
	return p
}

// Add appends a session to the pool.
func (p *TunnelPool) Add(session *yamux.Session, label string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	idx := len(p.sessions)
	entry := &PoolEntry{
		Session: session,
		Index:   idx,
		Label:   label,
	}
	entry.LastPingAt.Store(time.Now().UnixMilli())
	entry.LatencyMs.Store(50) // assume 50ms initial latency
	entry.ActiveStreams.Store(0)
	p.sessions = append(p.sessions, entry)
	poolLog.Info("Added session %s (total: %d)", label, len(p.sessions))
}

// Replace swaps an old session with a new one in-place (for reconnection).
// If oldSession is nil or not found, behaves like Add.
func (p *TunnelPool) Replace(oldSession *yamux.Session, newSession *yamux.Session, label string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.sessions {
		if e.Session == oldSession {
			e.Session = newSession
			e.FailCount.Store(0)
			e.LatencyMs.Store(50)
			e.LastPingAt.Store(time.Now().UnixMilli())
			e.ActiveStreams.Store(0)
			poolLog.Info("Replaced session %s (hot-swap)", label)
			return
		}
	}
	// Not found — append as new
	idx := len(p.sessions)
	entry := &PoolEntry{
		Session: newSession,
		Index:   idx,
		Label:   label,
	}
	entry.LastPingAt.Store(time.Now().UnixMilli())
	entry.LatencyMs.Store(50)
	entry.ActiveStreams.Store(0)
	p.sessions = append(p.sessions, entry)
	poolLog.Info("Added session %s via Replace (total: %d)", label, len(p.sessions))
}

// GetSession returns the next healthy session using round-robin.
// Skips closed sessions. Returns error if no sessions are available.
func (p *TunnelPool) GetSession() (*yamux.Session, error) {
	p.mu.RLock()
	n := len(p.sessions)
	if n == 0 {
		p.mu.RUnlock()
		return nil, fmt.Errorf("no active tunnels")
	}
	entries := make([]*PoolEntry, n)
	copy(entries, p.sessions)
	p.mu.RUnlock()

	// Try each session starting from next index
	start := atomic.AddUint32(&p.next, 1)
	for i := 0; i < n; i++ {
		idx := (int(start) + i) % n
		s := entries[idx].Session
		if s != nil && !s.IsClosed() {
			return s, nil
		}
	}

	return nil, fmt.Errorf("all %d tunnels are closed", n)
}

// OpenStream opens a yamux stream on the best available session.
//
// REGULATION ENGINE — Stream-Count Load Balancing:
// Selects the session with the fewest active streams (flow pinning).
// This ensures new TCP connections are distributed evenly across channels,
// preventing one channel from being saturated while others sit idle.
//
// Secondary sort: latency-weighted (sessions with 2x latency get 1/2 traffic).
//
// Returns a trackedStream that automatically decrements ActiveStreams on Close().
func (p *TunnelPool) OpenStream() (net.Conn, error) {
	p.mu.RLock()
	n := len(p.sessions)
	if n == 0 {
		p.mu.RUnlock()
		return nil, fmt.Errorf("no active tunnels")
	}
	entries := make([]*PoolEntry, n)
	copy(entries, p.sessions)
	p.mu.RUnlock()

	// Collect healthy sessions
	type candidate struct {
		entry        *PoolEntry
		latency      int64
		activeStreams int32
	}
	var candidates []candidate
	for _, e := range entries {
		if e.Session == nil || e.Session.IsClosed() {
			continue
		}
		lat := e.LatencyMs.Load()
		fails := e.FailCount.Load()
		if fails > 0 {
			lat += int64(fails) * 200 // +200ms penalty per failure
		}
		candidates = append(candidates, candidate{
			entry:        e,
			latency:      lat,
			activeStreams: e.ActiveStreams.Load(),
		})
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("all %d tunnels are closed", n)
	}

	// Primary sort: fewest active streams (flow pinning — REGULATION ENGINE)
	// Secondary sort: lowest latency
	// Simple insertion sort for small N (typically 2-8 channels)
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0; j-- {
			a, b := candidates[j], candidates[j-1]
			// Prefer fewer active streams; break ties with latency
			if a.activeStreams < b.activeStreams ||
				(a.activeStreams == b.activeStreams && a.latency < b.latency) {
				candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
			} else {
				break
			}
		}
	}

	// Apply latency-weighted selection only when active stream counts are tied
	if len(candidates) > 1 {
		// Check if top candidates have the same stream count
		topStreamCount := candidates[0].activeStreams
		var tied []candidate
		for _, c := range candidates {
			if c.activeStreams == topStreamCount {
				tied = append(tied, c)
			} else {
				break
			}
		}
		// If there's a real tie, use latency-weighted round-robin among tied candidates
		if len(tied) > 1 {
			baseLat := tied[0].latency
			if baseLat < 1 {
				baseLat = 1
			}
			counter := atomic.AddUint32(&p.next, 1)
			totalWeight := 0.0
			weights := make([]float64, len(tied))
			for i, c := range tied {
				w := float64(baseLat) / math.Max(float64(c.latency), 1.0)
				weights[i] = w
				totalWeight += w
			}
			target := math.Mod(float64(counter), totalWeight)
			cumulative := 0.0
			selectedIdx := 0
			for i, w := range weights {
				cumulative += w
				if target < cumulative {
					selectedIdx = i
					break
				}
			}
			// Put selected candidate first
			reordered := make([]candidate, 0, len(candidates))
			reordered = append(reordered, tied[selectedIdx])
			for i, c := range tied {
				if i != selectedIdx {
					reordered = append(reordered, c)
				}
			}
			// Append non-tied candidates at the end
			for _, c := range candidates {
				if c.activeStreams != topStreamCount {
					reordered = append(reordered, c)
				}
			}
			candidates = reordered
		}
	}

	// Try each candidate in sorted order
	for _, c := range candidates {
		stream, err := c.entry.Session.Open()
		if err != nil {
			c.entry.FailCount.Add(1)
			poolLog.Warn("Stream open failed on %s (fails=%d, active=%d): %v",
				c.entry.Label, c.entry.FailCount.Load(), c.entry.ActiveStreams.Load(), err)
			continue
		}
		// Reset fail count on success
		if c.entry.FailCount.Load() > 0 {
			c.entry.FailCount.Store(0)
		}
		// Increment active stream counter
		c.entry.ActiveStreams.Add(1)
		poolLog.Info("Opened stream on %s (active streams: %d, latency: %dms)",
			c.entry.Label, c.entry.ActiveStreams.Load(), c.entry.LatencyMs.Load())
		// Return a trackedStream that decrements ActiveStreams on Close()
		return &trackedStream{Conn: stream, entry: c.entry}, nil
	}

	return nil, fmt.Errorf("all %d tunnels failed to open stream", n)
}

// trackedStream wraps a yamux stream and automatically decrements the
// parent PoolEntry's ActiveStreams counter when the stream is closed.
// This gives the load balancer accurate real-time stream counts.
type trackedStream struct {
	net.Conn
	entry *PoolEntry
	once  sync.Once
}

func (s *trackedStream) Close() error {
	s.once.Do(func() {
		s.entry.ActiveStreams.Add(-1)
		if s.entry.ActiveStreams.Load() < 0 {
			s.entry.ActiveStreams.Store(0) // guard against underflow
		}
		poolLog.Info("Stream closed on %s (active streams: %d)",
			s.entry.Label, s.entry.ActiveStreams.Load())
	})
	return s.Conn.Close()
}

// ActiveCount returns the number of non-closed sessions.
func (p *TunnelPool) ActiveCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	count := 0
	for _, e := range p.sessions {
		if e.Session != nil && !e.Session.IsClosed() {
			count++
		}
	}
	return count
}

// Remove removes a specific session from the pool.
func (p *TunnelPool) Remove(session *yamux.Session) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, e := range p.sessions {
		if e.Session == session {
			p.sessions = append(p.sessions[:i], p.sessions[i+1:]...)
			poolLog.Info("Removed session %s (remaining: %d)", e.Label, len(p.sessions))
			return
		}
	}
}

// CloseAll closes all sessions in the pool and stops the health monitor.
func (p *TunnelPool) CloseAll() {
	p.once.Do(func() {
		close(p.done)
	})
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.sessions {
		if e.Session != nil {
			e.Session.Close()
		}
	}
	p.sessions = nil
	poolLog.Info("All sessions closed")
}

// Sessions returns a snapshot of all entries.
func (p *TunnelPool) Sessions() []*PoolEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*PoolEntry, len(p.sessions))
	copy(out, p.sessions)
	return out
}

// healthMonitorLoop periodically pings all sessions to measure latency
// and detect dead connections proactively.
//
// REGULATION ENGINE: 2s interval (was 5s) for faster dead-session detection.
func (p *TunnelPool) healthMonitorLoop() {
	ticker := time.NewTicker(2 * time.Second) // was 5s — faster dead detection
	defer ticker.Stop()

	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			p.pingAllSessions()
		}
	}
}

// pingAllSessions measures RTT to all active sessions and logs stream counts.
func (p *TunnelPool) pingAllSessions() {
	p.mu.RLock()
	entries := make([]*PoolEntry, len(p.sessions))
	copy(entries, p.sessions)
	p.mu.RUnlock()

	for _, e := range entries {
		if e.Session == nil || e.Session.IsClosed() {
			continue
		}
		go func(entry *PoolEntry) {
			start := time.Now()
			rtt, err := entry.Session.Ping()
			if err != nil {
				entry.LatencyMs.Store(5000) // 5s = "very slow"
				poolLog.Warn("Ping failed on %s: %v (active streams: %d)",
					entry.Label, err, entry.ActiveStreams.Load())
				return
			}
			_ = rtt
			latencyMs := time.Since(start).Milliseconds()
			entry.LatencyMs.Store(latencyMs)
			entry.LastPingAt.Store(time.Now().UnixMilli())
			poolLog.Info("Ping %s: %dms (active streams: %d)",
				entry.Label, latencyMs, entry.ActiveStreams.Load())
		}(e)
	}
}
