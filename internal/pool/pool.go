package pool

import (
	"fmt"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"
)

var poolLog = logger.New("pool")

// TunnelPool manages multiple yamux sessions and provides
// latency-aware load balancing across them.
// Sessions with lower latency receive more traffic.
// Dead sessions are detected proactively via periodic pings.
type TunnelPool struct {
	mu       sync.RWMutex
	sessions []*PoolEntry
	next     uint32
	done     chan struct{}
	once     sync.Once
}

// PoolEntry wraps a yamux session with metadata and latency tracking.
type PoolEntry struct {
	Session    *yamux.Session
	Index      int
	Label      string        // e.g. "ch1", "ch2"
	LatencyMs  atomic.Int64  // last measured RTT in milliseconds
	FailCount  atomic.Int32  // consecutive stream open failures
	LastPingAt atomic.Int64  // unix millis of last successful ping
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
// Uses latency-weighted selection: prefers sessions with lower latency.
// Falls back to round-robin if latency data is unavailable.
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

	// Collect healthy sessions with their latencies
	type candidate struct {
		entry   *PoolEntry
		latency int64
	}
	var candidates []candidate
	for _, e := range entries {
		if e.Session == nil || e.Session.IsClosed() {
			continue
		}
		lat := e.LatencyMs.Load()
		// Penalize sessions with recent failures
		fails := e.FailCount.Load()
		if fails > 0 {
			lat += int64(fails) * 200 // +200ms penalty per failure
		}
		candidates = append(candidates, candidate{entry: e, latency: lat})
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("all %d tunnels are closed", n)
	}

	// Weighted selection: try lowest-latency first, then fall back
	// Sort candidates by latency (simple insertion sort for small N)
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0 && candidates[j].latency < candidates[j-1].latency; j-- {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
		}
	}

	// Use weighted round-robin: sessions with 2x latency get 1/2 the traffic.
	// The base latency is the fastest session. We distribute proportionally.
	if len(candidates) > 1 {
		baseLat := candidates[0].latency
		if baseLat < 1 {
			baseLat = 1
		}
		// Use a counter to distribute — faster sessions get more turns
		counter := atomic.AddUint32(&p.next, 1)
		totalWeight := 0.0
		weights := make([]float64, len(candidates))
		for i, c := range candidates {
			w := float64(baseLat) / math.Max(float64(c.latency), 1.0)
			weights[i] = w
			totalWeight += w
		}
		// Weighted selection using the counter
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
		// Try the selected candidate first, then fall through
		reordered := make([]candidate, 0, len(candidates))
		reordered = append(reordered, candidates[selectedIdx])
		for i, c := range candidates {
			if i != selectedIdx {
				reordered = append(reordered, c)
			}
		}
		candidates = reordered
	}

	// Try each candidate in weighted order
	for _, c := range candidates {
		stream, err := c.entry.Session.Open()
		if err != nil {
			c.entry.FailCount.Add(1)
			poolLog.Warn("Stream open failed on %s (fails=%d): %v",
				c.entry.Label, c.entry.FailCount.Load(), err)
			continue
		}
		// Reset fail count on success
		if c.entry.FailCount.Load() > 0 {
			c.entry.FailCount.Store(0)
		}
		return stream, nil
	}

	return nil, fmt.Errorf("all %d tunnels failed to open stream", n)
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

// Remove removes closed sessions from the pool.
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
func (p *TunnelPool) healthMonitorLoop() {
	ticker := time.NewTicker(5 * time.Second)
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

// pingAllSessions measures RTT to all active sessions.
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
				// Ping failed — mark high latency but don't remove
				// (auto-reconnect will handle dead sessions)
				entry.LatencyMs.Store(5000) // 5s = "very slow"
				return
			}
			_ = rtt
			latencyMs := time.Since(start).Milliseconds()
			entry.LatencyMs.Store(latencyMs)
			entry.LastPingAt.Store(time.Now().UnixMilli())
		}(e)
	}
}
