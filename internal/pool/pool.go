package pool

import (
	"fmt"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"net"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/yamux"
)

var poolLog = logger.New("pool")

// TunnelPool manages multiple yamux sessions and provides
// session-level round-robin load balancing across them.
type TunnelPool struct {
	mu       sync.RWMutex
	sessions []*PoolEntry
	next     uint32
}

// PoolEntry wraps a yamux session with metadata.
type PoolEntry struct {
	Session *yamux.Session
	Index   int
	Label   string // e.g. "ch1", "ch2"
}

// New creates a new empty TunnelPool.
func New() *TunnelPool {
	return &TunnelPool{}
}

// Add appends a session to the pool.
func (p *TunnelPool) Add(session *yamux.Session, label string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	idx := len(p.sessions)
	p.sessions = append(p.sessions, &PoolEntry{
		Session: session,
		Index:   idx,
		Label:   label,
	})
	poolLog.Info("Added session %s (total: %d)", label, len(p.sessions))
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
// Implements retry logic: if the selected session fails, tries the next.
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

	start := atomic.AddUint32(&p.next, 1)
	for i := 0; i < n; i++ {
		idx := (int(start) + i) % n
		s := entries[idx].Session
		if s == nil || s.IsClosed() {
			continue
		}
		stream, err := s.Open()
		if err != nil {
			poolLog.Warn("Stream open failed on %s: %v", entries[idx].Label, err)
			continue
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

// CloseAll closes all sessions in the pool.
func (p *TunnelPool) CloseAll() {
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
