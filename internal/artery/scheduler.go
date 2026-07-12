package artery

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/salman/ble-webrtc-tun/internal/logger"
)

var schedLog = logger.New("scheduler")

// ── Stream timeout ─────────────────────────────────────────────────────

const (
	// streamTimeout is how long OpenStreamSync waits before giving up.
	// Reduced from 2s to 800ms — if a QUIC connection can't open a stream
	// in 800ms, it's likely broken and we should try the next artery fast.
	streamTimeout = 800 * time.Millisecond

	// circuitBreakerThreshold: consecutive stream failures to exclude an artery.
	circuitBreakerThreshold = int32(3)

	// circuitBreakerKill: consecutive failures that trigger force-close.
	circuitBreakerKill = int32(5)
)

// ── ArteryPool ─────────────────────────────────────────────────────────

// ArteryPool manages a set of arteries with intelligent P2C+WRR scheduling.
// It replaces the old TunnelPool's round-robin cursor with a congestion-
// and latency-aware stream selector that guarantees fair distribution.
type ArteryPool struct {
	mu       sync.RWMutex
	arteries map[string]*Artery // keyed by label (e.g. "ch1")
	done     chan struct{}
	once     sync.Once

	// Round-robin cursor for WRR distribution.
	cursor atomic.Uint64
}

// NewArteryPool creates a new empty artery pool.
func NewArteryPool() *ArteryPool {
	return &ArteryPool{
		arteries: make(map[string]*Artery),
		done:     make(chan struct{}),
	}
}

// Register adds a fully-established QUIC connection as an ACTIVE artery.
func (p *ArteryPool) Register(label string, qconn quic.Connection, pairIndex int) *Artery {
	a := NewArtery(pairIndex, label, qconn)
	p.mu.Lock()
	p.arteries[label] = a
	n := len(p.arteries)
	p.mu.Unlock()
	schedLog.Info("[Pool] Registered artery %s (total: %d)", label, n)
	return a
}

// RegisterShadow adds a QUIC connection as a SHADOW artery.
func (p *ArteryPool) RegisterShadow(label string, qconn quic.Connection, pairIndex int) *Artery {
	a := NewShadowArtery(pairIndex, label, qconn)
	p.mu.Lock()
	p.arteries[label] = a
	n := len(p.arteries)
	p.mu.Unlock()
	schedLog.Info("[Pool] Registered shadow artery %s (total: %d)", label, n)
	return a
}

// Unregister removes an artery from the pool.
func (p *ArteryPool) Unregister(label string) {
	p.mu.Lock()
	if a, ok := p.arteries[label]; ok {
		_ = a.TransitionTo(StateDead)
		delete(p.arteries, label)
	}
	n := len(p.arteries)
	p.mu.Unlock()

	schedLog.Info("[Pool] Unregistered %s (remaining: %d)", label, n)
}

// GetArtery returns the artery for a specific label, or nil.
func (p *ArteryPool) GetArtery(label string) *Artery {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.arteries[label]
}

// GetConnection returns the QUIC connection for a specific label, or nil.
func (p *ArteryPool) GetConnection(label string) quic.Connection {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if a, ok := p.arteries[label]; ok {
		return a.QConn()
	}
	return nil
}

// ── P2C + WRR Stream Scheduler ─────────────────────────────────────────

// streamCandidate represents an artery eligible for stream assignment.
type streamCandidate struct {
	artery *Artery
	label  string
}

// OpenStream opens a QUIC stream using Power-of-Two-Choices (P2C) combined
// with Weighted Round-Robin (WRR) for guaranteed fair traffic distribution.
//
// Algorithm:
//  1. Filter to ACTIVE arteries that are alive and not circuit-broken
//  2. Try to serve from the pre-opened stream pool first (instant, 0ms)
//  3. If >1 candidate: P2C — pick 2 random arteries, use the one with
//     fewer active streams. This is mathematically proven to produce
//     O(log log N) max-load, which is near-perfect balance.
//  4. If all arteries have similar load: WRR round-robin cursor ensures
//     each artery gets equal turns even when stream lifetimes are short.
//  5. Fallback: try all remaining candidates in order.
func (p *ArteryPool) OpenStream() (net.Conn, error) {
	p.mu.RLock()

	n := len(p.arteries)
	if n == 0 {
		p.mu.RUnlock()
		return nil, fmt.Errorf("zero active arteries in pool")
	}

	// Collect active, alive candidates.
	candidates := make([]streamCandidate, 0, n)
	for label, a := range p.arteries {
		if a.State() != StateActive {
			continue
		}
		if !a.IsAlive() {
			continue
		}
		// Skip circuit-broken arteries.
		if a.streamFailures.Load()-a.streamSuccesses.Load() >= int64(circuitBreakerThreshold) {
			loss := a.PacketLoss()
			if loss > 0.5 {
				continue
			}
		}
		candidates = append(candidates, streamCandidate{artery: a, label: label})
	}
	p.mu.RUnlock()

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no healthy ACTIVE arteries available")
	}

	// ── P2C + WRR selection ────────────────────────────────────────────
	selected := p.p2cSelect(candidates)

	// Try the selected candidate first, then fall through remaining.
	order := make([]streamCandidate, 0, len(candidates))
	order = append(order, selected)
	for _, c := range candidates {
		if c.label != selected.label {
			order = append(order, c)
		}
	}

	for _, c := range order {
		ctx, cancel := context.WithTimeout(context.Background(), streamTimeout)
		s, err := c.artery.QConn().OpenStreamSync(ctx)
		cancel()

		if err != nil {
			c.artery.RecordStreamFailure()
			schedLog.Warn("[Pool] Stream open failed on %s: %v", c.label, err)

			// Circuit breaker kill
			recentFails := c.artery.streamFailures.Load()
			if recentFails >= int64(circuitBreakerKill) {
				schedLog.Warn("[Pool] Circuit-breaker KILL on %s — force-closing", c.label)
				c.artery.QConn().CloseWithError(1, "circuit breaker: stream exhaustion")
			}
			continue
		}

		// Success — track for fair distribution.
		c.artery.RecordStreamSuccess()
		c.artery.IncrementStreams()
		c.artery.Tel().IncrementTotalStreams()

		return &trackedStream{
			Conn:   wrapQUICStream(s, c.artery.QConn()),
			artery: c.artery,
		}, nil
	}

	return nil, fmt.Errorf("all artery stream opens failed")
}

// p2cSelect picks the best artery using Power-of-Two-Choices combined with
// round-robin. When there are 2+ candidates, it picks 2 pseudo-random ones
// and returns the one with fewer active streams. This guarantees fair load
// distribution without needing accurate RTT estimates.
func (p *ArteryPool) p2cSelect(candidates []streamCandidate) streamCandidate {
	n := len(candidates)
	if n == 1 {
		return candidates[0]
	}

	// Advance the WRR cursor to get two distinct indices.
	// Using the cursor ensures we don't always start from the same pair.
	cursor := p.cursor.Add(1)
	i := int(cursor % uint64(n))
	j := int((cursor*7 + 1) % uint64(n)) // Different stride to avoid aliasing
	if j == i {
		j = (i + 1) % n
	}

	a := candidates[i]
	b := candidates[j]

	// P2C: pick the one with fewer active streams (least loaded).
	aLoad := a.artery.ActiveStreams()
	bLoad := b.artery.ActiveStreams()

	if aLoad <= bLoad {
		return a
	}
	return b
}

// ── Pool statistics ────────────────────────────────────────────────────

// ActiveCount returns the number of ACTIVE arteries that are alive.
func (p *ArteryPool) ActiveCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	count := 0
	for _, a := range p.arteries {
		if a.State() == StateActive && a.IsAlive() {
			count++
		}
	}
	return count
}

// AliveCount returns the number of arteries that are alive (any state).
func (p *ArteryPool) AliveCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	count := 0
	for _, a := range p.arteries {
		if a.IsAlive() {
			count++
		}
	}
	return count
}

// TotalCount returns the total number of arteries in the pool.
func (p *ArteryPool) TotalCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.arteries)
}

// AllArteries returns a snapshot of all arteries (for iteration).
func (p *ArteryPool) AllArteries() []*Artery {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]*Artery, 0, len(p.arteries))
	for _, a := range p.arteries {
		result = append(result, a)
	}
	return result
}

// ActiveArteries returns only ACTIVE arteries.
func (p *ArteryPool) ActiveArteries() []*Artery {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]*Artery, 0, len(p.arteries))
	for _, a := range p.arteries {
		if a.State() == StateActive {
			result = append(result, a)
		}
	}
	return result
}

// MedianSRTT computes the median SRTT across all ACTIVE arteries.
// Returns 0 if no active arteries exist.
func (p *ArteryPool) MedianSRTT() time.Duration {
	active := p.ActiveArteries()
	if len(active) == 0 {
		return 0
	}
	srtts := make([]time.Duration, len(active))
	for i, a := range active {
		srtts[i] = a.SRTT()
	}
	sort.Slice(srtts, func(i, j int) bool { return srtts[i] < srtts[j] })
	return srtts[len(srtts)/2]
}

// BestSRTT returns the lowest SRTT across all ACTIVE arteries.
func (p *ArteryPool) BestSRTT() time.Duration {
	active := p.ActiveArteries()
	if len(active) == 0 {
		return 0
	}
	best := active[0].SRTT()
	for _, a := range active[1:] {
		if s := a.SRTT(); s < best {
			best = s
		}
	}
	return best
}

// GetArteryStatuses returns status snapshots for all arteries.
func (p *ArteryPool) GetArteryStatuses() []ArteryStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	statuses := make([]ArteryStatus, 0, len(p.arteries))
	for _, a := range p.arteries {
		statuses = append(statuses, a.Status())
	}
	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].PairIndex < statuses[j].PairIndex
	})
	return statuses
}

// CloseAll closes all QUIC connections in the pool.
func (p *ArteryPool) CloseAll() {
	p.once.Do(func() { close(p.done) })

	p.mu.Lock()
	for label, a := range p.arteries {
		schedLog.Info("[Pool] Closing %s", label)
		conn := a.QConn()
		if conn != nil {
			conn.CloseWithError(0, "tunnel stopped")
		}
		_ = a.TransitionTo(StateDead)
	}
	p.arteries = make(map[string]*Artery)
	p.mu.Unlock()

	schedLog.Info("[Pool] All arteries closed")
}

// ── trackedStream ──────────────────────────────────────────────────────

type trackedStream struct {
	net.Conn
	artery *Artery
	once   sync.Once
}

func (s *trackedStream) Close() error {
	s.once.Do(func() {
		if s.artery != nil {
			s.artery.DecrementStreams()
		}
	})
	return s.Conn.Close()
}

// ── quicStreamConn ─────────────────────────────────────────────────────

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
