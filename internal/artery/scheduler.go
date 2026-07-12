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
	streamTimeout = 2 * time.Second

	// circuitBreakerThreshold: consecutive stream failures to exclude an artery.
	circuitBreakerThreshold = int32(3)

	// circuitBreakerKill: consecutive failures that trigger force-close.
	circuitBreakerKill = int32(5)
)

// ── ArteryPool ─────────────────────────────────────────────────────────

// ArteryPool manages a set of arteries with intelligent ECF scheduling.
// It replaces the old TunnelPool's round-robin cursor with a congestion-
// and latency-aware stream selector.
type ArteryPool struct {
	mu       sync.RWMutex
	arteries map[string]*Artery // keyed by label (e.g. "ch1")
	done     chan struct{}
	once     sync.Once

	// Fallback cursor for when all arteries have equal metrics.
	cursor atomic.Uint32
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

// ── ECF/BLEST Stream Scheduler ─────────────────────────────────────────

// OpenStream opens a QUIC stream on the artery with the lowest expected
// completion time (Earliest Completion First), with BLEST gating to skip
// arteries whose congestion windows are saturated.
//
// Algorithm:
//  1. Filter to ACTIVE arteries that are alive
//  2. Skip arteries that have exceeded the circuit breaker threshold
//  3. For each candidate, compute Expected Delivery Time:
//     ECF = SRTT + (ActiveStreams × latencyPenaltyPerStream)
//  4. BLEST gate: skip if ActiveStreams > threshold AND slower alternatives exist
//  5. Select the artery with minimum ECF
func (p *ArteryPool) OpenStream() (net.Conn, error) {
	p.mu.RLock()

	n := len(p.arteries)
	if n == 0 {
		p.mu.RUnlock()
		return nil, fmt.Errorf("zero active arteries in pool")
	}

	// Collect active, alive candidates
	type candidate struct {
		artery   *Artery
		label    string
		ecf      time.Duration
	}

	candidates := make([]candidate, 0, n)
	for label, a := range p.arteries {
		// Only ACTIVE arteries carry traffic
		if a.State() != StateActive {
			continue
		}

		// Skip dead connections
		if !a.IsAlive() {
			continue
		}

		// Skip circuit-broken arteries
		if a.streamFailures.Load()-a.streamSuccesses.Load() >= int64(circuitBreakerThreshold) {
			// Use consecutive fail logic: if recent failures dominate
			loss := a.PacketLoss()
			if loss > 0.5 {
				continue
			}
		}

		// ECF computation:
		// Expected delivery = SRTT + penalty for congestion (more active streams = slower)
		srtt := a.SRTT()
		activeStreams := a.ActiveStreams()

		// Penalty: each active stream adds ~2ms expected queuing delay
		penalty := time.Duration(activeStreams) * 2 * time.Millisecond
		ecf := srtt + penalty

		candidates = append(candidates, candidate{
			artery: a,
			label:  label,
			ecf:    ecf,
		})
	}
	p.mu.RUnlock()

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no healthy ACTIVE arteries available")
	}

	// Sort by ECF (lowest first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ecf < candidates[j].ecf
	})

	// BLEST gating: try the best candidate first, but skip if its streams
	// are heavily saturated AND a close second exists.
	bestIdx := 0
	if len(candidates) > 1 {
		best := candidates[0]
		second := candidates[1]

		// If best has >50 active streams and second is within 2× ECF,
		// prefer the second to avoid congestion
		if best.artery.ActiveStreams() > 50 && second.ecf < best.ecf*2 {
			bestIdx = 1
		}
	}

	// Try each candidate in ECF order starting from bestIdx
	for i := bestIdx; i < len(candidates); i++ {
		c := candidates[i]

		ctx, cancel := context.WithTimeout(context.Background(), streamTimeout)
		stream, err := c.artery.QConn().OpenStreamSync(ctx)
		cancel()

		if err != nil {
			c.artery.RecordStreamFailure()
			schedLog.Warn("[Pool] Stream open failed on %s (ECF=%dms): %v",
				c.label, c.ecf.Milliseconds(), err)

			// Circuit breaker kill
			recentFails := c.artery.streamFailures.Load()
			if recentFails >= int64(circuitBreakerKill) {
				schedLog.Warn("[Pool] Circuit-breaker KILL on %s — force-closing", c.label)
				c.artery.QConn().CloseWithError(1, "circuit breaker: stream exhaustion")
			}
			continue
		}

		// Success
		c.artery.RecordStreamSuccess()
		c.artery.IncrementStreams()

		return &trackedStream{
			Conn:   wrapQUICStream(stream, c.artery.QConn()),
			artery: c.artery,
		}, nil
	}

	return nil, fmt.Errorf("all artery stream opens failed")
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
