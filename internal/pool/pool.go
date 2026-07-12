// Package pool manages the multi-QUIC connection pool.
//
// Architecture (Autonomous Multi-Artery Orchestrator):
//
//   SOCKS5/HTTP proxy → pool.OpenStream()
//                           │
//                           ▼
//              [ ECF/BLEST Scheduler (latency-aware) ]
//                           │
//       ┌───────────┬───────┴───────┬───────────┐
//       ▼           ▼               ▼           ▼
//   [Artery 0]  [Artery 1]     [Artery 2]  [Artery N-1]
//   (QUIC conn) (QUIC conn)   (QUIC conn) (QUIC conn)
//   State:ACTIVE             State:ACTIVE
//     SRTT:25ms   SRTT:80ms    SRTT:40ms    SRTT:120ms
//     Loss:0%     Loss:2%      Loss:0%      Loss:5%→QUARANTINED
//
// Each WebRTC channel operates its own independent QUIC connection.
// The pool distributes incoming proxy stream requests across all
// ACTIVE arteries using ECF (Earliest Completion First) scheduling
// with BLEST (Blocking Estimation) gating to avoid saturated paths.
//
// State Machine per artery (Asymmetric Hysteresis):
//   ACTIVE → QUARANTINED → REVIVING → SHADOW → ACTIVE
//   Demotion:  SRTT > 1.5× median OR loss > 5% (5s window)
//   Promotion: SRTT within 1.2× best for 3 consecutive windows
//   Cooldown:  30s hard lock after demotion
//
// The orchestrator runs three background loops:
//   - Telemetry (500ms): EWMA RTT collection + loss tracking
//   - Hysteresis (1s):   Demotion/promotion evaluation
//   - Dead detection (3s): Connection liveness + revival pipeline
package pool

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/salman/ble-webrtc-tun/internal/artery"
	"github.com/salman/ble-webrtc-tun/internal/logger"
)

var poolLog = logger.New("pool")

// TunnelPool manages independent QUIC connections — one per WebRTC lane.
// OpenStream() selects a healthy connection via ECF scheduling and opens a
// multiplexed QUIC stream directly over that connection.
//
// Internally delegates to artery.ArteryPool for intelligent scheduling.
type TunnelPool struct {
	arteryPool *artery.ArteryPool
	mu         sync.RWMutex
	done       chan struct{}
	once       sync.Once
}

// New creates a new empty TunnelPool backed by the artery orchestrator.
func New() *TunnelPool {
	p := &TunnelPool{
		arteryPool: artery.NewArteryPool(),
		done:       make(chan struct{}),
	}
	go p.healthLogLoop()
	return p
}

// ArteryPool returns the underlying artery pool for orchestrator integration.
func (p *TunnelPool) ArteryPool() *artery.ArteryPool {
	return p.arteryPool
}

// Register adds a fully-established QUIC connection to the pool under
// the given label. The connection immediately starts carrying proxy traffic
// as an ACTIVE artery.
func (p *TunnelPool) Register(label string, qconn quic.Connection) {
	p.arteryPool.Register(label, qconn, 0)
	poolLog.Info("[Pool] Registered %s (total: %d)", label, p.arteryPool.TotalCount())
}

// RegisterWithIndex registers a QUIC connection with a specific pair index.
func (p *TunnelPool) RegisterWithIndex(label string, qconn quic.Connection, pairIndex int) *artery.Artery {
	a := p.arteryPool.Register(label, qconn, pairIndex)
	poolLog.Info("[Pool] Registered %s (pair=%d, total: %d)", label, pairIndex, p.arteryPool.TotalCount())
	return a
}

// Unregister removes a connection from the pool. Active proxy traffic
// is immediately steered to remaining arteries by the ECF scheduler.
func (p *TunnelPool) Unregister(label string) {
	p.arteryPool.Unregister(label)
}

// OpenStream opens a QUIC stream on the artery with the lowest expected
// completion time (ECF scheduling with BLEST gating).
// Thread-safe and backward-compatible with the original round-robin API.
func (p *TunnelPool) OpenStream() (net.Conn, error) {
	return p.arteryPool.OpenStream()
}

// ActiveCount returns the number of ACTIVE arteries that are alive.
func (p *TunnelPool) ActiveCount() int {
	return p.arteryPool.ActiveCount()
}

// CloseAll closes all QUIC connections in the pool.
func (p *TunnelPool) CloseAll() {
	p.once.Do(func() { close(p.done) })
	p.arteryPool.CloseAll()
}

// GetConnection returns the QUIC connection for a specific label, or nil.
func (p *TunnelPool) GetConnection(label string) quic.Connection {
	return p.arteryPool.GetConnection(label)
}

// GetArtery returns the artery for a specific label, or nil.
func (p *TunnelPool) GetArtery(label string) *artery.Artery {
	return p.arteryPool.GetArtery(label)
}

// GetArteryStatuses returns health snapshots for all arteries (admin panel).
func (p *TunnelPool) GetArteryStatuses() []artery.ArteryStatus {
	return p.arteryPool.GetArteryStatuses()
}

// healthLogLoop logs pool status every 5s and monitors artery health.
func (p *TunnelPool) healthLogLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			active := p.arteryPool.ActiveCount()
			total := p.arteryPool.TotalCount()
			alive := p.arteryPool.AliveCount()

			if total == 0 {
				continue
			}

			// Log per-artery health at INFO level
			for _, a := range p.arteryPool.AllArteries() {
				state := a.State()
				srtt := a.SRTT()
				loss := a.PacketLoss()
				streams := a.ActiveStreams()
				poolLog.Info("[Health] %s: state=%s srtt=%dms loss=%.1f%% streams=%d",
					a.Label, state, srtt.Milliseconds(), loss*100, streams)
			}

			poolLog.Info("[Health] Summary: %d/%d active, %d/%d alive",
				active, total, alive, total)
		}
	}
}

// ── Compatibility shims ───────────────────────────────────────────────

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
	arteries := p.arteryPool.AllArteries()
	out := make([]*PoolEntry, 0, len(arteries))
	for _, a := range arteries {
		entry := &PoolEntry{
			Conn:    a.QConn(),
			Label:   a.Label,
			addedAt: time.Now(),
		}
		entry.ActiveStreams.Store(a.ActiveStreams())
		entry.FailCount.Store(int32(a.PacketLoss() * 100))
		out = append(out, entry)
	}
	return out
}
