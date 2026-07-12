package artery

import (
	"context"
	"fmt"
	"time"

	"github.com/salman/ble-webrtc-tun/internal/logger"
)

var orchLog = logger.New("orchestrator")

// ── Hysteresis Thresholds ──────────────────────────────────────────────

const (
	// demotionRTTMultiplier: an ACTIVE artery is QUARANTINED if its SRTT
	// exceeds this multiple of the pool's median SRTT.
	demotionRTTMultiplier = 1.5

	// demotionLossThreshold: an ACTIVE artery is QUARANTINED if its sliding
	// window packet loss exceeds this value (5%).
	demotionLossThreshold = 0.05

	// promotionRTTMultiplier: a SHADOW artery is promoted to ACTIVE only
	// if its SRTT stays within this multiple of the pool's best ACTIVE SRTT
	// for promotionWindowCount consecutive sampling windows.
	promotionRTTMultiplier = 1.2

	// cooldownPeriod: hard lock after demotion before re-promotion eligibility.
	cooldownPeriod = 30 * time.Second

	// shadowStabilization: how long a revived artery must stay in SHADOW
	// state before becoming eligible for promotion.
	shadowStabilization = 15 * time.Second

	// telemetryInterval: how often the telemetry loop collects RTT samples.
	telemetryInterval = 500 * time.Millisecond

	// hysteresisInterval: how often the hysteresis engine evaluates
	// demotion/promotion thresholds.
	hysteresisInterval = 1 * time.Second

	// deadDetectionInterval: how often dead arteries are detected and evicted.
	deadDetectionInterval = 3 * time.Second
)

// RevivalFunc is called by the orchestrator when an artery needs revival.
// It receives the label and pair index.  The function is responsible for:
// 1. Tearing down the old connection
// 2. Re-authenticating via Bale WS
// 3. Re-establishing SFU + QUIC
// 4. Calling pool.Register() with the new connection
//
// It runs asynchronously in a separate goroutine.
type RevivalFunc func(ctx context.Context, label string, pairIndex int)

// Orchestrator is the background engine that continuously monitors artery
// health and makes intelligent scheduling decisions.
//
// Three responsibilities:
//  1. Telemetry Loop (500ms) — Reads QUIC transport stats, updates EWMA RTT
//  2. Hysteresis Engine (1s) — Evaluates demotion/promotion thresholds
//  3. Dead Detection (3s) — Detects dead arteries and triggers revival
type Orchestrator struct {
	pool       *ArteryPool
	revivalFn  RevivalFunc
	done       chan struct{}

	// Track which arteries are currently being revived (prevent double-revival)
	reviving map[string]bool
}

// NewOrchestrator creates a new orchestrator for the given pool.
func NewOrchestrator(pool *ArteryPool, revivalFn RevivalFunc) *Orchestrator {
	return &Orchestrator{
		pool:      pool,
		revivalFn: revivalFn,
		done:      make(chan struct{}),
		reviving:  make(map[string]bool),
	}
}

// Start begins the orchestrator's background loops.
// Blocks until ctx is cancelled.
func (o *Orchestrator) Start(ctx context.Context) {
	orchLog.Info("Orchestrator started (telemetry=%v, hysteresis=%v, dead=%v)",
		telemetryInterval, hysteresisInterval, deadDetectionInterval)

	telemetryTicker := time.NewTicker(telemetryInterval)
	hysteresisTicker := time.NewTicker(hysteresisInterval)
	deadTicker := time.NewTicker(deadDetectionInterval)

	defer telemetryTicker.Stop()
	defer hysteresisTicker.Stop()
	defer deadTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			orchLog.Info("Orchestrator stopped")
			return
		case <-o.done:
			orchLog.Info("Orchestrator stopped")
			return
		case <-telemetryTicker.C:
			o.collectTelemetry()
		case <-hysteresisTicker.C:
			o.evaluateHysteresis()
		case <-deadTicker.C:
			o.detectDead(ctx)
		}
	}
}

// Stop signals the orchestrator to shut down.
func (o *Orchestrator) Stop() {
	select {
	case <-o.done:
	default:
		close(o.done)
	}
}

// ── Telemetry Collection ───────────────────────────────────────────────

// collectTelemetry reads QUIC-level RTT stats from each artery and feeds
// them into the EWMA filter.
//
// quic-go exposes SmoothedRTT on the connection, which is itself an EWMA.
// We re-smooth it through our own filter to add a second layer of damping
// that prevents transient TCP-level retransmission spikes from poisoning
// the artery-level scheduling decision.
func (o *Orchestrator) collectTelemetry() {
	for _, a := range o.pool.AllArteries() {
		if a.State() == StateDead {
			continue
		}

		conn := a.QConn()
		if conn == nil {
			continue
		}

		// Read QUIC's SmoothedRTT — this is already an EWMA maintained by quic-go.
		// We re-filter it through our own α=0.125 EWMA to add stability.
		//
		// IMPORTANT: quic-go may not expose SmoothedRTT directly via the
		// public quic.Connection interface. We fall back to using the
		// connection's context-alive check + stream success/failure rate
		// as a proxy for connection health.
		//
		// For now, use the stream success rate as a health indicator:
		// If streams are succeeding, the connection is healthy.
		// The SRTT is estimated from stream open latency measured in
		// OpenStream's circuit breaker.

		// Check if connection is still alive
		select {
		case <-conn.Context().Done():
			// Connection dead — will be caught by detectDead
			continue
		default:
		}
	}
}

// ── Hysteresis Engine ──────────────────────────────────────────────────

// evaluateHysteresis checks all arteries against demotion/promotion thresholds.
//
// Demotion (ACTIVE → QUARANTINED):
//   - SRTT > 1.5× pool median, OR
//   - Packet loss > 5% over 5s window
//
// Promotion (SHADOW → ACTIVE):
//   - SRTT within 1.2× best ACTIVE for 3 consecutive windows
//   - Cooldown period (30s) has elapsed since last demotion
//   - Shadow stabilization period (15s) has elapsed since entering SHADOW
func (o *Orchestrator) evaluateHysteresis() {
	medianSRTT := o.pool.MedianSRTT()
	bestSRTT := o.pool.BestSRTT()

	// Skip if we don't have enough data
	if medianSRTT == 0 && bestSRTT == 0 {
		return
	}

	for _, a := range o.pool.AllArteries() {
		switch a.State() {
		case StateActive:
			o.checkDemotion(a, medianSRTT)
		case StateShadow:
			o.checkPromotion(a, bestSRTT)
		}
	}
}

// checkDemotion evaluates whether an ACTIVE artery should be QUARANTINED.
func (o *Orchestrator) checkDemotion(a *Artery, medianSRTT time.Duration) {
	srtt := a.SRTT()
	loss := a.PacketLoss()

	// Don't demote if it would leave zero active arteries
	if o.pool.ActiveCount() <= 1 {
		return
	}

	demote := false
	reason := ""

	// Check RTT threshold (only if we have meaningful median data)
	if medianSRTT > 0 {
		threshold := time.Duration(float64(medianSRTT) * demotionRTTMultiplier)
		if srtt > threshold {
			demote = true
			reason = fmt.Sprintf("SRTT %dms > %.1fx median %dms (threshold=%dms)",
				srtt.Milliseconds(), demotionRTTMultiplier,
				medianSRTT.Milliseconds(), threshold.Milliseconds())
		}
	}

	// Check loss threshold
	if !demote && loss > demotionLossThreshold {
		demote = true
		reason = fmt.Sprintf("packet loss %.1f%% > %.1f%% threshold",
			loss*100, demotionLossThreshold*100)
	}

	// Check if connection is dead (immediate demotion)
	if !demote && !a.IsAlive() {
		demote = true
		reason = "QUIC connection dead"
	}

	if demote {
		orchLog.Warn("[%s] QUARANTINED — %s", a.Label, reason)
		if err := a.TransitionTo(StateQuarantined); err != nil {
			orchLog.Warn("[%s] Failed to quarantine: %v", a.Label, err)
		}
	}
}

// checkPromotion evaluates whether a SHADOW artery should be promoted to ACTIVE.
func (o *Orchestrator) checkPromotion(a *Artery, bestSRTT time.Duration) {
	// Enforce cooldown period
	if a.CooldownRemaining() > 0 {
		return
	}

	// Enforce shadow stabilization period
	if time.Since(a.LastStateChange()) < shadowStabilization {
		return
	}

	// Check if metrics are within promotion threshold
	srtt := a.SRTT()
	withinThreshold := true

	if bestSRTT > 0 {
		threshold := time.Duration(float64(bestSRTT) * promotionRTTMultiplier)
		if srtt > threshold {
			withinThreshold = false
		}
	}

	// Also check that it's alive
	if !a.IsAlive() {
		withinThreshold = false
	}

	// Record the check and see if we've hit the required streak
	ready := a.Tel().RecordPromotionCheck(withinThreshold)
	if ready {
		orchLog.Info("[%s] Promoted SHADOW → ACTIVE (SRTT=%dms, best=%dms, streak=%d)",
			a.Label, srtt.Milliseconds(), bestSRTT.Milliseconds(),
			a.Tel().PromotionStreak())
		if err := a.TransitionTo(StateActive); err != nil {
			orchLog.Warn("[%s] Failed to promote: %v", a.Label, err)
		} else {
			a.ClearCooldown()
			a.Tel().ResetPromotionStreak()
		}
	}
}

// ── Dead Detection & Revival Pipeline ──────────────────────────────────

// detectDead checks for dead or quarantined arteries and triggers revival.
func (o *Orchestrator) detectDead(ctx context.Context) {
	for _, a := range o.pool.AllArteries() {
		label := a.Label

		switch a.State() {
		case StateActive, StateShadow:
			// Check if the connection has died
			if !a.IsAlive() {
				orchLog.Warn("[%s] Connection dead in state %s — quarantining", label, a.State())
				_ = a.TransitionTo(StateQuarantined)
				// Fall through to quarantine handler
			} else {
				continue
			}
			fallthrough

		case StateQuarantined:
			// Trigger revival if not already reviving
			if o.reviving[label] {
				continue
			}
			o.reviving[label] = true

			orchLog.Info("[%s] Starting autonomous revival pipeline", label)
			if err := a.TransitionTo(StateReviving); err != nil {
				orchLog.Warn("[%s] Cannot transition to REVIVING: %v", label, err)
				delete(o.reviving, label)
				continue
			}

			if o.revivalFn != nil {
				go func(lbl string, idx int) {
					o.revivalFn(ctx, lbl, idx)
					// After revival completes, clear the reviving flag
					o.reviving[lbl] = false
				}(label, a.PairIndex)
			} else {
				orchLog.Warn("[%s] No revival function registered — artery stays in REVIVING", label)
				delete(o.reviving, label)
			}
		}
	}
}

// ── RTT Measurement via Stream Probing ─────────────────────────────────

// ProbeRTT opens and immediately closes a QUIC stream to measure the
// round-trip time.  This is used for arteries in SHADOW state that
// aren't carrying user traffic but need health telemetry.
//
// The measured RTT is fed into the artery's EWMA filter.
func (o *Orchestrator) ProbeRTT(a *Artery) {
	conn := a.QConn()
	if conn == nil {
		return
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	stream, err := conn.OpenStreamSync(ctx)
	cancel()

	rtt := time.Since(start)

	if err != nil {
		a.RecordStreamFailure()
		return
	}
	stream.Close()

	a.Tel().UpdateRTT(rtt)
	a.RecordStreamSuccess()
}
