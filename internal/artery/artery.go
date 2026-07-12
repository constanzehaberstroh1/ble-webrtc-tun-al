// Package artery provides the Multi-Artery Virtual Pool abstraction layer.
//
// Each QUIC connection (one per Bale account pair) is wrapped in an Artery
// with a formal state machine, EWMA RTT telemetry, and sliding-window
// packet loss tracking.  The orchestrator uses these metrics to make
// scientifically-grounded scheduling and demotion/promotion decisions.
//
// State machine lifecycle:
//
//	ACTIVE ──→ QUARANTINED ──→ REVIVING ──→ SHADOW ──→ ACTIVE
//	                                           ↑
//	   (initial connect) ──────────────────────┘
//	ACTIVE/QUARANTINED ──→ DEAD  (terminal — token revoked)
package artery

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/salman/ble-webrtc-tun/internal/logger"
)

var arteryLog = logger.New("artery")

// ArteryState represents the lifecycle state of an artery.
type ArteryState string

const (
	// StateActive — fully operational, carrying live SOCKS5/HTTP payload.
	StateActive ArteryState = "ACTIVE"

	// StateShadow — connected and authenticated, but only routes background
	// keepalive PINGs.  Provides a zero-latency replacement when an ACTIVE
	// artery degrades.
	StateShadow ArteryState = "SHADOW"

	// StateQuarantined — crossed an error/latency/loss threshold.  Isolated
	// from the active data path while background diagnostics evaluate.
	StateQuarantined ArteryState = "QUARANTINED"

	// StateReviving — an asynchronous recovery goroutine is performing
	// full teardown, re-authentication, and hot re-injection.
	StateReviving ArteryState = "REVIVING"

	// StateDead — terminal state.  Token revoked, max retries exceeded,
	// or permanent failure.  Will not be revived.
	StateDead ArteryState = "DEAD"
)

// Artery wraps a single QUIC connection with health telemetry and lifecycle
// state management.  All fields protected by Lock unless noted as atomic.
type Artery struct {
	PairIndex int    // Index in the pairing array (0-based)
	Label     string // Human-readable label (e.g. "ch1")

	// State machine
	state           ArteryState
	lastStateChange time.Time
	demotionTime    *time.Time // Set when demoted; used for cooldown enforcement

	// QUIC connection (the actual data path)
	qconn quic.Connection

	// Telemetry (updated by the telemetry loop)
	telemetry *Telemetry

	// Stream counters (atomic — no lock needed for reads)
	streamSuccesses atomic.Int64
	streamFailures  atomic.Int64
	activeStreams    atomic.Int32

	// Registration metadata
	addedAt time.Time

	Lock sync.RWMutex
}

// NewArtery creates an artery in ACTIVE state with a connected QUIC connection.
func NewArtery(pairIndex int, label string, qconn quic.Connection) *Artery {
	now := time.Now()
	a := &Artery{
		PairIndex:       pairIndex,
		Label:           label,
		state:           StateActive,
		lastStateChange: now,
		qconn:           qconn,
		telemetry:       NewTelemetry(),
		addedAt:         now,
	}
	arteryLog.Info("[%s] Created (state=ACTIVE)", label)
	return a
}

// NewShadowArtery creates an artery in SHADOW state (for warm standby).
func NewShadowArtery(pairIndex int, label string, qconn quic.Connection) *Artery {
	now := time.Now()
	a := &Artery{
		PairIndex:       pairIndex,
		Label:           label,
		state:           StateShadow,
		lastStateChange: now,
		qconn:           qconn,
		telemetry:       NewTelemetry(),
		addedAt:         now,
	}
	arteryLog.Info("[%s] Created (state=SHADOW)", label)
	return a
}

// State returns the current artery state (thread-safe).
func (a *Artery) State() ArteryState {
	a.Lock.RLock()
	defer a.Lock.RUnlock()
	return a.state
}

// QConn returns the underlying QUIC connection (thread-safe).
func (a *Artery) QConn() quic.Connection {
	a.Lock.RLock()
	defer a.Lock.RUnlock()
	return a.qconn
}

// SetQConn replaces the underlying QUIC connection (used during revival).
func (a *Artery) SetQConn(qconn quic.Connection) {
	a.Lock.Lock()
	a.qconn = qconn
	a.Lock.Unlock()
}

// Telemetry returns the artery's telemetry tracker (thread-safe, never nil).
func (a *Artery) Tel() *Telemetry {
	return a.telemetry
}

// SRTT returns the smoothed round-trip time.
func (a *Artery) SRTT() time.Duration {
	return a.telemetry.SRTT()
}

// PacketLoss returns the current packet loss percentage (0.0–1.0).
func (a *Artery) PacketLoss() float64 {
	return a.telemetry.PacketLoss()
}

// IsAlive checks if the underlying QUIC connection context is still active.
func (a *Artery) IsAlive() bool {
	a.Lock.RLock()
	conn := a.qconn
	a.Lock.RUnlock()
	if conn == nil {
		return false
	}
	select {
	case <-conn.Context().Done():
		return false
	default:
		return true
	}
}

// RecordStreamSuccess increments the success counter and resets failure streak.
func (a *Artery) RecordStreamSuccess() {
	a.streamSuccesses.Add(1)
	a.telemetry.RecordStreamResult(true)
}

// RecordStreamFailure increments the failure counter.
func (a *Artery) RecordStreamFailure() {
	a.streamFailures.Add(1)
	a.telemetry.RecordStreamResult(false)
}

// ActiveStreams returns the number of in-flight streams on this artery.
func (a *Artery) ActiveStreams() int32 {
	return a.activeStreams.Load()
}

// IncrementStreams atomically increments the active stream count.
func (a *Artery) IncrementStreams() {
	a.activeStreams.Add(1)
}

// DecrementStreams atomically decrements the active stream count.
func (a *Artery) DecrementStreams() {
	if a.activeStreams.Add(-1) < 0 {
		a.activeStreams.Store(0)
	}
}

// ── State transitions ──────────────────────────────────────────────────

// TransitionTo changes the artery's state with validation.
// Returns an error if the transition is not allowed.
func (a *Artery) TransitionTo(newState ArteryState) error {
	a.Lock.Lock()
	defer a.Lock.Unlock()

	old := a.state
	if !isValidTransition(old, newState) {
		return fmt.Errorf("[%s] invalid transition %s → %s", a.Label, old, newState)
	}

	a.state = newState
	a.lastStateChange = time.Now()

	if newState == StateQuarantined {
		now := time.Now()
		a.demotionTime = &now
	}

	arteryLog.Info("[%s] %s → %s", a.Label, old, newState)
	return nil
}

// CooldownRemaining returns the time remaining before a demoted artery
// is eligible for re-promotion.  Returns 0 if no cooldown is active.
func (a *Artery) CooldownRemaining() time.Duration {
	a.Lock.RLock()
	defer a.Lock.RUnlock()
	if a.demotionTime == nil {
		return 0
	}
	elapsed := time.Since(*a.demotionTime)
	const cooldownPeriod = 30 * time.Second
	if elapsed >= cooldownPeriod {
		return 0
	}
	return cooldownPeriod - elapsed
}

// ClearCooldown clears the demotion cooldown (called after successful promotion).
func (a *Artery) ClearCooldown() {
	a.Lock.Lock()
	a.demotionTime = nil
	a.Lock.Unlock()
}

// LastStateChange returns when the last state transition occurred.
func (a *Artery) LastStateChange() time.Time {
	a.Lock.RLock()
	defer a.Lock.RUnlock()
	return a.lastStateChange
}

// ── Status snapshot for API/admin panel ─────────────────────────────────

// ArteryStatus is the JSON-serializable snapshot of an artery's health.
type ArteryStatus struct {
	PairIndex        int         `json:"pair_index"`
	Label            string      `json:"label"`
	State            ArteryState `json:"state"`
	SRTTMs           int64       `json:"srtt_ms"`
	PacketLossPct    float64     `json:"packet_loss_pct"`
	ActiveStreams     int32       `json:"active_streams"`
	StreamSuccesses  int64       `json:"stream_successes"`
	StreamFailures   int64       `json:"stream_failures"`
	CooldownSeconds  float64     `json:"cooldown_seconds"`
	LastStateChange  time.Time   `json:"last_state_change"`
	Alive            bool        `json:"alive"`
}

// Status returns a snapshot of the artery's current health for the API.
func (a *Artery) Status() ArteryStatus {
	return ArteryStatus{
		PairIndex:       a.PairIndex,
		Label:           a.Label,
		State:           a.State(),
		SRTTMs:          a.SRTT().Milliseconds(),
		PacketLossPct:   a.PacketLoss() * 100, // Convert to percentage
		ActiveStreams:    a.ActiveStreams(),
		StreamSuccesses: a.streamSuccesses.Load(),
		StreamFailures:  a.streamFailures.Load(),
		CooldownSeconds: a.CooldownRemaining().Seconds(),
		LastStateChange: a.LastStateChange(),
		Alive:           a.IsAlive(),
	}
}

// ── Transition validation ──────────────────────────────────────────────

// isValidTransition checks if a state transition is allowed.
func isValidTransition(from, to ArteryState) bool {
	switch from {
	case StateActive:
		return to == StateQuarantined || to == StateDead || to == StateShadow
	case StateShadow:
		return to == StateActive || to == StateQuarantined || to == StateDead
	case StateQuarantined:
		return to == StateReviving || to == StateDead
	case StateReviving:
		return to == StateShadow || to == StateDead
	case StateDead:
		return false // Terminal state
	}
	return false
}
