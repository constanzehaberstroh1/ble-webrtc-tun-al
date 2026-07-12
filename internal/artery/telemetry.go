package artery

import (
	"sync"
	"sync/atomic"
	"time"
)

// ── EWMA Constants ─────────────────────────────────────────────────────

const (
	// ewmaAlpha is the smoothing factor for EWMA RTT computation.
	// α = 0.125 (same as TCP's SRTT calculation in RFC 6298).
	// Lower values = smoother, more resistant to transient spikes.
	ewmaAlpha = 0.125

	// ewmaBeta is the smoothing factor for RTT variance.
	// β = 0.25 (RFC 6298 standard).
	ewmaBeta = 0.25

	// lossWindowDuration is the sliding window size for packet loss calculation.
	lossWindowDuration = 5 * time.Second

	// lossWindowBuckets divides the window into discrete time slots.
	// 10 buckets × 500ms = 5s window.
	lossWindowBuckets = 10

	// lossBucketDuration is the width of each bucket in the loss ring buffer.
	lossBucketDuration = lossWindowDuration / lossWindowBuckets

	// promotionWindowCount is the number of consecutive sampling windows that
	// a SHADOW artery must stay within the promotion threshold before being
	// promoted to ACTIVE.
	promotionWindowCount = 3
)

// ── Telemetry ──────────────────────────────────────────────────────────

// Telemetry tracks real-time health metrics for a single artery.
// All methods are thread-safe.
type Telemetry struct {
	mu sync.RWMutex

	// EWMA Smoothed RTT and variance
	srtt        time.Duration
	rttVariance time.Duration
	rttSamples  int64 // Total number of RTT samples received

	// Sliding window packet loss (ring buffer)
	lossBuckets      [lossWindowBuckets]lossBucket
	currentBucketIdx int
	lastBucketTime   time.Time

	// Promotion stability tracking: counts consecutive windows where
	// metrics are within the promotion threshold.
	promotionStreak int

	// Total streams ever assigned to this artery (for WRR weight tracking).
	// Atomic — no lock needed.
	totalStreams atomic.Int64
}

// lossBucket records success/failure counts for a time slot.
type lossBucket struct {
	successes int
	failures  int
}

// NewTelemetry creates a Telemetry with sensible initial values.
func NewTelemetry() *Telemetry {
	return &Telemetry{
		srtt:           50 * time.Millisecond, // Optimistic initial estimate
		rttVariance:    25 * time.Millisecond,
		lastBucketTime: time.Now(),
	}
}

// ── RTT Tracking ───────────────────────────────────────────────────────

// UpdateRTT applies an EWMA-filtered RTT sample.
//
// SRTT_new = (1 - α) × SRTT_old + α × RTT_sample
// RTTVAR_new = (1 - β) × RTTVAR_old + β × |SRTT_old - RTT_sample|
//
// On the very first sample, SRTT is set directly (RFC 6298 §2.2).
func (t *Telemetry) UpdateRTT(sample time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.rttSamples == 0 {
		// First sample — set directly
		t.srtt = sample
		t.rttVariance = sample / 2
	} else {
		// EWMA update
		diff := t.srtt - sample
		if diff < 0 {
			diff = -diff
		}
		t.rttVariance = time.Duration(
			(1-ewmaBeta)*float64(t.rttVariance) + ewmaBeta*float64(diff),
		)
		t.srtt = time.Duration(
			(1-ewmaAlpha)*float64(t.srtt) + ewmaAlpha*float64(sample),
		)
	}
	t.rttSamples++
}

// SRTT returns the current Smoothed RTT.
func (t *Telemetry) SRTT() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.srtt
}

// RTTVariance returns the current RTT variance (jitter indicator).
func (t *Telemetry) RTTVariance() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.rttVariance
}

// RTTSamples returns the total number of RTT samples collected.
func (t *Telemetry) RTTSamples() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.rttSamples
}

// ── Packet Loss Tracking ───────────────────────────────────────────────

// RecordStreamResult records a stream open success or failure into the
// sliding window ring buffer.
func (t *Telemetry) RecordStreamResult(success bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.advanceBuckets()

	if success {
		t.lossBuckets[t.currentBucketIdx].successes++
	} else {
		t.lossBuckets[t.currentBucketIdx].failures++
	}
}

// PacketLoss returns the packet loss ratio (0.0–1.0) over the sliding window.
func (t *Telemetry) PacketLoss() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	totalSuccess := 0
	totalFailure := 0
	for _, b := range t.lossBuckets {
		totalSuccess += b.successes
		totalFailure += b.failures
	}
	total := totalSuccess + totalFailure
	if total == 0 {
		return 0
	}
	return float64(totalFailure) / float64(total)
}

// advanceBuckets rotates the ring buffer forward if enough time has passed.
// Must be called with t.mu held.
func (t *Telemetry) advanceBuckets() {
	now := time.Now()
	elapsed := now.Sub(t.lastBucketTime)

	// How many buckets to advance
	advance := int(elapsed / lossBucketDuration)
	if advance <= 0 {
		return
	}

	// Cap at full window (don't loop more than once around the ring)
	if advance > lossWindowBuckets {
		advance = lossWindowBuckets
	}

	// Zero out the buckets we're advancing past
	for i := 0; i < advance; i++ {
		t.currentBucketIdx = (t.currentBucketIdx + 1) % lossWindowBuckets
		t.lossBuckets[t.currentBucketIdx] = lossBucket{}
	}

	t.lastBucketTime = now
}

// ── Promotion Streak ───────────────────────────────────────────────────

// RecordPromotionCheck records whether this artery is within the promotion
// threshold.  Returns true if the streak has reached promotionWindowCount.
func (t *Telemetry) RecordPromotionCheck(withinThreshold bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if withinThreshold {
		t.promotionStreak++
	} else {
		t.promotionStreak = 0
	}
	return t.promotionStreak >= promotionWindowCount
}

// ResetPromotionStreak clears the promotion stability counter.
func (t *Telemetry) ResetPromotionStreak() {
	t.mu.Lock()
	t.promotionStreak = 0
	t.mu.Unlock()
}

// PromotionStreak returns the current consecutive-good-window count.
func (t *Telemetry) PromotionStreak() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.promotionStreak
}

// ── Composite Health Score ─────────────────────────────────────────────

// HealthScore returns a composite score (lower is better) combining
// SRTT and loss.  Used for quick comparisons and logging.
//
// Score = SRTT_ms + (PacketLoss × 10000)
//
// A pair with 100ms RTT and 0% loss scores 100.
// A pair with 100ms RTT and 5% loss scores 600.
func (t *Telemetry) HealthScore() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return float64(t.srtt.Milliseconds()) + (float64(t.PacketLossUnlocked()) * 10000)
}

// PacketLossUnlocked returns loss without acquiring the lock (caller must hold it).
func (t *Telemetry) PacketLossUnlocked() float64 {
	totalSuccess := 0
	totalFailure := 0
	for _, b := range t.lossBuckets {
		totalSuccess += b.successes
		totalFailure += b.failures
	}
	total := totalSuccess + totalFailure
	if total == 0 {
		return 0
	}
	return float64(totalFailure) / float64(total)
}

// ── Total Stream Counter ───────────────────────────────────────────────

// IncrementTotalStreams atomically increments the total stream counter.
// Called every time a stream is assigned to this artery's connection.
func (t *Telemetry) IncrementTotalStreams() {
	t.totalStreams.Add(1)
}

// TotalStreams returns the total number of streams ever assigned.
func (t *Telemetry) TotalStreams() int64 {
	return t.totalStreams.Load()
}
