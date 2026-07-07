// Package quicconn — reorder_buffer.go
//
// Sliding-window priority-queue for reassembling packets that arrive
// out-of-order due to varying per-lane latencies in the BondedPacketConn.
//
// Design decisions (revised):
//   - Window size 256 (up from 64): at 1136 bytes/packet and 5 bonded lanes,
//     a 256-slot window covers ~290 KB of in-flight data, safely absorbing
//     inter-lane jitter even at multi-megabit aggregate speeds.
//   - Adaptive slide timeout replaces the fixed 15ms: the timeout tracks the
//     observed inter-lane jitter (EWMA) and adjusts between 5ms and 100ms.
//     Low jitter → tight 5ms timeout for minimum added latency.
//     High jitter → wider window to give FEC time to reconstruct.
//   - Deduplication bitmask handles hedged (speculative) duplicate packets.
//   - All public methods are safe for concurrent use by multiple lane readers.
package quicconn

import (
	"math"
	"sync"
	"time"
)

const (
	// reorderWindowSize must be a power of two for clean modulo masking.
	// 256 slots at ~1136 bytes each ≈ 290 KB in-flight capacity.
	reorderWindowSize = 256

	// slideTimeoutMin / Max define the adaptive timeout bounds.
	slideTimeoutMinMs = 5   // ms — minimum: used when all lanes are in sync
	slideTimeoutMaxMs = 100 // ms — maximum: used under high inter-lane jitter

	// maxSeqID is the uint16 wrap-around boundary.
	maxSeqID = 1 << 16
)

// reorderSlot holds one buffered packet.
type reorderSlot struct {
	data []byte
	set  bool
}

// reorderBuffer is a lock-protected sliding-window queue.
//
// The consumer calls Next() to obtain packets in strict sequence order.
// Multiple lane-reader goroutines call Insert() concurrently.
type reorderBuffer struct {
	mu           sync.Mutex
	cond         *sync.Cond
	slots        [reorderWindowSize]reorderSlot
	nextExpected uint16 // sequence ID the consumer is waiting for
	closed       bool

	// Deduplication bitmask for hedged (IS_HEDGED) packets.
	// 8192 bytes × 8 bits = 65536 bits — one bit per uint16 seq.
	seen [8192]uint8

	// Adaptive slide timeout state.
	// avgJitterMs tracks the exponential moving average of the per-packet
	// inter-arrival gap (i.e. how spread out arrivals are across lanes).
	avgJitterMs float64 // EWMA, in milliseconds

	// Last time a packet was inserted (used to compute inter-arrival gap).
	lastInsert time.Time
}

func newReorderBuffer() *reorderBuffer {
	rb := &reorderBuffer{
		avgJitterMs: float64(slideTimeoutMinMs),
		lastInsert:  time.Now(),
	}
	rb.cond = sync.NewCond(&rb.mu)
	return rb
}

// Insert places a packet into the sliding window at its sequence position.
//
//   - isHedged: if true, deduplication bitmask is checked before inserting.
//   - Returns false if the packet is a duplicate or outside the window.
//
// Thread-safe; called from multiple lane-reader goroutines simultaneously.
func (rb *reorderBuffer) Insert(seq uint16, data []byte, isHedged bool) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.closed {
		return false
	}

	// ── Deduplication for hedged packets ────────────────────────────────
	byteIdx := seq / 8
	bitIdx := seq % 8
	if isHedged && (rb.seen[byteIdx]&(1<<bitIdx)) != 0 {
		return false // duplicate hedged copy — discard
	}

	// ── Update adaptive jitter EWMA ───────────────────────────────────
	now := time.Now()
	if !rb.lastInsert.IsZero() {
		gapMs := float64(now.Sub(rb.lastInsert).Milliseconds())
		if gapMs > 0 && gapMs < 500 { // ignore outliers from idle periods
			const alpha = 0.15
			rb.avgJitterMs = rb.avgJitterMs*(1-alpha) + gapMs*alpha
		}
	}
	rb.lastInsert = now

	// ── Check window bounds ───────────────────────────────────────────
	diff := int(seq) - int(rb.nextExpected)
	if diff < 0 {
		diff += maxSeqID
	}
	if diff >= reorderWindowSize {
		// Too far ahead or too old — drop.
		return false
	}

	slotIdx := (int(rb.nextExpected) + diff) % reorderWindowSize
	if !rb.slots[slotIdx].set {
		rb.slots[slotIdx] = reorderSlot{data: data, set: true}
		rb.cond.Signal()
	}
	return true
}

// Next blocks until the next sequential packet is ready.
//
// If the expected slot is empty after the adaptive timeout fires, the window
// slides forward by one position (gap accepted — FEC should have covered it).
// Returns nil only when Close() has been called.
func (rb *reorderBuffer) Next() []byte {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for {
		if rb.closed {
			return nil
		}

		slotIdx := int(rb.nextExpected) % reorderWindowSize
		if rb.slots[slotIdx].set {
			data := rb.slots[slotIdx].data
			rb.slots[slotIdx] = reorderSlot{} // clear slot

			// Mark seq as seen in dedup bitmask.
			byteIdx := rb.nextExpected / 8
			bitIdx := rb.nextExpected % 8
			rb.seen[byteIdx] |= 1 << bitIdx

			rb.nextExpected++
			return data
		}

		// ── Adaptive timeout calculation ──────────────────────────────
		// slideTimeout = clamp(avgJitter × 1.5, 5ms, 100ms)
		jitterTimeout := rb.avgJitterMs * 1.5
		jitterTimeout = math.Max(float64(slideTimeoutMinMs), jitterTimeout)
		jitterTimeout = math.Min(float64(slideTimeoutMaxMs), jitterTimeout)
		slideTimeout := time.Duration(jitterTimeout) * time.Millisecond

		// Wake up after the adaptive timeout to slide past a gap.
		timedOut := make(chan struct{}, 1)
		go func(d time.Duration) {
			time.Sleep(d)
			rb.cond.Signal()
			timedOut <- struct{}{}
		}(slideTimeout)

		rb.cond.Wait()

		// Drain the background goroutine's notification.
		select {
		case <-timedOut:
		default:
		}

		// Re-check: if still empty after timeout, slide the window.
		if !rb.slots[slotIdx].set {
			// Gap persisted — force slide. FEC should have reconstructed;
			// if not, QUIC will handle it via its own loss detection.
			rb.avgJitterMs = math.Min(rb.avgJitterMs*1.2, float64(slideTimeoutMaxMs))
			rb.nextExpected++
		} else {
			// Packet arrived — reward with tighter timeout next time.
			rb.avgJitterMs = math.Max(rb.avgJitterMs*0.9, float64(slideTimeoutMinMs))
		}
	}
}

// Close unblocks any waiting Next() calls.
func (rb *reorderBuffer) Close() {
	rb.mu.Lock()
	rb.closed = true
	rb.cond.Broadcast()
	rb.mu.Unlock()
}

// Reset resets the buffer to a clean state (e.g. after reconnect).
func (rb *reorderBuffer) Reset(startSeq uint16) {
	rb.mu.Lock()
	rb.slots = [reorderWindowSize]reorderSlot{}
	rb.seen = [8192]uint8{}
	rb.nextExpected = startSeq
	rb.closed = false
	rb.avgJitterMs = float64(slideTimeoutMinMs)
	rb.lastInsert = time.Now()
	rb.mu.Unlock()
}
