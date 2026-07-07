// Package quicconn — reorder_buffer.go
//
// Sliding-window priority-queue for reassembling packets that arrive out of
// order due to varying per-lane latencies in the BondedPacketConn.
//
// Design decisions:
//   - Window size 64 is intentionally small: at 1140 bytes/pkt and 5 lanes,
//     a 64-slot window covers ~73 KB of in-flight data — more than enough to
//     absorb inter-lane jitter without accumulating queue delay.
//   - The 15ms slide timeout is a hard upper bound so that a single dropped
//     packet on one lane doesn't stall all other data indefinitely.
//   - All public methods are safe for concurrent use by multiple lane readers.
package quicconn

import (
	"sync"
	"time"
)

const (
	reorderWindowSize = 64   // number of in-flight sequence slots
	slideTimeoutMs    = 15   // ms to wait for a gap before forcing a slide
	maxSeqID          = 1<<16 // uint16 wrap-around boundary
)

// reorderSlot holds one buffered packet.
type reorderSlot struct {
	data []byte
	set  bool
}

// reorderBuffer is a lock-protected sliding-window queue.
// Packets are inserted by sequence ID; the consumer reads them in order.
type reorderBuffer struct {
	mu           sync.Mutex
	cond         *sync.Cond
	slots        [reorderWindowSize]reorderSlot
	nextExpected uint16 // sequence ID the consumer is waiting for
	closed       bool

	// Deduplication bitmask for hedged packets (IS_HEDGED flag).
	// A simple 65536-bit map; bit N is set when seq N has been delivered.
	seen [8192]uint8 // 8192 bytes × 8 bits = 65536 bits
}

func newReorderBuffer() *reorderBuffer {
	rb := &reorderBuffer{}
	rb.cond = sync.NewCond(&rb.mu)
	return rb
}

// Insert places a packet into the sliding window at its sequence position.
// Returns false if the packet is a duplicate (hedged copy already seen).
// Thread-safe; called from multiple lane reader goroutines.
func (rb *reorderBuffer) Insert(seq uint16, data []byte, isHedged bool) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.closed {
		return false
	}

	// Deduplicate hedged packets by bitmask.
	if isHedged {
		byteIdx := seq / 8
		bitIdx := seq % 8
		if rb.seen[byteIdx]&(1<<bitIdx) != 0 {
			return false // already delivered
		}
	}

	// Calculate slot index relative to the current window base.
	diff := int(seq) - int(rb.nextExpected)
	if diff < 0 {
		// Wrap-around correction for uint16 overflow.
		diff += maxSeqID
	}

	if diff >= reorderWindowSize {
		// Packet is outside the current window (too far ahead or old).
		// Drop it; the FEC layer should reconstruct any gap this causes.
		return false
	}

	slotIdx := (int(rb.nextExpected) + diff) % reorderWindowSize
	if !rb.slots[slotIdx].set {
		rb.slots[slotIdx] = reorderSlot{data: data, set: true}
		rb.cond.Signal()
	}
	return true
}

// Next blocks until the next sequential packet is ready or 15ms elapses.
// On timeout, the window slides forward by 1 (accepting the gap).
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
			rb.slots[slotIdx] = reorderSlot{} // clear
			// Mark as seen in dedup bitmask.
			byteIdx := rb.nextExpected / 8
			bitIdx := rb.nextExpected % 8
			rb.seen[byteIdx] |= 1 << bitIdx
			rb.nextExpected++
			return data
		}

		// Wait for a signal or 15ms timeout.
		waitDone := make(chan struct{}, 1)
		go func() {
			time.Sleep(slideTimeoutMs * time.Millisecond)
			rb.cond.Signal()
			waitDone <- struct{}{}
		}()

		rb.cond.Wait()

		// Drain the timer goroutine notification.
		select {
		case <-waitDone:
		default:
		}

		// Re-check: if still not set after 15ms, force a slide.
		if !rb.slots[slotIdx].set {
			rb.nextExpected++ // slide past the gap
			// The FEC layer will have attempted reconstruction; if it
			// couldn't, QUIC will detect the missing datagram and handle
			// it at the transport layer (BBR keeps the window open).
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

// Reset resets the buffer to a clean state for reuse (e.g. after reconnect).
func (rb *reorderBuffer) Reset(startSeq uint16) {
	rb.mu.Lock()
	rb.slots = [reorderWindowSize]reorderSlot{}
	rb.seen = [8192]uint8{}
	rb.nextExpected = startSeq
	rb.closed = false
	rb.mu.Unlock()
}
