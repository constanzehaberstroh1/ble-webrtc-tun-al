// Package dcconn wraps a Pion WebRTC DataChannel into an io.ReadWriteCloser
// suitable for use with yamux. Data is wrapped in LiveKit DataPacket protos
// so the SFU forwards it correctly.
//
// Features:
//   - ChaCha20-Poly1305 obfuscation (prevents DPI fingerprinting)
//   - Backpressure flow control (prevents SCTP buffer overflow)
//   - Write coalescing (batches small writes within 1ms window)
//   - Send serialization (prevents out-of-order dc.Send from concurrent goroutines)
package dcconn

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v3"
	"google.golang.org/protobuf/proto"

	lkproto "github.com/livekit/protocol/livekit"
)

const (
	// maxBufferedAmount is the SCTP buffered amount threshold for backpressure.
	// When the DC has more than this buffered, Write() blocks until it drains.
	maxBufferedAmount = 512 * 1024 // 512KB

	// lowBufferThreshold triggers the "buffer drained" signal.
	lowBufferThreshold = 256 * 1024 // 256KB

	// coalesceWindow is the maximum time to wait for more writes before flushing.
	// 1ms is enough to batch concurrent yamux frames without adding visible latency.
	coalesceWindow = 1 * time.Millisecond

	// coalesceMaxSize is the max coalesced payload before forced flush.
	// Keeps SCTP message size reasonable for SFU relay.
	// 16KB is the safe maximum for LiveKit/SFU DataChannel messages.
	coalesceMaxSize = 16 * 1024 // 16KB — SFU-safe, avoids fragmentation
)

// Conn adapts a WebRTC DataChannel to io.ReadWriteCloser.
type Conn struct {
	dc     *webrtc.DataChannel
	readCh chan []byte // incoming payloads
	buf    []byte      // partial read buffer
	closed atomic.Bool
	once   sync.Once

	// Obfuscation layer
	obfuscator *Obfuscator

	// Backpressure: signal when buffer drains below threshold
	writeMu     sync.Mutex
	bufferReady chan struct{} // signaled when buffered amount drops

	// Write coalescing: batch small writes into single DC sends.
	// coalesceMu protects the buffer and timer.
	coalesceMu    sync.Mutex
	coalesceBuf   []byte
	coalesceTimer *time.Timer

	// sendMu serializes ALL dc.Send() calls.
	// Without this, the coalesce timer goroutine and Write() goroutine
	// can call dc.Send() concurrently, causing out-of-order delivery
	// and "yamux: Invalid protocol version" errors.
	sendMu sync.Mutex

	bytesSent atomic.Int64
	bytesRecv atomic.Int64
}

// New creates a Conn from a publisher DataChannel.
// The caller must wire the subscriber side with HandleMessage().
// If obfuscator is nil, a disabled (passthrough) obfuscator is used.
func New(dc *webrtc.DataChannel, obfuscator *Obfuscator) *Conn {
	if obfuscator == nil {
		obfuscator = &Obfuscator{enabled: false}
	}

	c := &Conn{
		dc:          dc,
		readCh:      make(chan []byte, 4096), // Large buffer for proxy traffic throughput
		obfuscator:  obfuscator,
		bufferReady: make(chan struct{}, 1),
	}

	// Setup backpressure callback
	dc.SetBufferedAmountLowThreshold(lowBufferThreshold)
	dc.OnBufferedAmountLow(func() {
		select {
		case c.bufferReady <- struct{}{}:
		default:
		}
	})

	return c
}

// HandleMessage should be called from the subscriber's dc.OnMessage callback.
// It unwraps the LiveKit DataPacket, decrypts the payload, and delivers it.
func (c *Conn) HandleMessage(msg webrtc.DataChannelMessage) {
	// Guard: if already closed, silently drop (prevents send-on-closed-channel panic)
	if c.closed.Load() {
		return
	}

	dp := &lkproto.DataPacket{}
	if err := proto.Unmarshal(msg.Data, dp); err != nil {
		return
	}
	user, ok := dp.Value.(*lkproto.DataPacket_User)
	if !ok || user.User == nil {
		return
	}
	payload := user.User.Payload
	if len(payload) == 0 {
		return
	}

	// Decrypt/de-obfuscate the payload
	plaintext, err := c.obfuscator.Decrypt(payload)
	if err != nil {
		// If decryption fails, try passing through raw (backwards compatibility)
		plaintext = payload
	}

	c.bytesRecv.Add(int64(len(plaintext)))

	// Copy to avoid referencing the WebRTC buffer after return.
	buf := make([]byte, len(plaintext))
	copy(buf, plaintext)

	// Use defer/recover as safety net for rare race with Close().
	defer func() { recover() }()

	// Block until space is available — do NOT drop data.
	// On a reliable SCTP channel, dropping causes yamux data corruption.
	c.readCh <- buf
}

// Read implements io.Reader. Blocks until data is available.
// Preserves message boundaries: if a DataChannel message is larger
// than the caller's buffer, leftover bytes are saved for the next Read.
func (c *Conn) Read(p []byte) (int, error) {
	// Drain leftover from previous partial read
	if len(c.buf) > 0 {
		n := copy(p, c.buf)
		c.buf = c.buf[n:]
		return n, nil
	}

	data, ok := <-c.readCh
	if !ok {
		return 0, io.EOF
	}
	n := copy(p, data)
	if n < len(data) {
		c.buf = data[n:]
	}
	return n, nil
}

// Write implements io.Writer. Wraps data in LiveKit DataPacket_RELIABLE.
// Applies obfuscation before sending and implements backpressure when
// the SCTP buffer is full.
//
// Small writes (< coalesceMaxSize) are batched within a 1ms window
// to reduce per-message overhead through SCTP/DTLS/TURN.
func (c *Conn) Write(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, fmt.Errorf("connection closed")
	}
	dc := c.dc
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return 0, fmt.Errorf("data channel not open")
	}

	// Backpressure: wait if the SCTP buffer is too full.
	// This prevents overwhelming the DataChannel and causing write failures.
	c.writeMu.Lock()
	for dc.BufferedAmount() > maxBufferedAmount {
		c.writeMu.Unlock()
		select {
		case <-c.bufferReady:
			// Buffer drained, try again
		case <-time.After(30 * time.Second):
			return 0, fmt.Errorf("write timeout: SCTP buffer full for 30s")
		}
		c.writeMu.Lock()
		if c.closed.Load() {
			c.writeMu.Unlock()
			return 0, fmt.Errorf("connection closed during write wait")
		}
	}
	c.writeMu.Unlock()

	originalLen := len(p)

	// Write coalescing: accumulate small writes and flush together.
	// The sendMu is held during flush to prevent the timer goroutine
	// from interleaving dc.Send() calls with our flush.
	c.coalesceMu.Lock()
	if len(p)+len(c.coalesceBuf) >= coalesceMaxSize {
		// Flush any pending + this write immediately
		pending := c.coalesceBuf
		c.coalesceBuf = nil
		if c.coalesceTimer != nil {
			c.coalesceTimer.Stop()
			c.coalesceTimer = nil
		}
		c.coalesceMu.Unlock()

		// Serialize sends: hold sendMu for the entire flush sequence
		// so the timer goroutine can't interleave.
		c.sendMu.Lock()
		var err error
		if len(pending) > 0 {
			err = c.flushPayloadLocked(pending)
		}
		if err == nil {
			err = c.flushPayloadLocked(p)
		}
		c.sendMu.Unlock()
		if err != nil {
			return 0, err
		}
	} else {
		// Accumulate into coalesce buffer
		c.coalesceBuf = append(c.coalesceBuf, p...)
		if c.coalesceTimer == nil {
			c.coalesceTimer = time.AfterFunc(coalesceWindow, c.flushCoalesced)
		}
		c.coalesceMu.Unlock()
	}

	c.bytesSent.Add(int64(originalLen))
	return originalLen, nil
}

// flushCoalesced is called by the coalesce timer goroutine.
// It grabs the coalesced buffer and sends it under sendMu.
func (c *Conn) flushCoalesced() {
	c.coalesceMu.Lock()
	buf := c.coalesceBuf
	c.coalesceBuf = nil
	c.coalesceTimer = nil
	c.coalesceMu.Unlock()

	if len(buf) > 0 {
		c.sendMu.Lock()
		c.flushPayloadLocked(buf)
		c.sendMu.Unlock()
	}
}

// flushPayloadLocked encrypts and sends a payload as LiveKit DataPacket(s).
// CALLER MUST HOLD sendMu. Large payloads are split into SFU-safe chunks.
func (c *Conn) flushPayloadLocked(payload []byte) error {
	if c.closed.Load() {
		return fmt.Errorf("connection closed")
	}

	for len(payload) > 0 {
		chunk := payload
		if len(chunk) > coalesceMaxSize {
			chunk = payload[:coalesceMaxSize]
		}
		payload = payload[len(chunk):]

		if err := c.sendChunkLocked(chunk); err != nil {
			return err
		}
	}
	return nil
}

// sendChunkLocked encrypts and sends a single chunk as a LiveKit DataPacket.
// CALLER MUST HOLD sendMu.
func (c *Conn) sendChunkLocked(chunk []byte) error {
	// Encrypt/obfuscate the chunk
	ciphertext, err := c.obfuscator.Encrypt(chunk)
	if err != nil {
		return fmt.Errorf("encrypt: %w", err)
	}

	dp := &lkproto.DataPacket{
		Kind: lkproto.DataPacket_RELIABLE,
		Value: &lkproto.DataPacket_User{
			User: &lkproto.UserPacket{
				Payload: ciphertext,
			},
		},
	}
	buf, err := proto.Marshal(dp)
	if err != nil {
		return err
	}

	dc := c.dc
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return fmt.Errorf("data channel not open")
	}
	return dc.Send(buf)
}

// Close implements io.Closer.
func (c *Conn) Close() error {
	c.once.Do(func() {
		c.closed.Store(true)

		// Flush any pending coalesced data
		c.coalesceMu.Lock()
		if c.coalesceTimer != nil {
			c.coalesceTimer.Stop()
			c.coalesceTimer = nil
		}
		pending := c.coalesceBuf
		c.coalesceBuf = nil
		c.coalesceMu.Unlock()
		if len(pending) > 0 {
			c.sendMu.Lock()
			c.flushPayloadLocked(pending)
			c.sendMu.Unlock()
		}

		close(c.readCh)
	})
	return nil
}

// Stats returns bytes sent/received.
func (c *Conn) Stats() (sent, recv int64) {
	return c.bytesSent.Load(), c.bytesRecv.Load()
}
