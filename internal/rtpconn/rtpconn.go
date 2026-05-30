// Package rtpconn wraps a Pion WebRTC Opus audio track pair into an
// io.ReadWriteCloser suitable for use with yamux. VPN data bytes are
// encrypted with XChaCha20-Poly1305, then written as Opus RTP sample
// payloads. Incoming RTP payloads are decrypted before delivery.
// DPI sees a normal Opus voice call; the SFU sees opaque audio data.
//
// REGULATION ENGINE (20ms Pacer):
// The SFU audio policer expects exactly one Opus frame every 20ms (~50 pps).
// Writing faster causes burst drops that trigger KCP retransmissions that
// trigger more drops — a death spiral. The pacer goroutine enforces strict
// 20ms cadence: data frames are queued and drained one-per-tick.
// When the queue is empty, a 3-byte silence frame keeps the track alive.
package rtpconn

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/pion/webrtc/v3/pkg/media"
	"github.com/salman/ble-webrtc-tun/internal/dcconn"
	"github.com/salman/ble-webrtc-tun/internal/logger"
)

var rtpLog = logger.New("rtpconn")

const (
	// maxChunkSize is the max payload per RTP Opus sample.
	// Account for obfuscation overhead (40 bytes) to stay under MTU.
	maxPlainChunkSize = 1160 // 1200 - 40 (nonce + tag) = 1160 bytes of plaintext per packet

	// sampleDuration is the Opus frame duration — must match the pacer interval.
	sampleDuration = 20 * time.Millisecond

	// readChSize is the channel buffer for incoming RTP payloads.
	readChSize = 8192

	// pacerQueueSize is the max number of outbound frames queued.
	// 512 frames × 20ms = ~10.2 seconds of buffer — enough for bursts.
	pacerQueueSize = 512
)

// minimalOpusSilence is a 3-byte comfort-noise silence frame (RFC 6716).
// The SFU and remote peer treat this as a real Opus frame, keeping the
// track alive without consuming meaningful bandwidth.
var minimalOpusSilence = []byte{0xF8, 0xFF, 0xFE}

// Conn wraps a local audio track (for writing) and delivers incoming
// RTP payloads (for reading) as an io.ReadWriteCloser for yamux.
type Conn struct {
	localTrack *webrtc.TrackLocalStaticSample
	readCh     chan []byte // incoming RTP payloads (decrypted)
	buf        []byte     // partial read buffer
	closed     atomic.Bool
	once       sync.Once
	done       chan struct{}

	// Obfuscation layer — encrypts payloads so the SFU can't inspect them.
	// If nil, payloads pass through unencrypted (backwards compatible).
	obfuscator *dcconn.Obfuscator

	// Write serialization — all writes go through the pacer queue.
	// The pacer goroutine drains the queue at exactly 20ms intervals,
	// enforcing the SFU audio policer's expected packet rate.
	outboundQueue chan []byte // encrypted frames waiting to be paced

	bytesSent atomic.Int64
	bytesRecv atomic.Int64
}

// New creates a Conn. The localTrack is used for sending data;
// incoming data is fed via HandleRTP from the OnTrack callback.
// obfuscator encrypts payloads before they enter the RTP track.
// The 20ms pacer goroutine starts automatically.
func New(localTrack *webrtc.TrackLocalStaticSample, obfuscator *dcconn.Obfuscator) *Conn {
	c := &Conn{
		localTrack:    localTrack,
		readCh:        make(chan []byte, readChSize),
		done:          make(chan struct{}),
		obfuscator:    obfuscator,
		outboundQueue: make(chan []byte, pacerQueueSize),
	}
	if obfuscator != nil && obfuscator.Enabled() {
		rtpLog.Info("RTP obfuscation enabled (XChaCha20-Poly1305, overhead: %d bytes/pkt)", obfuscator.Overhead())
	}
	// Start the Regulation Engine pacer — enforces 20ms Opus cadence
	go c.pacerLoop()
	return c
}

// pacerLoop is the Regulation Engine core.
// It runs at exactly 20ms intervals, sending one RTP frame per tick.
// When the outboundQueue has data, it sends real VPN payload.
// When empty, it sends a 3-byte Opus silence frame to keep the track alive.
//
// This prevents SFU audio policer drops by never exceeding 50 pps.
func (c *Conn) pacerLoop() {
	ticker := time.NewTicker(sampleDuration)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if c.closed.Load() {
				return
			}
			select {
			case pkt := <-c.outboundQueue:
				// Real data frame — send it to the SFU
				_ = c.localTrack.WriteSample(media.Sample{
					Data:     pkt,
					Duration: sampleDuration,
				})
			default:
				// No data queued — send silence to keep the Opus track alive.
				// The SFU must receive continuous audio to maintain the call;
				// silence frames are 3 bytes (vs ~1100 for data) so they're
				// essentially free in bandwidth terms.
				_ = c.localTrack.WriteSample(media.Sample{
					Data:     minimalOpusSilence,
					Duration: sampleDuration,
				})
			}
		}
	}
}

// HandleRTP should be called from the OnTrack ReadRTP loop.
// It decrypts the raw RTP payload bytes and delivers them for Read().
func (c *Conn) HandleRTP(payload []byte) {
	if c.closed.Load() || len(payload) == 0 {
		return
	}

	// Skip Opus silence frames (real keepalive, not data)
	if isOpusSilence(payload) {
		return
	}

	// Decrypt if obfuscation is enabled
	plaintext := payload
	if c.obfuscator != nil && c.obfuscator.Enabled() {
		decrypted, err := c.obfuscator.Decrypt(payload)
		if err != nil {
			// Could be a genuine Opus frame from another participant or
			// a backwards-compatible unencrypted packet — pass through
			plaintext = payload
		} else {
			plaintext = decrypted
		}
	}

	// Copy to avoid referencing the WebRTC buffer after return.
	buf := make([]byte, len(plaintext))
	copy(buf, plaintext)

	c.bytesRecv.Add(int64(len(plaintext)))

	// Deliver — block if full to avoid data loss (yamux needs reliability)
	select {
	case c.readCh <- buf:
	case <-c.done:
	}
}

// Read implements io.Reader. Blocks until data is available.
func (c *Conn) Read(p []byte) (int, error) {
	// Drain leftover from previous partial read
	if len(c.buf) > 0 {
		n := copy(p, c.buf)
		c.buf = c.buf[n:]
		return n, nil
	}

	select {
	case data, ok := <-c.readCh:
		if !ok {
			return 0, io.EOF
		}
		n := copy(p, data)
		if n < len(data) {
			c.buf = data[n:]
		}
		return n, nil
	case <-c.done:
		return 0, io.EOF
	}
}

// Write implements io.Writer.
// Encrypts data (if obfuscation is enabled), splits into MTU-safe chunks,
// and enqueues each chunk for paced delivery at 20ms intervals.
//
// IMPORTANT: This method does NOT write to the RTP track directly.
// The pacerLoop() goroutine drains the outboundQueue at 20ms cadence.
// This ensures the SFU never receives more than ~50 packets/second,
// preventing audio policer drops.
//
// Backpressure: If the queue is full (512 frames = ~10s of data queued),
// Write() blocks until space is available. This naturally slows down
// KCP/yamux without causing goroutine leaks.
func (c *Conn) Write(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, fmt.Errorf("connection closed")
	}
	if c.localTrack == nil {
		return 0, fmt.Errorf("no local track")
	}

	totalLen := len(p)
	remaining := p

	// Choose chunk size based on whether obfuscation is active
	chunkSize := 1200 // default: raw mode, full MTU
	if c.obfuscator != nil && c.obfuscator.Enabled() {
		chunkSize = maxPlainChunkSize // leave room for nonce + auth tag
	}

	for len(remaining) > 0 {
		chunk := remaining
		if len(chunk) > chunkSize {
			chunk = remaining[:chunkSize]
		}
		remaining = remaining[len(chunk):]

		// Encrypt if obfuscation is enabled
		data := chunk
		if c.obfuscator != nil && c.obfuscator.Enabled() {
			encrypted, err := c.obfuscator.Encrypt(chunk)
			if err != nil {
				return 0, fmt.Errorf("encrypt: %w", err)
			}
			data = encrypted
		}

		// Enqueue for paced delivery — block if queue is full (backpressure)
		select {
		case c.outboundQueue <- data:
		case <-c.done:
			return 0, io.ErrClosedPipe
		}
	}

	c.bytesSent.Add(int64(totalLen))
	return totalLen, nil
}

// StartSilenceLoop is a no-op. The pacerLoop() goroutine handles silence
// frames automatically when the outboundQueue is empty. This stub is kept
// for API compatibility with sfutransport.go callers.
func (c *Conn) StartSilenceLoop() {
	// No-op: silence is now handled by pacerLoop()
	rtpLog.Info("StartSilenceLoop() called — silence is now managed by the 20ms pacer (no-op)")
}

// Close implements io.Closer.
func (c *Conn) Close() error {
	c.once.Do(func() {
		c.closed.Store(true)
		close(c.done)
		close(c.readCh)
	})
	return nil
}

// Stats returns bytes sent/received.
func (c *Conn) Stats() (sent, recv int64) {
	return c.bytesSent.Load(), c.bytesRecv.Load()
}

// QueueDepth returns the number of frames currently waiting in the pacer queue.
// Useful for monitoring backpressure.
func (c *Conn) QueueDepth() int {
	return len(c.outboundQueue)
}

// isOpusSilence detects standard Opus comfort noise / silence frames
// so we don't try to decrypt keepalive packets.
func isOpusSilence(payload []byte) bool {
	if len(payload) < 1 || len(payload) > 3 {
		return false
	}
	// Standard Opus silence: 0xF8 0xFF 0xFE (3 bytes)
	if len(payload) == 3 && payload[0] == 0xF8 && payload[1] == 0xFF && payload[2] == 0xFE {
		return true
	}
	return false
}
