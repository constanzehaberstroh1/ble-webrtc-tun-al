// Package rtpconn wraps a Pion WebRTC Opus audio track pair into an
// io.ReadWriteCloser suitable for use with yamux. VPN data bytes are
// encrypted with XChaCha20-Poly1305, then written as Opus RTP sample
// payloads. Incoming RTP payloads are decrypted before delivery.
// DPI sees a normal Opus voice call; the SFU sees opaque audio data.
//
// No framing is added — yamux provides its own reliable framing layer
// on top of this raw byte pipe.
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

	// sampleDuration is the fake Opus frame duration (20ms is standard).
	sampleDuration = 20 * time.Millisecond

	// readChSize is the channel buffer for incoming RTP payloads.
	readChSize = 8192

	// silenceInterval is how often to send a silence frame when idle.
	silenceInterval = 20 * time.Millisecond
)

// Conn wraps a local audio track (for writing) and delivers incoming
// RTP payloads (for reading) as an io.ReadWriteCloser for yamux.
type Conn struct {
	localTrack *webrtc.TrackLocalStaticSample
	readCh     chan []byte // incoming RTP payloads (decrypted)
	buf        []byte      // partial read buffer
	closed     atomic.Bool
	once       sync.Once
	done       chan struct{}

	// Obfuscation layer — encrypts payloads so the SFU can't inspect them.
	// If nil, payloads pass through unencrypted (backwards compatible).
	obfuscator *dcconn.Obfuscator

	// Write serialization
	writeMu sync.Mutex

	bytesSent atomic.Int64
	bytesRecv atomic.Int64
}

// New creates a Conn. The localTrack is used for sending data;
// incoming data is fed via HandleRTP from the OnTrack callback.
// obfuscator encrypts payloads before they enter the RTP track;
// pass nil for no encryption (backwards compatible but DPI-visible).
func New(localTrack *webrtc.TrackLocalStaticSample, obfuscator *dcconn.Obfuscator) *Conn {
	c := &Conn{
		localTrack: localTrack,
		readCh:     make(chan []byte, readChSize),
		done:       make(chan struct{}),
		obfuscator: obfuscator,
	}
	if obfuscator != nil && obfuscator.Enabled() {
		rtpLog.Info("RTP obfuscation enabled (XChaCha20-Poly1305, overhead: %d bytes/pkt)", obfuscator.Overhead())
	}
	return c
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

// Write implements io.Writer. Encrypts data (if obfuscation is enabled)
// and writes as Opus audio samples. Large writes are split into MTU-safe
// chunks. No framing header is added — yamux provides its own
// length-prefixed framing layer.
func (c *Conn) Write(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, fmt.Errorf("connection closed")
	}
	if c.localTrack == nil {
		return 0, fmt.Errorf("no local track")
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

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

		// Write as Opus sample — DPI sees a normal audio frame
		if err := c.localTrack.WriteSample(media.Sample{
			Data:     data,
			Duration: sampleDuration,
		}); err != nil {
			return 0, fmt.Errorf("write sample: %w", err)
		}
	}

	c.bytesSent.Add(int64(totalLen))
	return totalLen, nil
}

// StartSilenceLoop sends periodic silence frames to keep the audio
// track alive when no data is being sent.
func (c *Conn) StartSilenceLoop() {
	go func() {
		// Minimal Opus silence frame (single byte)
		silence := []byte{0xF8, 0xFF, 0xFE}

		ticker := time.NewTicker(silenceInterval)
		defer ticker.Stop()

		for {
			select {
			case <-c.done:
				return
			case <-ticker.C:
				if c.closed.Load() {
					return
				}
				c.localTrack.WriteSample(media.Sample{
					Data:     silence,
					Duration: sampleDuration,
				})
			}
		}
	}()
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
