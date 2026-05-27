// Package rtpconn wraps a Pion WebRTC Opus audio track pair into an
// io.ReadWriteCloser suitable for use with yamux. VPN data bytes are
// written directly as Opus RTP sample payloads, and received directly
// from ReadRTP payloads. DPI sees a normal Opus voice call.
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
	"github.com/salman/ble-webrtc-tun/internal/logger"
)

var rtpLog = logger.New("rtpconn")

const (
	// maxChunkSize is the max payload per RTP Opus sample.
	// Keep under standard MTU to avoid fragmentation.
	maxChunkSize = 1200

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
	readCh     chan []byte // incoming RTP payloads
	buf        []byte      // partial read buffer
	closed     atomic.Bool
	once       sync.Once
	done       chan struct{}

	// Write serialization
	writeMu sync.Mutex

	bytesSent atomic.Int64
	bytesRecv atomic.Int64
}

// New creates a Conn. The localTrack is used for sending data;
// incoming data is fed via HandleRTP from the OnTrack callback.
// The second argument is reserved for future encryption; pass nil.
func New(localTrack *webrtc.TrackLocalStaticSample, _ interface{}) *Conn {
	return &Conn{
		localTrack: localTrack,
		readCh:     make(chan []byte, readChSize),
		done:       make(chan struct{}),
	}
}

// HandleRTP should be called from the OnTrack ReadRTP loop.
// It delivers the raw RTP payload bytes for Read().
func (c *Conn) HandleRTP(payload []byte) {
	if c.closed.Load() || len(payload) == 0 {
		return
	}

	// Copy to avoid referencing the WebRTC buffer after return.
	buf := make([]byte, len(payload))
	copy(buf, payload)

	c.bytesRecv.Add(int64(len(payload)))

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

// Write implements io.Writer. Writes raw bytes as Opus audio samples.
// Large writes are split into MTU-safe chunks. No framing header is
// added — yamux provides its own length-prefixed framing layer.
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

	for len(remaining) > 0 {
		chunk := remaining
		if len(chunk) > maxChunkSize {
			chunk = remaining[:maxChunkSize]
		}
		remaining = remaining[len(chunk):]

		// Write as Opus sample — DPI sees a normal audio frame
		if err := c.localTrack.WriteSample(media.Sample{
			Data:     chunk,
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
