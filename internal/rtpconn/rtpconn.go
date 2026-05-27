// Package rtpconn wraps a Pion WebRTC Opus audio track pair into an
// io.ReadWriteCloser suitable for use with yamux. Data is injected
// directly as raw Opus RTP payloads, making the traffic appear as a
// normal voice call to DPI systems and the Bale SFU.
//
// Features:
//   - DPI evasion: traffic looks like Opus voice call audio
//   - Backpressure flow control via internal buffering
//   - Chunk splitting: large writes split into ≤1000-byte RTP payloads
//   - 4-byte length header per chunk for reliable reassembly
package rtpconn

import (
	"encoding/binary"
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
	// maxPayloadSize is the max payload per RTP packet (Opus-safe size).
	// Real Opus frames are typically 20-160 bytes; we use larger payloads
	// but the SFU still forwards them as-is on the audio track.
	maxPayloadSize = 1000

	// sampleDuration is the fake Opus frame duration.
	// 20ms is the standard Opus frame size, keeps the SFU's audio
	// pipeline happy without triggering rate limiting.
	sampleDuration = 20 * time.Millisecond

	// readChSize is the channel buffer for incoming RTP payloads.
	readChSize = 8192

	// silenceInterval is how often to send a silence frame when idle.
	// Keeps the audio track alive so the SFU doesn't tear it down.
	silenceInterval = 20 * time.Millisecond
)

// Conn wraps a local audio track (for writing) and a remote audio track
// (for reading) into an io.ReadWriteCloser for yamux.
type Conn struct {
	localTrack *webrtc.TrackLocalStaticSample
	readCh     chan []byte // incoming reassembled payloads
	buf        []byte      // partial read buffer
	closed     atomic.Bool
	once       sync.Once
	done       chan struct{}

	// Obfuscation layer (optional)
	obfuscator Encryptor

	// Write serialization
	writeMu sync.Mutex

	bytesSent atomic.Int64
	bytesRecv atomic.Int64

	// Reassembly buffer for incoming frames
	reassemblyMu  sync.Mutex
	reassemblyBuf []byte
}

// Encryptor is an optional interface for payload obfuscation.
// If nil, data is sent raw (still looks like Opus to DPI).
type Encryptor interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// New creates a Conn. The localTrack is used for sending data;
// incoming data is fed via HandleRTP from the OnTrack callback.
func New(localTrack *webrtc.TrackLocalStaticSample, enc Encryptor) *Conn {
	c := &Conn{
		localTrack: localTrack,
		readCh:     make(chan []byte, readChSize),
		done:       make(chan struct{}),
		obfuscator: enc,
	}
	return c
}

// HandleRTP should be called from the OnTrack callback's ReadRTP loop.
// It extracts VPN data from the RTP payload and delivers it for Read().
//
// Frame format: [4-byte BE length][payload data]
// Multiple frames may be packed in a single RTP payload.
func (c *Conn) HandleRTP(payload []byte) {
	if c.closed.Load() || len(payload) == 0 {
		return
	}

	// Decrypt if obfuscator is set
	data := payload
	if c.obfuscator != nil {
		decrypted, err := c.obfuscator.Decrypt(payload)
		if err != nil {
			// Try raw (backwards compat or silence frames)
			data = payload
		} else {
			data = decrypted
		}
	}

	// Feed into reassembly buffer
	c.reassemblyMu.Lock()
	c.reassemblyBuf = append(c.reassemblyBuf, data...)

	// Extract complete frames: [4-byte BE length][payload]
	for len(c.reassemblyBuf) >= 4 {
		frameLen := int(binary.BigEndian.Uint32(c.reassemblyBuf[:4]))
		if frameLen == 0 {
			// Silence/keepalive frame — skip
			c.reassemblyBuf = c.reassemblyBuf[4:]
			continue
		}
		if frameLen > 64*1024 {
			// Invalid — likely corruption, reset buffer
			rtpLog.Warn("Invalid frame length %d, resetting buffer", frameLen)
			c.reassemblyBuf = nil
			break
		}
		if len(c.reassemblyBuf) < 4+frameLen {
			// Incomplete frame, wait for more data
			break
		}

		// Extract complete frame
		frame := make([]byte, frameLen)
		copy(frame, c.reassemblyBuf[4:4+frameLen])
		c.reassemblyBuf = c.reassemblyBuf[4+frameLen:]

		c.bytesRecv.Add(int64(frameLen))

		// Deliver (block if full to avoid data loss)
		select {
		case c.readCh <- frame:
		case <-c.done:
			c.reassemblyMu.Unlock()
			return
		}
	}
	c.reassemblyMu.Unlock()
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

// Write implements io.Writer. Chops data into RTP-sized chunks and
// writes them as Opus samples on the audio track.
//
// Each chunk: [4-byte BE length][payload data]
// The receiver reconstructs the original write from these chunks.
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

	// Build framed payload: [4-byte length][data]
	framed := make([]byte, 4+len(p))
	binary.BigEndian.PutUint32(framed[:4], uint32(len(p)))
	copy(framed[4:], p)

	// Split into RTP-sized chunks
	for len(framed) > 0 {
		chunk := framed
		if len(chunk) > maxPayloadSize {
			chunk = framed[:maxPayloadSize]
		}
		framed = framed[len(chunk):]

		// Encrypt if obfuscator is set
		payload := chunk
		if c.obfuscator != nil {
			encrypted, err := c.obfuscator.Encrypt(chunk)
			if err != nil {
				return 0, fmt.Errorf("encrypt: %w", err)
			}
			payload = encrypted
		}

		// Write as Opus sample
		if err := c.localTrack.WriteSample(media.Sample{
			Data:     payload,
			Duration: sampleDuration,
		}); err != nil {
			return 0, fmt.Errorf("write sample: %w", err)
		}
	}

	c.bytesSent.Add(int64(totalLen))
	return totalLen, nil
}

// StartSilenceLoop sends periodic silence frames to keep the audio
// track alive when no data is being sent. Call this in a goroutine.
func (c *Conn) StartSilenceLoop() {
	go func() {
		// Silence frame: 4-byte zero length = keepalive
		silence := make([]byte, 4) // all zeros

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
				// Only send silence if there's no recent data
				// (check by last write time would be more efficient,
				// but this simple approach works for keepalive)
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
