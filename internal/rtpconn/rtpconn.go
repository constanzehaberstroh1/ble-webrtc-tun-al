// Package rtpconn bridges the Pion WebRTC Opus track to quic-go via WriteFrame/ReadPacket.
//
// SPEED ARCHITECTURE (post-pacer removal):
//
//	WriteFrame() → WriteSample() immediately (no sleep, no queue)
//	               Pion internally increments RTP timestamp by 960 per call,
//	               so the SFU sees perfectly spaced logical timestamps even
//	               when packets arrive physically back-to-back.
//
//	silenceLoop() → fires every 20ms, but ONLY injects a 3-byte comfort noise
//	               frame when no real data was written in the last 20ms.
//	               This keeps the Opus track alive without blocking data writes.
//
// Result: QUIC can blast packets at full link speed. The SFU sees valid RTP
// timestamps (it checks timestamps, not wall-clock arrival rate). The track
// stays alive during idle periods via silence frames.
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
	// maxPlainChunkSize is only used by the legacy Write() method.
	// WriteFrame() receives pre-sized QUIC datagrams (≤1100 bytes).
	maxPlainChunkSize = 1160

	// sampleDuration is passed to Pion's WriteSample so it can compute
	// the correct RTP timestamp increment (960 samples at 48kHz per 20ms).
	sampleDuration = 20 * time.Millisecond

	// readChSize is the channel buffer for incoming RTP payloads.
	readChSize = 8192
)

// minimalOpusSilence is the 3-byte Opus DTX comfort noise frame.
// Sent when the track is idle to prevent the SFU from tearing it down.
var minimalOpusSilence = []byte{0xF8, 0xFF, 0xFE}

// Conn wraps one or more Pion local audio tracks and an RTP receive channel.
// It exposes WriteFrame/ReadPacket for quic-go's OpusPacketConn bridge,
// and the legacy Read/Write interface for backward compatibility.
//
// Phase 3 — Spatial Multi-Tracking: when multiple tracks are provided, data
// frames are striped round-robin across the track pool.  Each individual
// track maintains a low, voice-like cadence while the aggregate throughput
// scales with track count.  Per-track silence keepalive prevents the SFU
// from tearing down idle tracks.
type Conn struct {
	localTracks    []*webrtc.TrackLocalStaticSample
	trackIdx       atomic.Uint32  // round-robin index for WriteFrame
	trackLastWrite []atomic.Int64 // per-track last-write time (UnixNano)

	readCh chan []byte
	buf    []byte
	closed atomic.Bool
	once   sync.Once
	done   chan struct{}

	// Obfuscation (XChaCha20-Poly1305)
	obfuscator *dcconn.Obfuscator

	// writeMu serialises legacy Write() calls only.
	// WriteFrame() is already called from a single quic-go goroutine per stream.
	writeMu sync.Mutex

	bytesSent atomic.Int64
	bytesRecv atomic.Int64
}

// New creates a Conn with a single audio track (backward compatible).
func New(localTrack *webrtc.TrackLocalStaticSample, obfuscator *dcconn.Obfuscator) *Conn {
	return NewMulti([]*webrtc.TrackLocalStaticSample{localTrack}, obfuscator)
}

// NewMulti creates a Conn with multiple audio tracks for spatial
// multi-tracking camouflage.  Data frames are round-robin striped across
// all tracks; each track gets its own silence keepalive.
func NewMulti(tracks []*webrtc.TrackLocalStaticSample, obfuscator *dcconn.Obfuscator) *Conn {
	c := &Conn{
		localTracks:    tracks,
		trackLastWrite: make([]atomic.Int64, len(tracks)),
		readCh:         make(chan []byte, readChSize),
		done:           make(chan struct{}),
		obfuscator:     obfuscator,
	}
	now := time.Now().UnixNano()
	for i := range c.trackLastWrite {
		c.trackLastWrite[i].Store(now)
	}
	if obfuscator != nil && obfuscator.Enabled() {
		rtpLog.Info("RTP obfuscation enabled (XChaCha20-Poly1305, overhead: %d bytes/pkt)", obfuscator.Overhead())
	}
	rtpLog.Info("rtpconn initialized: %d track(s), Opus TOC camouflage active", len(tracks))
	go c.silenceLoop()
	return c
}

// NumTracks returns the number of audio tracks in the pool.
func (c *Conn) NumTracks() int {
	return len(c.localTracks)
}

// nextTrack returns the next track in round-robin order and records the
// write time for that track.
func (c *Conn) nextTrack() *webrtc.TrackLocalStaticSample {
	if len(c.localTracks) == 0 {
		return nil
	}
	idx := int(c.trackIdx.Add(1)-1) % len(c.localTracks)
	c.trackLastWrite[idx].Store(time.Now().UnixNano())
	return c.localTracks[idx]
}

// silenceLoop fires every 20ms and injects a 3-byte Opus comfort noise frame
// to EACH track that hasn't received real data in the last 20ms.
//
// This is non-blocking for data writes — real frames are never queued or delayed.
// Per-track tracking ensures all tracks in the pool stay alive independently.
func (c *Conn) silenceLoop() {
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
			now := time.Now().UnixNano()
			for i, track := range c.localTracks {
				last := c.trackLastWrite[i].Load()
				if now-last >= int64(sampleDuration) {
					_ = track.WriteSample(media.Sample{
						Data:     minimalOpusSilence,
						Duration: sampleDuration,
					})
				}
			}
		}
	}
}

// HandleRTP is called from the OnTrack ReadRTP loop.
// Strips the Opus TOC container, decrypts the payload, and delivers it
// to ReadPacket/Read.
func (c *Conn) HandleRTP(payload []byte) {
	if c.closed.Load() || len(payload) == 0 {
		return
	}
	// Skip the 3-byte Opus DTX silence keepalive frame.
	if isOpusSilence(payload) {
		return
	}

	// Phase 1: Strip the Opus TOC container + VBR padding.
	data := UnwrapFrame(payload)

	plaintext := data
	if c.obfuscator != nil && c.obfuscator.Enabled() {
		decrypted, err := c.obfuscator.Decrypt(data)
		if err != nil {
			plaintext = data
		} else {
			plaintext = decrypted
		}
	}

	buf := make([]byte, len(plaintext))
	copy(buf, plaintext)
	c.bytesRecv.Add(int64(len(plaintext)))

	select {
	case c.readCh <- buf:
	case <-c.done:
	}
}

// Read implements io.Reader (legacy yamux path — not used in QUIC mode).
func (c *Conn) Read(p []byte) (int, error) {
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

// Write implements io.Writer (legacy path — not used in QUIC mode).
// Splits large writes into MTU-safe chunks, wraps each in the Opus TOC
// container, and round-robins across the track pool.
func (c *Conn) Write(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, fmt.Errorf("connection closed")
	}
	if len(c.localTracks) == 0 {
		return 0, fmt.Errorf("no local tracks")
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	totalLen := len(p)
	remaining := p
	chunkSize := 1200
	if c.obfuscator != nil && c.obfuscator.Enabled() {
		chunkSize = maxPlainChunkSize
	}

	for len(remaining) > 0 {
		chunk := remaining
		if len(chunk) > chunkSize {
			chunk = remaining[:chunkSize]
		}
		remaining = remaining[len(chunk):]

		data := chunk
		if c.obfuscator != nil && c.obfuscator.Enabled() {
			encrypted, err := c.obfuscator.Encrypt(chunk)
			if err != nil {
				return 0, fmt.Errorf("encrypt: %w", err)
			}
			data = encrypted
		}

		// Phase 1+2: Wrap in Opus TOC container with VBR padding
		framed := WrapFrame(data)

		track := c.nextTrack()
		if track == nil {
			return 0, fmt.Errorf("no track available")
		}
		if err := track.WriteSample(media.Sample{
			Data:     framed,
			Duration: sampleDuration,
		}); err != nil {
			return 0, fmt.Errorf("write sample: %w", err)
		}
	}
	c.bytesSent.Add(int64(totalLen))
	return totalLen, nil
}

// WriteFrame writes exactly ONE RTP frame INSTANTLY — no queuing, no sleeping.
//
// Phase 1+2: The payload is encrypted (if obfuscation is enabled), then wrapped
// in a valid Opus TOC container with VBR padding before being written to the
// audio track.  This makes every data frame parse as a valid Opus audio sample
// to DPI inspection, with variable-length padding disrupting size analysis.
//
// Phase 3: Frames are round-robin striped across the track pool so each
// individual track maintains a low, voice-like cadence.
//
// Speed design: QUIC calls WriteTo (which calls WriteFrame) as fast as its
// congestion window allows. Pion's WriteSample increments the RTP timestamp
// by 960 on every call (48kHz × 20ms), so the SFU sees a perfectly spaced
// logical timestamp sequence even when physical arrival is back-to-back.
func (c *Conn) WriteFrame(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, fmt.Errorf("connection closed")
	}
	if len(c.localTracks) == 0 {
		return 0, fmt.Errorf("no local tracks")
	}

	data := p
	if c.obfuscator != nil && c.obfuscator.Enabled() {
		encrypted, err := c.obfuscator.Encrypt(p)
		if err != nil {
			return 0, fmt.Errorf("encrypt: %w", err)
		}
		data = encrypted
	}

	// Phase 1+2: Wrap in Opus TOC container with VBR padding
	framed := WrapFrame(data)

	// Phase 3: Round-robin across the track pool
	track := c.nextTrack()
	if track == nil {
		return 0, fmt.Errorf("no track available")
	}

	// WRITE INSTANTLY — Pion fakes the RTP timestamp from Duration
	if err := track.WriteSample(media.Sample{
		Data:     framed,
		Duration: sampleDuration, // Pion adds 960 RTP samples per call
	}); err != nil {
		return 0, err
	}

	c.bytesSent.Add(int64(len(p)))
	return len(p), nil
}

// ReadPacket reads one complete decrypted RTP payload (one QUIC datagram).
// Used by quicconn.OpusPacketConn.ReadFrom().
func (c *Conn) ReadPacket() ([]byte, error) {
	select {
	case data, ok := <-c.readCh:
		if !ok {
			return nil, fmt.Errorf("connection closed")
		}
		return data, nil
	case <-c.done:
		return nil, fmt.Errorf("connection closed")
	}
}

// StartSilenceLoop is a no-op — silence is managed by the silenceLoop goroutine
// started in New(). Kept for API compatibility.
func (c *Conn) StartSilenceLoop() {}

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

// QueueDepth is always 0 in the non-queued design. Kept for API compatibility.
func (c *Conn) QueueDepth() int { return 0 }

// isOpusSilence detects the 3-byte keepalive frame so we don't decrypt it.
func isOpusSilence(payload []byte) bool {
	return len(payload) == 3 &&
		payload[0] == 0xF8 && payload[1] == 0xFF && payload[2] == 0xFE
}
