package kcptun

import (
	"bytes"
	"encoding/binary"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"sync"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

var tunnelLog = logger.New("tunnel")

// Tunnel wraps raw byte-level send/recv functions with KCP for
// reliable, ordered delivery over a lossy transport (VP8 video track).
//
// KCP implements ARQ (Automatic Repeat reQuest) — it handles
// retransmissions, duplicate detection, and ordering on top of
// any unreliable byte pipe.
type Tunnel struct {
	kcp    *kcp.KCP
	mu     sync.Mutex
	sendFn func([]byte) error // raw send (VP8 frame)
	recvFn func([]byte)       // deliver reassembled data to application
	done   chan struct{}
	convID uint32
}

// NewTunnel creates a new KCP reliability layer.
//   - sendFn: called when KCP wants to send a raw frame (goes to VP8 track)
//   - recvFn: called when KCP delivers a complete, ordered IP packet
//   - convID: conversation ID (must be same on both sides)
func NewTunnel(convID uint32, sendFn func([]byte) error, recvFn func([]byte)) *Tunnel {
	t := &Tunnel{
		sendFn: sendFn,
		recvFn: recvFn,
		done:   make(chan struct{}),
		convID: convID,
	}

	// Create KCP instance with our output callback
	t.kcp = kcp.NewKCP(convID, func(buf []byte, size int) {
		data := make([]byte, size)
		copy(data, buf[:size])
		if err := t.sendFn(data); err != nil {
			tunnelLog.Error("Send error: %v", err)
		}
	})

	// Tune KCP for low-latency tunneling over a lossy SFU:
	// - NoDelay=1: disable delayed ACK
	// - Interval=10ms: fast update interval
	// - Resend=2: fast resend on 2 duplicate ACKs
	// - NC=1: disable congestion control (we manage bandwidth externally)
	t.kcp.NoDelay(1, 10, 2, 1)

	// Set window sizes (in packets) — large window for high-bandwidth WAN paths
	t.kcp.WndSize(2048, 2048)

	// Maximum segment size: matches kcpconn.go for consistency
	t.kcp.SetMtu(1100)

	// Start update loop
	go t.updateLoop()

	return t
}

// Send queues data for reliable delivery through KCP.
func (t *Tunnel) Send(data []byte) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	// KCP.Send queues the data; it will be flushed on next Update()
	return t.kcp.Send(data)
}

// Input feeds raw data received from the remote peer into KCP.
// Call this when you receive a VP8 frame containing KCP data.
func (t *Tunnel) Input(data []byte) {
	t.mu.Lock()
	t.kcp.Input(data, kcp.IKCP_PACKET_REGULAR, true)
	// Force immediate flush — push ACKs/responses without waiting for next update tick
	t.kcp.Update()

	// After input, try to receive any complete messages
	for {
		size := t.kcp.PeekSize()
		if size <= 0 {
			break
		}
		buf := make([]byte, size)
		n := t.kcp.Recv(buf)
		if n > 0 {
			t.mu.Unlock()
			t.recvFn(buf[:n])
			t.mu.Lock()
		}
	}
	t.mu.Unlock()
}

// Close shuts down the KCP tunnel.
func (t *Tunnel) Close() {
	select {
	case <-t.done:
	default:
		close(t.done)
	}
}

// updateLoop calls KCP.Update() at regular intervals to flush pending data,
// process retransmissions, and handle timeouts.
func (t *Tunnel) updateLoop() {
	// KCP uses millisecond timestamps
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-t.done:
			return
		case <-ticker.C:
			t.mu.Lock()
			t.kcp.Update()
			t.mu.Unlock()
		}
	}
}

// =====================================================================
// VP8 Steganography Framing
// =====================================================================
//
// The SFU (LiveKit) actively parses VP8 frame headers to determine
// keyframe status, PictureID, temporal layers, etc. If we send raw
// bytes with arbitrary markers (0xFF, 0xFE), the SFU classifies them
// as corrupt VP8 and drops them silently.
//
// Solution: Disguise our KCP data as valid VP8 keyframes.
//
// Frame format:
//   [10-byte VP8 keyframe header] [4-byte BE payload length] [KCP data]
//
// The VP8 header is a minimal 2x2 keyframe that the SFU will parse
// and forward without complaint.

// validVP8Header is a minimal, valid VP8 keyframe header (10 bytes).
// - Byte 0: 0x50 = keyframe (bit0=0), show_frame=1, version=1, partition0 size LSBs
// - Bytes 1-2: 0x02 0x00 = partition0 size MSBs
// - Bytes 3-5: 0x9D 0x01 0x2A = VP8 keyframe start code (mandatory)
// - Bytes 6-7: 0x02 0x00 = width=2 pixels
// - Bytes 8-9: 0x02 0x00 = height=2 pixels
var validVP8Header = []byte{
	0x50, 0x02, 0x00, 0x9D, 0x01, 0x2A, 0x02, 0x00, 0x02, 0x00,
}

// WrapFrame wraps KCP output data in a valid VP8 keyframe for SFU transport.
func WrapFrame(data []byte) []byte {
	// [VP8 Header (10)] + [4-byte BE length] + [KCP/IP Data]
	frame := make([]byte, len(validVP8Header)+4+len(data))
	copy(frame, validVP8Header)
	binary.BigEndian.PutUint32(frame[len(validVP8Header):], uint32(len(data)))
	copy(frame[len(validVP8Header)+4:], data)
	return frame
}

// IsFrame checks if a received frame has our VP8 steganography header.
func IsFrame(frame []byte) bool {
	if len(frame) < len(validVP8Header)+4 {
		return false
	}
	return bytes.Equal(frame[:len(validVP8Header)], validVP8Header)
}

// ExtractPayload extracts the KCP segment data from a VP8 steganography frame.
func ExtractPayload(frame []byte) []byte {
	hdrLen := len(validVP8Header)
	if len(frame) < hdrLen+4 {
		return nil
	}
	payloadLen := binary.BigEndian.Uint32(frame[hdrLen : hdrLen+4])
	if int(payloadLen) > len(frame)-hdrLen-4 {
		return nil
	}
	return frame[hdrLen+4 : hdrLen+4+int(payloadLen)]
}
