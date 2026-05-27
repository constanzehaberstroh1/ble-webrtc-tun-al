package transport

import (
	"encoding/binary"
	"fmt"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v3"
	"github.com/pion/webrtc/v3/pkg/media"
)

var webrtcLog = logger.New("webrtc")

const (
	tunnelMarker      byte = 0xFF
	keepaliveMs            = 40  // 25 fps
	keyframeEveryMs        = 2000
	framesPerKeyframe      = keyframeEveryMs / keepaliveMs // ~50
)

// Minimal valid VP8 keyframe (2x2 black image, ~30 bytes)
var vp8KeyFrame = []byte{
	0x50, 0x02, 0x00, // frame_tag: keyframe, version=0, show=1, part_size=18
	0x9D, 0x01, 0x2A, // VP8 start code
	0x02, 0x00, // width=2
	0x02, 0x00, // height=2
	0x34, 0x25, 0xA4, 0x00, 0x03, 0x70, 0x00,
	0xFE, 0xFB, 0x94, 0x00, 0x00,
}

// Minimal valid VP8 interframe (P-frame, ~4 bytes)
var vp8InterFrame = []byte{
	0x11, 0x00, 0x00, // frame_tag: interframe, version=0, show=1, part_size=0
	0x00, // empty partition
}

// EnableVideoTunnel sets up video track based tunneling.
// Call this BEFORE CreateOffer/HandleOffer.
func (t *WebRTCTransport) EnableVideoTunnel() error {
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
		"video", "ble-tunnel-video",
	)
	if err != nil {
		return fmt.Errorf("creating video track: %w", err)
	}
	t.mu.Lock()
	t.videoTrack = track
	t.videoMode = true
	t.mu.Unlock()

	if _, err := t.pc.AddTrack(track); err != nil {
		return fmt.Errorf("adding video track: %w", err)
	}

	// Handle incoming video track from remote peer
	t.pc.OnTrack(func(remote *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		webrtcLog.Info("📹 Received remote track: codec=%s", remote.Codec().MimeType)
		go t.readVideoTrack(remote)
	})

	webrtcLog.Info("Video tunnel enabled (VP8 steganography)")
	return nil
}

// SendVideo sends an IP packet disguised as a VP8 video frame.
// Format: [0xFF marker][4-byte BE length][IP packet data]
func (t *WebRTCTransport) SendVideo(data []byte) error {
	t.mu.RLock()
	track := t.videoTrack
	connected := t.connected
	t.mu.RUnlock()

	if !connected || track == nil {
		return fmt.Errorf("not connected")
	}

	// Build tunnel frame: marker + length + payload
	frame := make([]byte, 5+len(data))
	frame[0] = tunnelMarker
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(data)))
	copy(frame[5:], data)

	err := track.WriteSample(media.Sample{
		Data:     frame,
		Duration: time.Duration(keepaliveMs) * time.Millisecond,
	})
	if err != nil {
		return err
	}

	t.stats.BytesSent.Add(int64(len(data)))
	t.stats.PacketsSent.Add(1)
	return nil
}

// readVideoTrack reads VP8 frames from remote track and extracts tunnel data.
func (t *WebRTCTransport) readVideoTrack(track *webrtc.TrackRemote) {
	depacketizer := &codecs.VP8Packet{}
	var frameBuf []byte

	for {
		pkt, _, err := track.ReadRTP()
		if err != nil {
			webrtcLog.Error("Track read error: %v", err)
			return
		}

		// Depacketize VP8 to get frame payload
		payload, err := depacketizer.Unmarshal(pkt.Payload)
		if err != nil || len(payload) == 0 {
			continue
		}

		// Check if this is start of a new frame (S bit in VP8 descriptor)
		isStart := len(pkt.Payload) > 0 && (pkt.Payload[0]&0x10) != 0
		isEnd := pkt.Header.Marker

		if isStart {
			frameBuf = append([]byte{}, payload...)
		} else {
			frameBuf = append(frameBuf, payload...)
		}

		if !isEnd {
			continue // Wait for complete frame
		}

		// Complete frame received
		frame := frameBuf
		frameBuf = nil

		if len(frame) < 5 || frame[0] != tunnelMarker {
			// Not a tunnel frame — keepalive, discard
			continue
		}

		// Extract IP packet
		payloadLen := binary.BigEndian.Uint32(frame[1:5])
		if int(payloadLen) > len(frame)-5 {
			continue // Invalid length
		}

		ipPacket := frame[5 : 5+payloadLen]

		t.stats.BytesReceived.Add(int64(payloadLen))
		t.stats.PacketsRecv.Add(1)

		t.mu.RLock()
		fn := t.onData
		t.mu.RUnlock()
		if fn != nil {
			fn(ipPacket)
		}
	}
}

// StartKeepalive sends periodic VP8 keyframes and interframes to keep
// the SFU happy and prevent call disconnection.
func (t *WebRTCTransport) StartKeepalive() {
	go func() {
		ticker := time.NewTicker(time.Duration(keepaliveMs) * time.Millisecond)
		defer ticker.Stop()

		frameCount := 0
		for {
			select {
			case <-t.done:
				return
			case <-ticker.C:
				t.mu.RLock()
				track := t.videoTrack
				connected := t.connected
				t.mu.RUnlock()

				if track == nil || !connected {
					continue
				}

				frameCount++
				var frame []byte
				if frameCount%framesPerKeyframe == 0 {
					frame = vp8KeyFrame
				} else {
					frame = vp8InterFrame
				}

				track.WriteSample(media.Sample{
					Data:     frame,
					Duration: time.Duration(keepaliveMs) * time.Millisecond,
				})
			}
		}
	}()
}

// reassembleRTP is a helper to reconstruct VP8 frames from RTP packets.
// Returns the complete frame when marker bit is set.
func reassembleRTP(pkt *rtp.Packet, buf []byte, depacketizer *codecs.VP8Packet) ([]byte, bool) {
	payload, err := depacketizer.Unmarshal(pkt.Payload)
	if err != nil || len(payload) == 0 {
		return buf, false
	}

	isStart := len(pkt.Payload) > 0 && (pkt.Payload[0]&0x10) != 0
	if isStart {
		buf = append([]byte{}, payload...)
	} else {
		buf = append(buf, payload...)
	}

	return buf, pkt.Header.Marker
}
