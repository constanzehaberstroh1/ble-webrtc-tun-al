package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/salman/ble-webrtc-tun/internal/config"
	"github.com/salman/ble-webrtc-tun/internal/dcconn"
	"github.com/salman/ble-webrtc-tun/internal/livekit"
	"github.com/salman/ble-webrtc-tun/internal/rtpconn"
)

// webrtcLog is declared in videotunnel.go

// SDPExchange is used for SDP serialization over the wire.
// Pion v3's SDPType is an int enum that serializes to 0/1 instead
// of "offer"/"answer" strings, breaking interop. This struct fixes that.
type SDPExchange struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

func sdpTypeToString(t webrtc.SDPType) string {
	switch t {
	case webrtc.SDPTypeOffer:
		return "offer"
	case webrtc.SDPTypePranswer:
		return "pranswer"
	case webrtc.SDPTypeAnswer:
		return "answer"
	case webrtc.SDPTypeRollback:
		return "rollback"
	default:
		return "offer"
	}
}

func stringToSDPType(s string) webrtc.SDPType {
	switch s {
	case "offer":
		return webrtc.SDPTypeOffer
	case "pranswer":
		return webrtc.SDPTypePranswer
	case "answer":
		return webrtc.SDPTypeAnswer
	case "rollback":
		return webrtc.SDPTypeRollback
	default:
		return webrtc.SDPTypeOffer
	}
}

// Stats holds connection statistics.
type Stats struct {
	BytesSent     atomic.Int64
	BytesReceived atomic.Int64
	PacketsSent   atomic.Int64
	PacketsRecv   atomic.Int64
	StartTime     time.Time
}

// WebRTCTransport manages the Pion WebRTC connection.
// All tunnel data flows through a fake Opus audio track via rtpconn,
// making traffic appear as a normal voice call to DPI systems.
type WebRTCTransport struct {
	cfg        *config.Config
	pc         *webrtc.PeerConnection
	audioTrack *webrtc.TrackLocalStaticSample // The fake Opus track
	rtpConn    *rtpconn.Conn                  // Wraps the track for Read/Write
	onData     func([]byte)                  // Callback for received data
	mu         sync.RWMutex
	connected  bool
	stats      Stats
	done       chan struct{}

	// Obfuscation layer (anti-DPI)
	obfuscator *dcconn.Obfuscator
}

// NewWebRTCTransport creates a new WebRTC transport.
func NewWebRTCTransport(cfg *config.Config) *WebRTCTransport {
	return &WebRTCTransport{
		cfg:   cfg,
		done:  make(chan struct{}),
		stats: Stats{StartTime: time.Now()},
	}
}

// SetObfuscator sets the obfuscator for payload encryption.
// Must be called before Initialize/setupAudioTrack.
func (t *WebRTCTransport) SetObfuscator(obf *dcconn.Obfuscator) {
	t.mu.Lock()
	t.obfuscator = obf
	t.mu.Unlock()
}

// SetOnData sets the callback for received data.
func (t *WebRTCTransport) SetOnData(fn func([]byte)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onData = fn
}

// Initialize creates the PeerConnection with ICE servers.
// CRITICAL: Registers ONLY Opus codec with RTCPFeedback:nil to disable
// TWCC bandwidth throttling by the SFU.
func (t *WebRTCTransport) Initialize(iceServersFromLK []livekit.ICEServerInfo) error {
	iceServers := t.buildICEServers(iceServersFromLK)

	pcConfig := webrtc.Configuration{
		ICEServers: iceServers,
	}

	settingEngine := webrtc.SettingEngine{}
	settingEngine.SetICETimeouts(5*time.Second, 25*time.Second, 2*time.Second)

	// Register ONLY Opus. RTCPFeedback: nil disables TWCC/Bandwidth Throttling.
	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeOpus,
			ClockRate:    48000,
			Channels:     2,
			SDPFmtpLine:  "minptime=10;useinbandfec=1",
			RTCPFeedback: nil,
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return fmt.Errorf("registering Opus codec: %w", err)
	}

	api := webrtc.NewAPI(
		webrtc.WithSettingEngine(settingEngine),
		webrtc.WithMediaEngine(mediaEngine),
	)

	pc, err := api.NewPeerConnection(pcConfig)
	if err != nil {
		return fmt.Errorf("creating peer connection: %w", err)
	}

	t.pc = pc
	t.setupHandlers()

	webrtcLog.Info("PeerConnection initialized with %d ICE servers (Opus Only, no TWCC)", len(iceServers))
	return nil
}

// CreateOffer creates an SDP offer (client side).
func (t *WebRTCTransport) CreateOffer() (string, error) {
	if err := t.setupAudioTrack(); err != nil {
		return "", err
	}

	offer, err := t.pc.CreateOffer(nil)
	if err != nil {
		return "", fmt.Errorf("creating offer: %w", err)
	}

	if err := t.pc.SetLocalDescription(offer); err != nil {
		return "", fmt.Errorf("setting local description: %w", err)
	}

	// Wait for ICE gathering to complete
	gatherComplete := webrtc.GatheringCompletePromise(t.pc)
	select {
	case <-gatherComplete:
	case <-time.After(10 * time.Second):
		webrtcLog.Info("ICE gathering timed out, using partial candidates")
	}

	localDesc := t.pc.LocalDescription()
	exchange := SDPExchange{
		Type: sdpTypeToString(localDesc.Type),
		SDP:  localDesc.SDP,
	}
	sdpBytes, err := json.Marshal(exchange)
	if err != nil {
		return "", fmt.Errorf("marshaling SDP: %w", err)
	}

	webrtcLog.Info("Offer created (SDP type: %s, length: %d)",
		exchange.Type, len(localDesc.SDP))

	return string(sdpBytes), nil
}

// HandleOffer processes an SDP offer and returns an answer (server side).
func (t *WebRTCTransport) HandleOffer(offerJSON string) (string, error) {
	if err := t.setupAudioTrack(); err != nil {
		return "", err
	}

	var exchange SDPExchange
	if err := json.Unmarshal([]byte(offerJSON), &exchange); err != nil {
		return "", fmt.Errorf("unmarshaling offer: %w", err)
	}

	webrtcLog.Info("Received offer type=%q sdp_len=%d", exchange.Type, len(exchange.SDP))

	offer := webrtc.SessionDescription{
		Type: stringToSDPType(exchange.Type),
		SDP:  exchange.SDP,
	}

	if err := t.pc.SetRemoteDescription(offer); err != nil {
		return "", fmt.Errorf("setting remote description: %w", err)
	}

	answer, err := t.pc.CreateAnswer(nil)
	if err != nil {
		return "", fmt.Errorf("creating answer: %w", err)
	}

	if err := t.pc.SetLocalDescription(answer); err != nil {
		return "", fmt.Errorf("setting local description: %w", err)
	}

	// Wait for ICE gathering
	gatherComplete := webrtc.GatheringCompletePromise(t.pc)
	select {
	case <-gatherComplete:
	case <-time.After(10 * time.Second):
		webrtcLog.Info("ICE gathering timed out")
	}

	localDesc := t.pc.LocalDescription()
	answerExchange := SDPExchange{
		Type: sdpTypeToString(localDesc.Type),
		SDP:  localDesc.SDP,
	}
	sdpBytes, err := json.Marshal(answerExchange)
	if err != nil {
		return "", fmt.Errorf("marshaling SDP: %w", err)
	}

	webrtcLog.Info("Answer created (type=%s)", answerExchange.Type)
	return string(sdpBytes), nil
}

// HandleAnswer processes an SDP answer (client side).
func (t *WebRTCTransport) HandleAnswer(answerJSON string) error {
	var exchange SDPExchange
	if err := json.Unmarshal([]byte(answerJSON), &exchange); err != nil {
		return fmt.Errorf("unmarshaling answer: %w", err)
	}

	answer := webrtc.SessionDescription{
		Type: stringToSDPType(exchange.Type),
		SDP:  exchange.SDP,
	}

	if err := t.pc.SetRemoteDescription(answer); err != nil {
		return fmt.Errorf("setting remote description: %w", err)
	}

	webrtcLog.Info("Remote description set (type=%s)", exchange.Type)
	return nil
}

// setupAudioTrack initializes the fake Opus track and connects it to rtpconn.
func (t *WebRTCTransport) setupAudioTrack() error {
	audioTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "bale-audio",
	)
	if err != nil {
		return fmt.Errorf("creating audio track: %w", err)
	}

	if _, err = t.pc.AddTrack(audioTrack); err != nil {
		return fmt.Errorf("adding audio track: %w", err)
	}

	t.mu.Lock()
	t.audioTrack = audioTrack
	t.rtpConn = rtpconn.New(audioTrack, t.obfuscator)
	t.mu.Unlock()

	// Start silence loop to keep the voice call "alive"
	t.rtpConn.StartSilenceLoop()

	// Start reading from rtpConn and pushing via onData callback
	go t.readLoop()

	return nil
}

// readLoop reads from rtpConn and pushes data via the onData callback.
func (t *WebRTCTransport) readLoop() {
	buf := make([]byte, 2048)
	for {
		t.mu.RLock()
		conn := t.rtpConn
		fn := t.onData
		t.mu.RUnlock()

		if conn == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				return
			}
			continue
		}

		if n > 0 && fn != nil {
			t.stats.BytesReceived.Add(int64(n))
			t.stats.PacketsRecv.Add(1)
			fn(buf[:n])
		}
	}
}

// Send writes data via the Opus audio track (disguised as voice call).
func (t *WebRTCTransport) Send(data []byte) error {
	t.mu.RLock()
	connected := t.connected
	conn := t.rtpConn
	t.mu.RUnlock()

	if !connected || conn == nil {
		return fmt.Errorf("not connected")
	}

	n, err := conn.Write(data)
	if err != nil {
		return fmt.Errorf("sending data via rtp: %w", err)
	}

	t.stats.BytesSent.Add(int64(n))
	t.stats.PacketsSent.Add(1)
	return nil
}

// EnableVideoTunnel is now a no-op — all traffic goes through Opus audio.
// Kept for API compatibility with existing server code.
func (t *WebRTCTransport) EnableVideoTunnel() error {
	webrtcLog.Info("EnableVideoTunnel called — using Opus audio tunnel (no-op)")
	return nil
}

// StartKeepalive is now a no-op — rtpconn.StartSilenceLoop handles this.
// Kept for API compatibility with existing server code.
func (t *WebRTCTransport) StartKeepalive() {
	webrtcLog.Info("StartKeepalive called — handled by rtpconn silence loop (no-op)")
}

// IsVideoMode always returns false — we use audio mode now.
// Kept for API compatibility.
func (t *WebRTCTransport) IsVideoMode() bool {
	return false
}

// IsConnected returns whether the transport is connected.
func (t *WebRTCTransport) IsConnected() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.connected
}

// GetStats returns current connection statistics.
func (t *WebRTCTransport) GetStats() map[string]interface{} {
	uptime := time.Since(t.stats.StartTime)
	return map[string]interface{}{
		"connected":      t.IsConnected(),
		"bytes_sent":     t.stats.BytesSent.Load(),
		"bytes_received": t.stats.BytesReceived.Load(),
		"packets_sent":   t.stats.PacketsSent.Load(),
		"packets_recv":   t.stats.PacketsRecv.Load(),
		"uptime_seconds": int(uptime.Seconds()),
	}
}

// WaitForConnection blocks until the transport is connected or context is cancelled.
func (t *WebRTCTransport) WaitForConnection(ctx context.Context) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.done:
			return fmt.Errorf("transport closed")
		case <-ticker.C:
			if t.IsConnected() {
				return nil
			}
		}
	}
}

// Close shuts down the transport.
func (t *WebRTCTransport) Close() error {
	select {
	case <-t.done:
		// already closed
		return nil
	default:
		close(t.done)
	}
	t.mu.Lock()
	if t.rtpConn != nil {
		t.rtpConn.Close()
	}
	t.mu.Unlock()
	if t.pc != nil {
		return t.pc.Close()
	}
	return nil
}

// setupHandlers configures PeerConnection event handlers.
func (t *WebRTCTransport) setupHandlers() {
	t.pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		webrtcLog.Info("ICE connection state: %s", state.String())
		switch state {
		case webrtc.ICEConnectionStateFailed:
			t.mu.Lock()
			t.connected = false
			t.mu.Unlock()
		case webrtc.ICEConnectionStateConnected:
			t.mu.Lock()
			t.connected = true
			t.mu.Unlock()
		}
	})

	t.pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		webrtcLog.Info("Connection state: %s", state.String())
	})

	t.pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			webrtcLog.Info("ICE candidate: %s %s",
				c.Typ.String(), c.Address)
		}
	})

	// INCOMING RTP: Catch the raw Opus audio packets from the peer and pass to rtpconn
	t.pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		if track.Codec().MimeType != webrtc.MimeTypeOpus {
			return
		}
		webrtcLog.Info("🔊 Opus audio track received, starting VPN decapsulation")

		t.mu.RLock()
		conn := t.rtpConn
		t.mu.RUnlock()

		if conn == nil {
			return
		}

		for {
			rtpPacket, _, err := track.ReadRTP()
			if err != nil {
				webrtcLog.Error("ReadRTP error: %v", err)
				return
			}
			conn.HandleRTP(rtpPacket.Payload)
		}
	})
}

// buildICEServers converts ICE server info to Pion format.
func (t *WebRTCTransport) buildICEServers(lkServers []livekit.ICEServerInfo) []webrtc.ICEServer {
	var servers []webrtc.ICEServer

	for _, s := range lkServers {
		server := webrtc.ICEServer{URLs: s.URLs}
		if s.Username != "" {
			server.Username = s.Username
			server.Credential = s.Credential
			server.CredentialType = webrtc.ICECredentialTypePassword
		}
		servers = append(servers, server)
	}

	if len(servers) == 0 {
		cfgServers := t.cfg.ICEServersConfig()
		for _, s := range cfgServers {
			server := webrtc.ICEServer{URLs: s.URLs}
			if s.Username != "" {
				server.Username = s.Username
				server.Credential = s.Credential
				server.CredentialType = webrtc.ICECredentialTypePassword
			}
			servers = append(servers, server)
		}
	}

	return servers
}
