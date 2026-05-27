package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/salman/ble-webrtc-tun/internal/config"
	"github.com/salman/ble-webrtc-tun/internal/livekit"
)

// webrtcLog is declared in videotunnel.go

const (
	dataChannelLabel  = "ble-tunnel"
	maxBufferedAmount = 1024 * 1024 // 1MB buffer threshold
)

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

// WebRTCTransport manages the Pion WebRTC connection and DataChannel.
type WebRTCTransport struct {
	cfg        *config.Config
	pc         *webrtc.PeerConnection
	dc         *webrtc.DataChannel
	videoTrack *webrtc.TrackLocalStaticSample // VP8 tunnel track
	videoMode  bool                          // true = use video tunnel instead of DataChannel
	onData     func([]byte)                  // Callback for received data
	mu         sync.RWMutex
	connected  bool
	stats      Stats
	done       chan struct{}
}

// NewWebRTCTransport creates a new WebRTC transport.
func NewWebRTCTransport(cfg *config.Config) *WebRTCTransport {
	return &WebRTCTransport{
		cfg:  cfg,
		done: make(chan struct{}),
		stats: Stats{StartTime: time.Now()},
	}
}

// SetOnData sets the callback for received data.
func (t *WebRTCTransport) SetOnData(fn func([]byte)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onData = fn
}

// Initialize creates the PeerConnection with ICE servers.
func (t *WebRTCTransport) Initialize(iceServersFromLK []livekit.ICEServerInfo) error {
	iceServers := t.buildICEServers(iceServersFromLK)

	pcConfig := webrtc.Configuration{
		ICEServers: iceServers,
		// Allow all ICE candidate types (host, srflx, relay)
		// Relay-only fails when the TURN allocation isn't shared with LiveKit
	}

	settingEngine := webrtc.SettingEngine{}
	settingEngine.SetICETimeouts(5*time.Second, 25*time.Second, 2*time.Second)
	settingEngine.SetDTLSRetransmissionInterval(500 * time.Millisecond)

	// Register only VP8 codec to keep SDP small (Bale has message size limits)
	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeVP8,
			ClockRate: 90000,
		},
		PayloadType: 96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return fmt.Errorf("registering VP8 codec: %w", err)
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

	webrtcLog.Info("PeerConnection initialized with %d ICE servers", len(iceServers))
	return nil
}

// CreateOffer creates an SDP offer (client side).
func (t *WebRTCTransport) CreateOffer() (string, error) {
	// Create the DataChannel before creating offer (no negotiated ID — let browser-style negotiation work)
	dcConfig := &webrtc.DataChannelInit{
		Ordered: boolPtr(true),
	}

	dc, err := t.pc.CreateDataChannel(dataChannelLabel, dcConfig)
	if err != nil {
		return "", fmt.Errorf("creating data channel: %w", err)
	}

	t.setupDataChannel(dc)

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

// Send writes data to the DataChannel or video tunnel.
func (t *WebRTCTransport) Send(data []byte) error {
	t.mu.RLock()
	vm := t.videoMode
	dc := t.dc
	connected := t.connected
	t.mu.RUnlock()

	// Route through video tunnel if enabled
	if vm {
		return t.SendVideo(data)
	}

	if !connected || dc == nil {
		return fmt.Errorf("not connected")
	}

	if dc.BufferedAmount() > maxBufferedAmount {
		return fmt.Errorf("buffer full")
	}

	if err := dc.Send(data); err != nil {
		return fmt.Errorf("sending data: %w", err)
	}

	t.stats.BytesSent.Add(int64(len(data)))
	t.stats.PacketsSent.Add(1)
	return nil
}

// IsVideoMode returns whether the transport uses video tunnel.
func (t *WebRTCTransport) IsVideoMode() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.videoMode
}

// IsConnected returns whether the DataChannel is open.
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

// WaitForConnection blocks until the DataChannel is connected or context is cancelled.
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
	close(t.done)
	if t.dc != nil {
		t.dc.Close()
	}
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
			// Set connected when ICE reconnects (if DC or video track exists)
			t.mu.Lock()
			if t.dc != nil || t.videoMode {
				t.connected = true
			}
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

	// Server side: handle incoming DataChannel from the offerer
	t.pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		webrtcLog.Info("📥 DataChannel received: label=%s", dc.Label())
		t.setupDataChannel(dc)
	})
}

// setupDataChannel configures the DataChannel event handlers.
func (t *WebRTCTransport) setupDataChannel(dc *webrtc.DataChannel) {
	dc.OnOpen(func() {
		webrtcLog.Info("DataChannel '%s' opened", dc.Label())
		t.mu.Lock()
		t.dc = dc
		t.connected = true
		t.mu.Unlock()
	})

	dc.OnClose(func() {
		webrtcLog.Info("DataChannel '%s' closed", dc.Label())
		t.mu.Lock()
		t.connected = false
		t.mu.Unlock()
	})

	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		t.stats.BytesReceived.Add(int64(len(msg.Data)))
		t.stats.PacketsRecv.Add(1)

		t.mu.RLock()
		fn := t.onData
		t.mu.RUnlock()

		if fn != nil {
			fn(msg.Data)
		}
	})

	dc.OnError(func(err error) {
		webrtcLog.Error("DataChannel error: %v", err)
	})

	// Set buffered amount low threshold for flow control
	dc.SetBufferedAmountLowThreshold(maxBufferedAmount / 2)
	dc.OnBufferedAmountLow(func() {
		// Buffer drained, can resume sending
	})
}

// buildICEServers converts ICE server info to Pion format.
func (t *WebRTCTransport) buildICEServers(lkServers []livekit.ICEServerInfo) []webrtc.ICEServer {
	var servers []webrtc.ICEServer

	// Add servers from LiveKit
	for _, s := range lkServers {
		server := webrtc.ICEServer{URLs: s.URLs}
		if s.Username != "" {
			server.Username = s.Username
			server.Credential = s.Credential
			server.CredentialType = webrtc.ICECredentialTypePassword
		}
		servers = append(servers, server)
	}

	// Add configured servers if no LiveKit servers available
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

func boolPtr(b bool) *bool { return &b }
