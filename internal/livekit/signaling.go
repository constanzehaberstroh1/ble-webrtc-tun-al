package livekit

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/salman/ble-webrtc-tun/internal/config"
	"google.golang.org/protobuf/proto"

	lkproto "github.com/livekit/protocol/livekit"
)

// sfuLog is declared in sfutransport.go

// ICEServerInfo holds extracted ICE server credentials from LiveKit.
type ICEServerInfo struct {
	URLs       []string
	Username   string
	Credential string
}

// SignalClient manages the LiveKit WebSocket connection to extract
// ICE/TURN credentials and maintain a fake call presence.
type SignalClient struct {
	cfg        *config.Config
	conn       *websocket.Conn
	iceServers []ICEServerInfo
	mu         sync.RWMutex
	done       chan struct{}
	connected  bool
}

// NewSignalClient creates a new LiveKit signal client.
func NewSignalClient(cfg *config.Config) *SignalClient {
	return &SignalClient{
		cfg:  cfg,
		done: make(chan struct{}),
	}
}

// Connect establishes a WebSocket connection to Bale's LiveKit server
// and extracts ICE server credentials from the JoinResponse.
func (s *SignalClient) Connect(ctx context.Context) error {
	wsURL, err := s.buildWSURL()
	if err != nil {
		return fmt.Errorf("building WS URL: %w", err)
	}

	sfuLog.Info("Connecting to %s", s.cfg.LiveKitWSURL)

	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: false,
		},
		HandshakeTimeout: 15 * time.Second,
		Subprotocols:     []string{"lk-protocol-15"},
	}

	headers := http.Header{
		"User-Agent": []string{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
		"Origin":     []string{"https://web.ble.ir"},
	}

	conn, resp, err := dialer.DialContext(ctx, wsURL, headers)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("websocket dial failed (status %d): %w", resp.StatusCode, err)
		}
		return fmt.Errorf("websocket dial failed: %w", err)
	}

	s.conn = conn
	s.connected = true
	sfuLog.Info("WebSocket connected successfully")

	// Read the first message - should be JoinResponse
	if err := s.readJoinResponse(ctx); err != nil {
		return fmt.Errorf("reading join response: %w", err)
	}

	// Start keepalive loop
	go s.keepAlive(ctx)

	return nil
}

// GetICEServers returns the extracted ICE server configurations.
func (s *SignalClient) GetICEServers() []ICEServerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.iceServers
}

// IsConnected returns whether the WebSocket is connected.
func (s *SignalClient) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// Close gracefully shuts down the connection.
func (s *SignalClient) Close() error {
	close(s.done)
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

// buildWSURL constructs the full WebSocket URL with query parameters.
func (s *SignalClient) buildWSURL() (string, error) {
	u, err := url.Parse(s.cfg.LiveKitWSURL)
	if err != nil {
		return "", err
	}

	q := u.Query()
	q.Set("access_token", s.cfg.LiveKitToken)
	q.Set("auto_subscribe", "1")
	q.Set("sdk", "js")
	q.Set("version", "2.13.6")
	q.Set("protocol", "15")
	q.Set("adaptive_stream", "1")
	u.RawQuery = q.Encode()

	return u.String(), nil
}

// readJoinResponse reads and parses the LiveKit JoinResponse from WebSocket.
func (s *SignalClient) readJoinResponse(ctx context.Context) error {
	// Set read deadline for initial response
	s.conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	defer s.conn.SetReadDeadline(time.Time{})

	_, msg, err := s.conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("reading message: %w", err)
	}

	// Try to parse as protobuf SignalResponse
	sigResp := &lkproto.SignalResponse{}
	if err := proto.Unmarshal(msg, sigResp); err != nil {
		// Might be JSON, try that
		sfuLog.Warn("Protobuf parse failed, trying JSON fallback")
		return s.parseJSONResponse(msg)
	}

	// Extract JoinResponse
	joinResp := sigResp.GetJoin()
	if joinResp == nil {
		sfuLog.Info("First message is not JoinResponse, type: %T", sigResp.Message)
		// Continue anyway, try to extract from subsequent messages
		go s.readMessages(ctx)
		return nil
	}

	s.extractICEServers(joinResp)
	sfuLog.Info("Room joined: %s, participant: %s",
		joinResp.Room.GetName(), joinResp.Participant.GetIdentity())

	// Start reading subsequent messages
	go s.readMessages(ctx)

	return nil
}

// extractICEServers pulls ICE server configuration from JoinResponse.
func (s *SignalClient) extractICEServers(join *lkproto.JoinResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ice := range join.GetIceServers() {
		server := ICEServerInfo{
			URLs:       ice.GetUrls(),
			Username:   ice.GetUsername(),
			Credential: ice.GetCredential(),
		}
		s.iceServers = append(s.iceServers, server)
		sfuLog.Info("ICE Server: urls=%v, username=%s",
			server.URLs, server.Username)
	}

	if len(s.iceServers) == 0 {
		sfuLog.Warn("Warning: No ICE servers in JoinResponse")
	}
}

// parseJSONResponse attempts to parse the response as JSON (fallback).
func (s *SignalClient) parseJSONResponse(data []byte) error {
	var resp map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		sfuLog.Info("Could not parse response as JSON: %v", err)
		sfuLog.Info("Raw response (first 200 bytes): %s", truncate(string(data), 200))
		return nil // Don't fail, continue with default ICE servers
	}

	sfuLog.Info("JSON response: %v", resp)

	// Try to extract ICE servers from JSON response
	if join, ok := resp["join"].(map[string]interface{}); ok {
		if iceServers, ok := join["iceServers"].([]interface{}); ok {
			s.mu.Lock()
			for _, srv := range iceServers {
				if srvMap, ok := srv.(map[string]interface{}); ok {
					info := ICEServerInfo{}
					if urls, ok := srvMap["urls"].([]interface{}); ok {
						for _, u := range urls {
							info.URLs = append(info.URLs, fmt.Sprint(u))
						}
					}
					if u, ok := srvMap["username"].(string); ok {
						info.Username = u
					}
					if c, ok := srvMap["credential"].(string); ok {
						info.Credential = c
					}
					s.iceServers = append(s.iceServers, info)
				}
			}
			s.mu.Unlock()
		}
	}

	return nil
}

// readMessages continuously reads and processes WebSocket messages.
func (s *SignalClient) readMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		default:
		}

		s.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := s.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err) {
				sfuLog.Info("WebSocket closed: %v", err)
				s.mu.Lock()
				s.connected = false
				s.mu.Unlock()
				return
			}
			continue
		}

		// Parse and handle signaling messages
		sigResp := &lkproto.SignalResponse{}
		if err := proto.Unmarshal(msg, sigResp); err != nil {
			continue
		}

		s.handleSignalResponse(sigResp)
	}
}

// handleSignalResponse processes incoming LiveKit signaling messages.
func (s *SignalClient) handleSignalResponse(resp *lkproto.SignalResponse) {
	switch resp.Message.(type) {
	case *lkproto.SignalResponse_Pong:
		// Keepalive response - expected
	case *lkproto.SignalResponse_PongResp:
		// Keepalive response - expected
	case *lkproto.SignalResponse_Update:
		sfuLog.Info("Participant update received")
	case *lkproto.SignalResponse_Trickle:
		sfuLog.Info("ICE trickle candidate received")
	case *lkproto.SignalResponse_Reconnect:
		sfuLog.Info("Reconnect request received")
	default:
		// Silently ignore other messages
	}
}

// keepAlive sends periodic ping messages to keep the connection alive.
func (s *SignalClient) keepAlive(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-ticker.C:
			if !s.IsConnected() {
				return
			}

			// Send LiveKit ping (protobuf)
			pingReq := &lkproto.SignalRequest{
				Message: &lkproto.SignalRequest_Ping{
					Ping: time.Now().UnixMilli(),
				},
			}
			data, err := proto.Marshal(pingReq)
			if err != nil {
				continue
			}

			if err := s.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				sfuLog.Warn("Ping failed: %v", err)
				s.mu.Lock()
				s.connected = false
				s.mu.Unlock()
				return
			}
		}
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
