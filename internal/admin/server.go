package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	pty "github.com/creack/pty"
	"github.com/gorilla/websocket"
	"github.com/salman/ble-webrtc-tun/internal/config"
	"github.com/salman/ble-webrtc-tun/internal/transport"
)

var adminLog = logger.New("admin")

// LogEntry represents a log message.
type LogEntry struct {
	Time    string `json:"time"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

// Server is the admin panel HTTP server.
type Server struct {
	cfg       *config.Config
	transport *transport.WebRTCTransport
	mux       *http.ServeMux
	upgrader  websocket.Upgrader

	// Signaling: pending SDP offer from client
	sdpOfferCh  chan string
	sdpAnswerCh chan string

	// Tunnel status tracking
	tunnelStatus TunnelStatus
	statusMu     sync.RWMutex
	startTime    time.Time
}

// TunnelStatus tracks the current tunnel state.
type TunnelStatus struct {
	BaleConnected    bool   `json:"bale_connected"`
	LiveKitJoined    bool   `json:"livekit_joined"`
	TunnelActive     bool   `json:"tunnel_active"`
	Mode             string `json:"mode"`
	ClientIP         string `json:"client_ip"`
	RoomID           string `json:"room_id"`
	CallID           string `json:"call_id"`
	ConnectedSince   string `json:"connected_since"`
	TotalSessions    int    `json:"total_sessions"`
	BytesSent        int64  `json:"bytes_sent"`
	BytesReceived    int64  `json:"bytes_received"`
	ActiveConns      int    `json:"active_connections"`
	ActiveChannels   int    `json:"active_channels"`
	TotalChannels    int    `json:"total_channels"`
	SpeedUp          int64  `json:"speed_up"`
	SpeedDown        int64  `json:"speed_down"`
	PrevBytesSent    int64  `json:"-"` // internal, not JSON
	PrevBytesRecv    int64  `json:"-"`
}

// NewServer creates a new admin panel server.
func NewServer(cfg *config.Config, t *transport.WebRTCTransport) *Server {
	s := &Server{
		cfg:         cfg,
		transport:   t,
		mux:         http.NewServeMux(),
		sdpOfferCh:  make(chan string, 1),
		sdpAnswerCh: make(chan string, 1),
		startTime:   time.Now(),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}

	s.registerRoutes()
	return s
}

// Start starts the admin panel HTTP server.
func (s *Server) Start(ctx context.Context) error {
	srv := &http.Server{
		Addr:    s.cfg.AdminListenAddr,
		Handler: s.authMiddleware(s.mux),
	}

	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	adminLog.Info("Panel starting on %s", s.cfg.AdminListenAddr)
	return srv.ListenAndServe()
}

// AddLog adds a log entry via the centralized async logger.
// This is a compatibility wrapper — the centralized logger handles
// ring-buffer storage and WebSocket fan-out automatically.
func (s *Server) AddLog(level, message string) {
	switch strings.ToLower(level) {
	case "error":
		adminLog.Error("%s", message)
	case "warn", "warning":
		adminLog.Warn("%s", message)
	case "debug":
		adminLog.Debug("%s", message)
	default:
		adminLog.Info("%s", message)
	}
}

// GetSDPOffer returns the channel for receiving SDP offers from clients.
func (s *Server) GetSDPOfferCh() chan string {
	return s.sdpOfferCh
}

// GetSDPAnswerCh returns the channel for sending SDP answers to clients.
func (s *Server) GetSDPAnswerCh() chan string {
	return s.sdpAnswerCh
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/", s.handleDashboard)
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/api/status", s.handleStatus)
	s.mux.HandleFunc("/api/logs", s.handleLogs)
	s.mux.HandleFunc("/api/logs/ws", s.handleLogsWS)
	s.mux.HandleFunc("/api/shell", s.handleShell)
	s.mux.HandleFunc("/api/shell/ws", s.handleShellWS)
	s.mux.Handle("/admin/static/", StaticHandler())

	// SDP signaling endpoint (no auth - client needs access)
	s.mux.HandleFunc("/signal/offer", s.HandleSignalOffer)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for signaling, health, and static asset endpoints
		if strings.HasPrefix(r.URL.Path, "/signal/") || r.URL.Path == "/health" || strings.HasPrefix(r.URL.Path, "/admin/static/") {
			next.ServeHTTP(w, r)
			return
		}

		user, pass, ok := r.BasicAuth()
		if !ok || user != s.cfg.AdminUsername || pass != s.cfg.AdminPassword {
			w.Header().Set("WWW-Authenticate", `Basic realm="Admin Panel"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl := template.Must(template.New("dashboard").Parse(dashboardHTML))
	tmpl.Execute(w, nil)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	stats := s.transport.GetStats()

	s.statusMu.RLock()
	tStatus := s.tunnelStatus
	s.statusMu.RUnlock()

	uptime := time.Since(s.startTime).Truncate(time.Second).String()

	resp := map[string]interface{}{
		"uptime":       uptime,
		"transport":    stats,
		"tunnel":       tStatus,
		"server_role":  s.cfg.Role,
		"bale_mode":    s.cfg.BaleAccessToken != "",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// SetTunnelStatus updates the tunnel status.
func (s *Server) SetTunnelStatus(fn func(*TunnelStatus)) {
	s.statusMu.Lock()
	fn(&s.tunnelStatus)
	s.statusMu.Unlock()
}

// handleLogs serves recent logs from the centralized ring buffer.
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	logs := logger.GetLogs(500)
	// Convert to legacy format for the admin template
	type legacyEntry struct {
		Time    string `json:"time"`
		Level   string `json:"level"`
		Message string `json:"message"`
	}
	result := make([]legacyEntry, len(logs))
	for i, l := range logs {
		result[i] = legacyEntry{
			Time:    l.Timestamp,
			Level:   strings.ToLower(l.Level),
			Message: fmt.Sprintf("[%s] %s", l.Component, l.Message),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// handleLogsWS streams logs via WebSocket using the centralized subscriber.
func (s *Server) handleLogsWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// Subscribe to centralized log stream
	sub := logger.Subscribe(logger.DEBUG, "")
	defer logger.Unsubscribe(sub)

	// Read pump — detect disconnection
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// Write pump — stream entries in legacy format
	for {
		select {
		case entry, ok := <-sub.Ch:
			if !ok {
				return
			}
			legacy := LogEntry{
				Time:    entry.Timestamp,
				Level:   strings.ToLower(entry.Level),
				Message: fmt.Sprintf("[%s] %s", entry.Component, entry.Message),
			}
			if err := conn.WriteJSON(legacy); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}

func (s *Server) handleShell(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Command string `json:"command"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", "-c", req.Command)
	output, err := cmd.CombinedOutput()

	resp := map[string]interface{}{
		"output": string(output),
		"error":  "",
	}
	if err != nil {
		resp["error"] = err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleShellWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// Spawn a real bash shell with PTY
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/bash"
	}
	cmd := exec.Command(shell)
	cmd.Env = append(os.Environ(),
		"TERM=xterm-256color",
		"COLORTERM=truecolor",
		"LANG=en_US.UTF-8",
	)

	ptmx, err := pty.Start(cmd)
	if err != nil {
		conn.WriteMessage(websocket.TextMessage,
			[]byte(fmt.Sprintf("\r\nError starting shell: %v\r\n", err)))
		return
	}
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
		ptmx.Close()
	}()

	// Set initial terminal size
	pty.Setsize(ptmx, &pty.Winsize{Rows: 24, Cols: 80})

	var wsMu sync.Mutex
	done := make(chan struct{})

	// PTY → WebSocket (output)
	go func() {
		buf := make([]byte, 8192)
		for {
			n, err := ptmx.Read(buf)
			if err != nil {
				break
			}
			wsMu.Lock()
			conn.WriteMessage(websocket.BinaryMessage, buf[:n])
			wsMu.Unlock()
		}
		close(done)
	}()

	// WebSocket → PTY (input + resize)
	go func() {
		for {
			msgType, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}

			if msgType == websocket.TextMessage {
				// Check for resize command
				var resizeCmd struct {
					Type string `json:"type"`
					Cols int    `json:"cols"`
					Rows int    `json:"rows"`
				}
				if json.Unmarshal(msg, &resizeCmd) == nil && resizeCmd.Type == "resize" {
					if resizeCmd.Cols > 0 && resizeCmd.Rows > 0 {
						pty.Setsize(ptmx, &pty.Winsize{
							Rows: uint16(resizeCmd.Rows),
							Cols: uint16(resizeCmd.Cols),
						})
					}
					continue
				}
				// Regular text input
				ptmx.Write(msg)
			} else if msgType == websocket.BinaryMessage {
				ptmx.Write(msg)
			}
		}
	}()

	<-done
}

// HandleSignalOffer handles SDP offer forwarding from clients.
func (s *Server) HandleSignalOffer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	adminLog.Info("Received SDP offer from client (%d bytes)", len(body))
	s.AddLog("info", "SDP offer received from client")

	// Send offer to server main loop
	select {
	case s.sdpOfferCh <- string(body):
	default:
		http.Error(w, "Server busy", http.StatusServiceUnavailable)
		return
	}

	// Wait for answer from server main loop
	select {
	case answer := <-s.sdpAnswerCh:
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(answer))
		adminLog.Info("SDP answer sent to client")
		s.AddLog("info", "SDP answer sent to client")
	case <-time.After(30 * time.Second):
		http.Error(w, "Timeout waiting for answer", http.StatusGatewayTimeout)
	}
}
