// Package api provides the REST API server for managing accounts, pairings,
// connections, and system stats. It replaces the static HTML admin panel
// with a JSON API consumed by the React frontend.
package api

import (
	"context"
	"encoding/json"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"net/http"
	"strings"
	"time"

	"github.com/salman/ble-webrtc-tun/internal/accounts"
	"github.com/salman/ble-webrtc-tun/internal/db"
	"github.com/salman/ble-webrtc-tun/internal/router"
	"github.com/salman/ble-webrtc-tun/internal/webui"
)

var apiLog = logger.New("api")

// Server is the REST API server.
type Server struct {
	mux      *http.ServeMux
	database *db.Database
	manager  *accounts.Manager
	router   *router.Router

	// Server start time for uptime calculation
	startTime time.Time

	// Legacy admin panel reference (for signaling, logs, tunnel status)
	adminPanel interface {
		HandleSignalOffer(w http.ResponseWriter, r *http.Request)
	}



	// Tunnel callbacks (used by client role)
	OnTunnelStart func() error
	OnTunnelStop  func() error
	GetTunnelStatus func() (interface{}, error)
	GetClientID     func() string
	OnForceEndCall  func() (map[string]interface{}, error)

	// Remote server URL (auto-detected from Clever Cloud, used by client)
	RemoteServerURL string
}

// Config holds configuration for the API server.
type Config struct {
	ListenAddr string
	Username   string
	Password   string
}

// NewServer creates a new API server.
func NewServer(database *db.Database, mgr *accounts.Manager, rt *router.Router, cfg Config) *Server {
	s := &Server{
		mux:       http.NewServeMux(),
		database:  database,
		manager:   mgr,
		router:    rt,
		startTime: time.Now(),
	}

	// Seed admin users (safe to call multiple times — won't duplicate)
	if err := database.SeedAdminUsers(map[string]string{
		"salman": "Salman136517",
		"azam":   "136517",
	}); err != nil {
		apiLog.Warn("Failed to seed admin users: %v", err)
	}

	s.registerRoutes()
	return s
}

// Start starts the API server.
func (s *Server) Start(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:    addr,
		Handler: s.corsMiddleware(s.authMiddleware(s.mux)),
	}

	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	apiLog.Info("Server starting on %s", addr)
	return srv.ListenAndServe()
}

// Handler returns the HTTP handler (for embedding in an existing server).
func (s *Server) Handler() http.Handler {
	return s.corsMiddleware(s.authMiddleware(s.mux))
}

func (s *Server) registerRoutes() {
	// Accounts
	s.mux.HandleFunc("/api/accounts/sync-all", s.handleSyncAllAccounts)
	s.mux.HandleFunc("/api/accounts/available-servers", s.handleAvailableServers)
	s.mux.HandleFunc("/api/accounts", s.handleAccounts)
	s.mux.HandleFunc("/api/accounts/", s.handleAccountByID)

	// Pairings
	s.mux.HandleFunc("/api/pairings", s.handlePairings)
	s.mux.HandleFunc("/api/pairings/", s.handlePairingByID)
	s.mux.HandleFunc("/api/pairings/auto", s.handleAutoPair)

	// Connections
	s.mux.HandleFunc("/api/connections/active", s.handleActiveConnections)
	s.mux.HandleFunc("/api/connections/history", s.handleConnectionHistory)
	s.mux.HandleFunc("/api/connections/end-all", s.handleForceEndAllCalls)
	s.mux.HandleFunc("/api/connections/end/", s.handleForceEndCall) // /api/connections/end/{server_account_id}

	// Stats
	s.mux.HandleFunc("/api/stats", s.handleStats)
	s.mux.HandleFunc("/api/health", s.handleHealth)

	// Backup & Restore (full DB export/import)
	s.mux.HandleFunc("/api/db/backup", s.handleBackup)
	s.mux.HandleFunc("/api/db/restore", s.handleRestore)

	// Events (for sync)
	s.mux.HandleFunc("/api/events", s.handleEvents)

	// Migration & Validation
	s.mux.HandleFunc("/api/migrate", s.handleMigrate)
	s.mux.HandleFunc("/api/validate", s.handleValidate)

	// Settings
	s.mux.HandleFunc("/api/settings", s.handleSettings)

	// Guide
	s.mux.HandleFunc("/api/guide", s.handleGuide)
	
	// System logs (for UI floating console)
	s.mux.HandleFunc("/api/logs", s.handleLogs)

	// Auth
	s.mux.HandleFunc("/api/login", s.handleLogin)

	// Sync (HTTP-based client↔server sync)
	s.mux.HandleFunc("/api/sync/push", s.handleSyncPush)
	s.mux.HandleFunc("/api/sync/pull", s.handleSyncPull)
	s.mux.HandleFunc("/api/sync/status", s.handleSyncStatus)

	// Long-polling sync (client admin ↔ server admin)
	s.mux.HandleFunc("/api/sync/long-poll", s.handleSyncLongPoll)
	s.mux.HandleFunc("/api/sync/snapshot", s.handleSyncSnapshot)
	s.mux.HandleFunc("/api/sync/account-created", s.handleSyncAccountCreated)
	s.mux.HandleFunc("/api/sync/account-deleted", s.handleSyncAccountDeleted)
	s.mux.HandleFunc("/api/sync/pairing-created", s.handleSyncPairingCreated)
	s.mux.HandleFunc("/api/sync/pairing-deleted", s.handleSyncPairingDeleted)
	s.mux.HandleFunc("/api/sync/check-role", s.handleCheckAccountRole)

	// Bale OTP login (add accounts via phone number)
	s.mux.HandleFunc("/api/bale/login/start", s.handleBaleLoginStart)
	s.mux.HandleFunc("/api/bale/login/verify", s.handleBaleLoginVerify)

	// Legacy signaling endpoint (forwarded from old admin panel)
	s.mux.HandleFunc("/signal/offer", s.handleSignalForward)

	// Remote server proxy (client → Clever Cloud server)
	s.mux.HandleFunc("/api/remote/server-url", s.handleRemoteServerURL)
	s.mux.HandleFunc("/api/remote/sync-all", s.handleRemoteSyncAll)
	s.mux.HandleFunc("/api/remote/accounts", s.handleRemoteAccounts)
	s.mux.HandleFunc("/api/remote/accounts/create", s.handleRemoteCreateAccount)
	s.mux.HandleFunc("/api/remote/sync/pull", s.handleRemoteSyncPull)
	s.mux.HandleFunc("/api/remote/sync/push", s.handleRemoteSyncPush)
	s.mux.HandleFunc("/api/remote/sync/push-accounts", s.handleRemoteSyncPushAccounts)
	s.mux.HandleFunc("/api/remote/sync/push-pairings", s.handleRemotePushAllPairings)
	s.mux.HandleFunc("/api/remote/accounts/", s.handleRemotePushSingleAccount)  // /api/remote/accounts/{id}/push
	s.mux.HandleFunc("/api/remote/pairings/", s.handleRemotePushSinglePairing) // /api/remote/pairings/{id}/push
	s.mux.HandleFunc("/api/remote/db/backup", s.handleRemoteDBBackup)
	s.mux.HandleFunc("/api/remote/db/restore", s.handleRemoteDBRestore)
	s.mux.HandleFunc("/api/remote/pull-accounts", s.handleRemotePullAccounts)
	s.mux.HandleFunc("/api/remote/sync-from-server", s.handleRemoteSyncFromServer)

	// Tunnel controls
	s.mux.HandleFunc("/api/tunnel/start", s.handleTunnelStart)
	s.mux.HandleFunc("/api/tunnel/stop", s.handleTunnelStop)
	s.mux.HandleFunc("/api/tunnel/status", s.handleTunnelStatus)
	s.mux.HandleFunc("/api/tunnel/force-end-call", s.handleTunnelForceEndCall)
	s.mux.HandleFunc("/api/client-id", s.handleClientID)

	// Web Terminal (xterm.js)
	s.mux.HandleFunc("/api/terminal/ws", s.handleTerminalWS)
	s.mux.HandleFunc("/api/terminal/info", s.handleTerminalInfo)

	// Embedded React admin panel (served at root)
	s.mux.Handle("/", webui.Handler(""))
}

// SetAdminPanel sets the legacy admin panel reference for signaling forwarding.
func (s *Server) SetAdminPanel(panel interface {
	HandleSignalOffer(w http.ResponseWriter, r *http.Request)
}) {
	s.adminPanel = panel
}



func (s *Server) handleSignalForward(w http.ResponseWriter, r *http.Request) {
	if s.adminPanel != nil {
		s.adminPanel.HandleSignalOffer(w, r)
		return
	}
	http.Error(w, "signaling not available", http.StatusServiceUnavailable)
}

func (s *Server) handleTunnelStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.OnTunnelStart != nil {
		if err := s.OnTunnelStart(); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeOK(w, "Tunnel started")
		return
	}
	writeError(w, http.StatusNotImplemented, "tunnel control not available")
}

func (s *Server) handleTunnelStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.OnTunnelStop != nil {
		if err := s.OnTunnelStop(); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeOK(w, "Tunnel stopped")
		return
	}
	writeError(w, http.StatusNotImplemented, "tunnel control not available")
}

func (s *Server) handleTunnelStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.GetTunnelStatus != nil {
		status, err := s.GetTunnelStatus()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, status)
		return
	}
	// Server role: tunnel controls not available
	writeError(w, http.StatusNotFound, "tunnel control not available in server mode")
}

func (s *Server) handleClientID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	clientID := ""
	if s.GetClientID != nil {
		clientID = s.GetClientID()
	}
	writeJSON(w, http.StatusOK, map[string]string{"client_id": clientID})
}

// handleForceEndCall handles POST /api/tunnel/force-end-call.
// Sends ENDCALL command to all paired server accounts via Bale chat,
// waits for acknowledgment, and cleans up messages.
func (s *Server) handleTunnelForceEndCall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.OnForceEndCall != nil {
		result, err := s.OnForceEndCall()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	writeError(w, http.StatusNotImplemented, "force end call not available")
}

// ---- Middleware ----

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for health check, signaling, login, and frontend static files
		if r.URL.Path == "/api/health" || r.URL.Path == "/api/login" ||
			strings.HasPrefix(r.URL.Path, "/signal/") ||
			!strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}

		user, pass, ok := r.BasicAuth()
		if !ok {
			// For WebSocket endpoints: extract auth from ?auth= query parameter.
			// WebSocket subprotocols can't contain spaces, so the frontend
			// passes 'Basic base64(user:pass)' as a URL-encoded query param.
			if r.URL.Path == "/api/terminal/ws" {
				if authParam := r.URL.Query().Get("auth"); authParam != "" {
					r.Header.Set("Authorization", authParam)
					user, pass, ok = r.BasicAuth()
				}
			}
			if !ok {
				w.Header().Set("WWW-Authenticate", `Basic realm="API"`)
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
		}

		// Authenticate against database
		if _, err := s.database.AuthenticateAdmin(user, pass); err != nil {
			w.Header().Set("WWW-Authenticate", `Basic realm="API"`)
			writeError(w, http.StatusUnauthorized, "invalid credentials")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ---- Response helpers ----

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeOK(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusOK, map[string]string{"message": msg})
}

// extractID extracts the last path segment as a numeric ID.
// e.g., "/api/accounts/5" → "5"
func extractID(path, prefix string) string {
	trimmed := strings.TrimPrefix(path, prefix)
	trimmed = strings.TrimSuffix(trimmed, "/")
	// Handle sub-paths like "/api/accounts/5/info"
	parts := strings.SplitN(trimmed, "/", 2)
	return parts[0]
}

// extractSubPath extracts the sub-path after the ID.
// e.g., "/api/accounts/5/info" → "info"
func extractSubPath(path, prefix string) string {
	trimmed := strings.TrimPrefix(path, prefix)
	trimmed = strings.TrimSuffix(trimmed, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) > 1 {
		return parts[1]
	}
	return ""
}
