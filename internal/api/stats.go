package api

import (
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/salman/ble-webrtc-tun/internal/db"
)

// handleStats handles GET /api/stats.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Connection stats from DB
	connStats, err := s.database.GetConnectionStats()
	if err != nil {
		connStats = &db.ConnectionStats{}
	}

	// Account counts
	totalClients, _ := s.database.CountAccounts(db.RoleClient, "")
	totalServers, _ := s.database.CountAccounts(db.RoleServer, "")
	idleServers, _ := s.database.CountAccounts(db.RoleServer, db.StatusIdle)
	inCallServers, _ := s.database.CountAccounts(db.RoleServer, db.StatusInCall)
	offlineAccounts, _ := s.database.CountAccounts("", db.StatusOffline)

	// Active sessions from router
	activeSessions := s.router.ActiveSessionCount()

	// Active Bale clients
	activeClients := s.manager.ActiveClientCount()

	uptime := time.Since(s.startTime)

	resp := map[string]interface{}{
		"uptime_seconds":   int(uptime.Seconds()),
		"uptime":           uptime.Truncate(time.Second).String(),
		"active_sessions":  activeSessions,
		"active_clients":   activeClients,
		"total_clients":    totalClients,
		"total_servers":    totalServers,
		"idle_servers":     idleServers,
		"in_call_servers":  inCallServers,
		"offline_accounts": offlineAccounts,
		"connections":      connStats,
		"role":             s.database.Role(),
		"server_urls":      detectServerURLs(r),
	}

	if s.RemoteServerURL != "" {
		resp["remote_server_url"] = s.RemoteServerURL
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleHealth handles GET /api/health.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Detect own URL from the request
	selfURL := detectSelfURL(r)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":      "ok",
		"role":        s.database.Role(),
		"uptime":      time.Since(s.startTime).Truncate(time.Second).String(),
		"db_path":     s.database.Path(),
		"sessions":    s.router.ActiveSessionCount(),
		"server_urls": detectServerURLs(r),
		"self_url":    selfURL,
	})
}

// detectSelfURL returns the server's own URL from the request's Host header.
func detectSelfURL(r *http.Request) string {
	scheme := "https"
	if r.TLS == nil && r.Header.Get("X-Forwarded-Proto") == "" {
		scheme = "http"
	}
	host := r.Host
	if fwdHost := r.Header.Get("X-Forwarded-Host"); fwdHost != "" {
		host = fwdHost
	}
	return scheme + "://" + host
}

// detectServerURLs returns all known URLs for this server.
func detectServerURLs(r *http.Request) []string {
	urls := []string{}

	// 1. Self-detect from request
	selfURL := detectSelfURL(r)
	if selfURL != "" {
		urls = append(urls, selfURL)
	}

	// 2. Explicit SERVER_URL env var
	if serverURL := os.Getenv("SERVER_URL"); serverURL != "" && serverURL != selfURL {
		urls = append(urls, serverURL)
	}

	// 3. CC_DOMAIN (Clever Cloud custom domains)
	if domain := os.Getenv("CC_DOMAIN"); domain != "" {
		for _, d := range strings.Split(domain, ",") {
			d = strings.TrimSpace(d)
			if d != "" {
				u := "https://" + d
				if u != selfURL {
					urls = append(urls, u)
				}
			}
		}
	}

	return urls
}

