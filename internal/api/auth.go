package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/salman/ble-webrtc-tun/internal/db"
)

// handleLogin validates credentials and returns a success response.
// The frontend stores the credentials for subsequent Basic Auth requests.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	user, err := s.database.AuthenticateAdmin(req.Username, req.Password)
	if err != nil {
		apiLog.Warn("Login failed for '%s': %v", req.Username, err)
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	apiLog.Info("Admin '%s' logged in", user.Username)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":  "login successful",
		"username": user.Username,
	})
}

// handleSyncPush receives events from a remote peer and applies them locally.
// POST /api/sync/push — body: { "events": [...] }
func (s *Server) handleSyncPush(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Events []syncEventData `json:"events"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	applied := 0
	for _, e := range req.Events {
		t, _ := time.Parse(time.RFC3339, e.Timestamp)
		event := db.Event{
			Type:      e.Type,
			Payload:   e.Payload,
			Source:    e.Source,
			Timestamp: t,
		}
		if err := s.database.ApplyEvent(event); err != nil {
			apiLog.Warn("Sync push: failed to apply event: %v", err)
			continue
		}
		applied++
	}

	apiLog.Info("Sync push: applied %d/%d events", applied, len(req.Events))
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"applied": applied,
		"total":   len(req.Events),
	})
}

// handleSyncPull returns events since a given sequence ID.
// GET /api/sync/pull?since=0&limit=500
func (s *Server) handleSyncPull(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	sinceStr := r.URL.Query().Get("since")
	since, _ := strconv.Atoi(sinceStr)
	limitStr := r.URL.Query().Get("limit")
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 {
		limit = 500
	}

	events, err := s.database.GetEventsSince(uint(since), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get events")
		return
	}

	latestID, _ := s.database.GetLatestEventID()

	var data []syncEventData
	for _, e := range events {
		data = append(data, syncEventData{
			ID:        e.ID,
			Type:      e.Type,
			Payload:   e.Payload,
			Source:    e.Source,
			Timestamp: e.Timestamp.Format(time.RFC3339),
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"events":    data,
		"latest_id": latestID,
	})
}

// handleSyncStatus returns the current sync state.
// GET /api/sync/status
func (s *Server) handleSyncStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	latestID, _ := s.database.GetLatestEventID()
	acctCount, _ := s.database.CountAccounts("", "")
	var pairCount int64
	s.database.DB.Model(&db.Pairing{}).Where("active = ?", true).Count(&pairCount)
	lastSyncStr, _ := s.database.GetSetting("sync_last_remote_seq")

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"role":            s.database.Role(),
		"latest_event_id": latestID,
		"accounts":        acctCount,
		"pairings":        pairCount,
		"last_sync_seq":   lastSyncStr,
		"timestamp":       time.Now().Format(time.RFC3339),
	})
}

// syncEventData is the wire format for sync events over HTTP.
type syncEventData struct {
	ID        uint   `json:"id"`
	Type      string `json:"type"`
	Payload   string `json:"payload"`
	Source    string `json:"source"`
	Timestamp string `json:"timestamp"`
}
