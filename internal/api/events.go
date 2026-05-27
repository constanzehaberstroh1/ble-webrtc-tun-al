package api

import (
	"net/http"
	"strconv"

	"github.com/salman/ble-webrtc-tun/internal/logger"
)

// handleEvents handles GET /api/events?since=N&limit=M.
// Used by the sync protocol and admin panel for real-time event streaming.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	sinceID := uint(0)
	if s := r.URL.Query().Get("since"); s != "" {
		if parsed, err := strconv.ParseUint(s, 10, 32); err == nil {
			sinceID = uint(parsed)
		}
	}

	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	events, err := s.database.GetEventsSince(sinceID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	latestID, _ := s.database.GetLatestEventID()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"events":    events,
		"latest_id": latestID,
		"count":     len(events),
	})
}

// handleLogs handles GET /api/logs?limit=N.
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	logs := logger.GetLogs(limit)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"logs": logs,
	})
}
