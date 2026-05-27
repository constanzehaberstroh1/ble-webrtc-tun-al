package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// handleActiveConnections handles GET /api/connections/active.
func (s *Server) handleActiveConnections(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Get sessions from router (in-memory, real-time)
	sessions := s.router.GetAllSessions()

	// Also get from DB (for sessions that might have been missed)
	dbActive, err := s.database.GetActiveConnections()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := map[string]interface{}{
		"sessions":   sessions,
		"db_records": dbActive,
		"count":      len(sessions),
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleConnectionHistory handles GET /api/connections/history?limit=50.
func (s *Server) handleConnectionHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	history, err := s.database.GetConnectionHistory(limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, history)
}

// handleForceEndCall handles POST /api/connections/end/{server_account_id}.
// Forces termination of an active call by server account ID.
func (s *Server) handleForceEndCall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Extract server_account_id from URL: /api/connections/end/123
	parts := strings.Split(strings.TrimSuffix(r.URL.Path, "/"), "/")
	if len(parts) == 0 {
		writeError(w, http.StatusBadRequest, "missing server_account_id")
		return
	}
	idStr := parts[len(parts)-1]
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid server_account_id")
		return
	}

	session := s.router.GetSession(uint(id))
	if session == nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("no active session for server account %d", id))
		return
	}

	s.router.ForceEndCall(uint(id))
	apiLog.Info("Admin force-ended call for server account %d (callID=%d)", id, session.CallID)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":           "call ended",
		"server_account_id": id,
		"call_id":           session.CallID,
	})
}

// handleForceEndAllCalls handles POST /api/connections/end-all.
// Forces termination of ALL active calls.
func (s *Server) handleForceEndAllCalls(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	sessions := s.router.GetAllSessions()
	count := len(sessions)

	for _, session := range sessions {
		s.router.ForceEndCall(session.ServerAccountID)
	}

	apiLog.Info("Admin force-ended ALL calls (%d sessions)", count)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": fmt.Sprintf("%d calls ended", count),
		"count":   count,
	})
}
