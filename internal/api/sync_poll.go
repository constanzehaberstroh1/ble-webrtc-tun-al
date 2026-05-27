package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/salman/ble-webrtc-tun/internal/db"
)

// dataVersion tracks mutations to accounts and pairings.
// Incremented on any add/delete/update of accounts or pairings.
// Long-poll clients compare against this to detect changes.
var (
	dataVersionMu sync.RWMutex
	dataVersion   int64 = 0
	dataVersionCh       = make(chan struct{}, 1) // Signaled when version changes
)

// bumpDataVersion increments the data version and notifies long-poll waiters.
func bumpDataVersion() {
	dataVersionMu.Lock()
	dataVersion++
	dataVersionMu.Unlock()

	// Non-blocking notify
	select {
	case dataVersionCh <- struct{}{}:
	default:
	}
}

// getDataVersion returns the current data version.
func getDataVersion() int64 {
	dataVersionMu.RLock()
	defer dataVersionMu.RUnlock()
	return dataVersion
}

// SyncSnapshot represents the full state of accounts and pairings for sync.
type SyncSnapshot struct {
	Version  int64        `json:"version"`
	Accounts []db.Account `json:"accounts"`
	Pairings []db.Pairing `json:"pairings"`
}

// handleSyncSnapshot returns the current full state of accounts and pairings.
// GET /api/sync/snapshot
func (s *Server) handleSyncSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	accounts, err := s.database.ListAccounts("")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list accounts")
		return
	}

	pairings, err := s.database.ListPairings()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list pairings")
		return
	}

	writeJSON(w, http.StatusOK, SyncSnapshot{
		Version:  getDataVersion(),
		Accounts: accounts,
		Pairings: pairings,
	})
}

// handleSyncLongPoll holds the request open until data changes or timeout.
// GET /api/sync/long-poll?since_version=N
// Returns immediately if current version > since_version.
// Otherwise waits up to 30 seconds for a change.
func (s *Server) handleSyncLongPoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	sinceStr := r.URL.Query().Get("since_version")
	sinceVersion, _ := strconv.ParseInt(sinceStr, 10, 64)

	currentVersion := getDataVersion()

	// If client is already behind, return immediately
	if currentVersion > sinceVersion {
		s.writeSyncResponse(w)
		return
	}

	// Wait for change or timeout (30 seconds)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			// Timeout — return current state with no-change indicator
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"changed": false,
				"version": getDataVersion(),
			})
			return
		case <-dataVersionCh:
			// Data changed — check if version actually increased
			newVersion := getDataVersion()
			if newVersion > sinceVersion {
				s.writeSyncResponse(w)
				return
			}
		}
	}
}

// writeSyncResponse writes the full snapshot as a long-poll response.
func (s *Server) writeSyncResponse(w http.ResponseWriter) {
	accounts, _ := s.database.ListAccounts("")
	pairings, _ := s.database.ListPairings()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"changed":  true,
		"version":  getDataVersion(),
		"accounts": accounts,
		"pairings": pairings,
	})
}

// ---- Sync mutation endpoints ----
// These are called by the client admin to push changes to the server admin.
// They mirror local mutations on the remote side.

// handleSyncAccountCreated is called when a client admin adds an account.
// The server creates the same account with the provided role.
// POST /api/sync/account-created
func (s *Server) handleSyncAccountCreated(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Token       string `json:"token"`
		Role        string `json:"role"`
		BaleUserID  int64  `json:"bale_user_id"`
		DisplayName string `json:"display_name"`
		Phone       string `json:"phone"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Token == "" || req.Role == "" || req.BaleUserID == 0 {
		writeError(w, http.StatusBadRequest, "token, role, and bale_user_id are required")
		return
	}

	// Validate: the account should not already exist with the opposite role
	existing, _ := s.database.GetAccountByBaleUserID(req.BaleUserID)
	if existing != nil {
		if existing.Role != req.Role {
			writeError(w, http.StatusConflict,
				fmt.Sprintf("account %d already exists as %s, cannot be %s", req.BaleUserID, existing.Role, req.Role))
			return
		}
		// Already exists with same role — update token and restore if needed
		s.database.DB.Unscoped().Model(existing).Updates(map[string]interface{}{
			"token":      req.Token,
			"token_hash": db.HashToken(req.Token),
			"deleted_at": nil,
			"enabled":    true,
			"status":     db.StatusIdle,
		})
		if req.DisplayName != "" || req.Phone != "" {
			s.database.UpdateAccountInfo(existing.ID, req.DisplayName, req.Phone, 0)
		}
		bumpDataVersion()
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"message":    "account updated",
			"account_id": existing.ID,
		})
		return
	}

	// Also check soft-deleted
	existingUnscoped, _ := s.database.GetAccountByBaleUserIDUnscoped(req.BaleUserID)
	if existingUnscoped != nil {
		s.database.DB.Unscoped().Model(existingUnscoped).Updates(map[string]interface{}{
			"token":      req.Token,
			"token_hash": db.HashToken(req.Token),
			"role":       req.Role,
			"deleted_at": nil,
			"enabled":    true,
			"status":     db.StatusIdle,
		})
		if req.DisplayName != "" || req.Phone != "" {
			s.database.UpdateAccountInfo(existingUnscoped.ID, req.DisplayName, req.Phone, 0)
		}
		bumpDataVersion()
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"message":    "account restored",
			"account_id": existingUnscoped.ID,
		})
		return
	}

	acct, err := s.database.CreateAccount(req.Token, req.Role, req.BaleUserID)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	// Set display info
	if req.DisplayName != "" || req.Phone != "" {
		s.database.UpdateAccountInfo(acct.ID, req.DisplayName, req.Phone, 0)
	}

	bumpDataVersion()
	apiLog.Info("Sync: account created via sync — ID=%d BaleID=%d Role=%s", acct.ID, req.BaleUserID, req.Role)
	writeJSON(w, http.StatusCreated, acct)
}

// handleSyncAccountDeleted is called when a client admin deletes an account.
// POST /api/sync/account-deleted
func (s *Server) handleSyncAccountDeleted(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		BaleUserID int64 `json:"bale_user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.BaleUserID == 0 {
		writeError(w, http.StatusBadRequest, "bale_user_id is required")
		return
	}

	acct, err := s.database.GetAccountByBaleUserID(req.BaleUserID)
	if err != nil {
		writeError(w, http.StatusNotFound, "account not found")
		return
	}

	// Delete associated pairings first
	pairings, _ := s.database.ListPairings()
	for _, p := range pairings {
		if p.ClientAccountID == acct.ID || p.ServerAccountID == acct.ID {
			s.database.DeletePairing(p.ID)
		}
	}

	if err := s.database.DeleteAccount(acct.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	bumpDataVersion()
	apiLog.Info("Sync: account deleted via sync — BaleID=%d", req.BaleUserID)
	writeOK(w, "account deleted")
}

// handleSyncPairingCreated is called when a pairing is created on the other side.
// POST /api/sync/pairing-created
func (s *Server) handleSyncPairingCreated(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		ClientBaleUserID int64 `json:"client_bale_user_id"`
		ServerBaleUserID int64 `json:"server_bale_user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ClientBaleUserID == 0 || req.ServerBaleUserID == 0 {
		writeError(w, http.StatusBadRequest, "client_bale_user_id and server_bale_user_id are required")
		return
	}

	clientAcct, err := s.database.GetAccountByBaleUserID(req.ClientBaleUserID)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("client account (Bale %d) not found", req.ClientBaleUserID))
		return
	}

	serverAcct, err := s.database.GetAccountByBaleUserID(req.ServerBaleUserID)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("server account (Bale %d) not found", req.ServerBaleUserID))
		return
	}

	// Check if pairing already exists
	existingPairings, _ := s.database.ListPairings()
	for _, p := range existingPairings {
		if p.ClientAccountID == clientAcct.ID && p.ServerAccountID == serverAcct.ID {
			bumpDataVersion()
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"message":    "pairing already exists",
				"pairing_id": p.ID,
			})
			return
		}
	}

	pairing, err := s.database.CreatePairing(clientAcct.ID, serverAcct.ID, "")
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	bumpDataVersion()
	apiLog.Info("Sync: pairing created via sync — client=%d server=%d", req.ClientBaleUserID, req.ServerBaleUserID)
	writeJSON(w, http.StatusCreated, pairing)
}

// handleSyncPairingDeleted is called when a pairing is deleted on the other side.
// POST /api/sync/pairing-deleted
func (s *Server) handleSyncPairingDeleted(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		ClientBaleUserID int64 `json:"client_bale_user_id"`
		ServerBaleUserID int64 `json:"server_bale_user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ClientBaleUserID == 0 || req.ServerBaleUserID == 0 {
		writeError(w, http.StatusBadRequest, "client_bale_user_id and server_bale_user_id are required")
		return
	}

	clientAcct, _ := s.database.GetAccountByBaleUserID(req.ClientBaleUserID)
	serverAcct, _ := s.database.GetAccountByBaleUserID(req.ServerBaleUserID)

	if clientAcct == nil || serverAcct == nil {
		writeOK(w, "accounts not found, pairing likely already deleted")
		return
	}

	// Find and delete the matching pairing
	pairings, _ := s.database.ListPairings()
	for _, p := range pairings {
		if p.ClientAccountID == clientAcct.ID && p.ServerAccountID == serverAcct.ID {
			s.database.DeletePairing(p.ID)
			bumpDataVersion()
			apiLog.Info("Sync: pairing deleted via sync — client=%d server=%d", req.ClientBaleUserID, req.ServerBaleUserID)
			writeOK(w, "pairing deleted")
			return
		}
	}

	writeOK(w, "pairing not found")
}

// handleCheckAccountRole checks if a bale_user_id already exists with a specific role.
// GET /api/sync/check-role?bale_user_id=123
func (s *Server) handleCheckAccountRole(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	baleIDStr := r.URL.Query().Get("bale_user_id")
	baleID, err := strconv.ParseInt(baleIDStr, 10, 64)
	if err != nil || baleID == 0 {
		writeError(w, http.StatusBadRequest, "valid bale_user_id is required")
		return
	}

	acct, _ := s.database.GetAccountByBaleUserID(baleID)
	if acct == nil {
		// Also check soft-deleted
		acct, _ = s.database.GetAccountByBaleUserIDUnscoped(baleID)
	}

	if acct != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"exists":       true,
			"role":         acct.Role,
			"bale_user_id": acct.BaleUserID,
			"account_id":   acct.ID,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"exists": false,
	})
}
