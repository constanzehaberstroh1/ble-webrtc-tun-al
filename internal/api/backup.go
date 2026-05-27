package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/salman/ble-webrtc-tun/internal/db"
)

// BackupData represents a full database export for backup/restore.
type BackupData struct {
	Version   string          `json:"version"`
	Role      string          `json:"role"`
	CreatedAt string          `json:"created_at"`
	Accounts  []BackupAccount `json:"accounts"`
	Pairings  []BackupPairing `json:"pairings"`
	Settings  []db.Setting    `json:"settings"`
}

// BackupAccount includes the token for full restore capability.
type BackupAccount struct {
	BaleUserID  int64  `json:"bale_user_id"`
	Token       string `json:"token"`
	TokenHash   string `json:"token_hash"`
	Role        string `json:"role"`
	DisplayName string `json:"display_name"`
	Phone       string `json:"phone"`
	AccessHash  int64  `json:"access_hash"`
	Enabled     bool   `json:"enabled"`
}

// BackupPairing stores pairings by Bale user IDs (portable across DBs).
type BackupPairing struct {
	ClientBaleUserID int64  `json:"client_bale_user_id"`
	ServerBaleUserID int64  `json:"server_bale_user_id"`
	OwnerID          string `json:"owner_id"`
	Active           bool   `json:"active"`
}

// handleBackup handles GET /api/db/backup.
// Exports all accounts (with tokens), pairings, and settings as JSON.
func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
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

	settings, err := s.database.ListSettings()
	if err != nil {
		settings = []db.Setting{}
	}

	backupAccounts := make([]BackupAccount, 0, len(accounts))
	for _, a := range accounts {
		// Always compute token hash from the actual token
		tokenHash := a.TokenHash
		if tokenHash == "" && a.Token != "" {
			tokenHash = db.HashToken(a.Token)
		}
		backupAccounts = append(backupAccounts, BackupAccount{
			BaleUserID:  a.BaleUserID,
			Token:       a.Token,
			TokenHash:   tokenHash,
			Role:        a.Role,
			DisplayName: a.DisplayName,
			Phone:       a.Phone,
			AccessHash:  a.AccessHash,
			Enabled:     a.Enabled,
		})
	}

	backupPairings := make([]BackupPairing, 0, len(pairings))
	for _, p := range pairings {
		clientBaleID := int64(0)
		serverBaleID := int64(0)
		if p.ClientAccount != nil {
			clientBaleID = p.ClientAccount.BaleUserID
		}
		if p.ServerAccount != nil {
			serverBaleID = p.ServerAccount.BaleUserID
		}
		// Skip pairings where we can't resolve Bale IDs
		if clientBaleID == 0 || serverBaleID == 0 {
			continue
		}
		backupPairings = append(backupPairings, BackupPairing{
			ClientBaleUserID: clientBaleID,
			ServerBaleUserID: serverBaleID,
			OwnerID:          p.OwnerID,
			Active:           p.Active,
		})
	}

	backup := BackupData{
		Version:   "1.0",
		Role:      s.database.Role(),
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Accounts:  backupAccounts,
		Pairings:  backupPairings,
		Settings:  settings,
	}

	filename := fmt.Sprintf("ble-tunnel-%s-backup-%s.json",
		s.database.Role(), time.Now().Format("20060102-150405"))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	json.NewEncoder(w).Encode(backup)
}

// handleRestore handles POST /api/db/restore.
// Imports accounts, pairings, and settings from a backup JSON.
// Existing records are updated/restored; new ones are created.
func (s *Server) handleRestore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var backup BackupData
	if err := json.NewDecoder(r.Body).Decode(&backup); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if backup.Version == "" {
		writeError(w, http.StatusBadRequest, "invalid backup: missing version")
		return
	}

	accountsCreated := 0
	accountsUpdated := 0
	accountsFailed := 0

	// 1. Restore accounts
	for _, ba := range backup.Accounts {
		if ba.BaleUserID == 0 {
			accountsFailed++
			continue
		}

		existing, _ := s.database.GetAccountByBaleUserID(ba.BaleUserID)
		if existing != nil {
			// Update existing account
			updates := map[string]interface{}{
				"enabled": ba.Enabled,
			}
			if ba.Token != "" {
				updates["token"] = ba.Token
				updates["token_hash"] = db.HashToken(ba.Token)
			}
			if ba.DisplayName != "" {
				updates["display_name"] = ba.DisplayName
			}
			if ba.Phone != "" {
				updates["phone"] = ba.Phone
			}
			if ba.AccessHash != 0 {
				updates["access_hash"] = ba.AccessHash
			}
			s.database.UpdateAccount(existing.ID, updates)
			accountsUpdated++
			continue
		}

		// Check soft-deleted
		existingUnscoped, _ := s.database.GetAccountByBaleUserIDUnscoped(ba.BaleUserID)
		if existingUnscoped != nil {
			s.database.DB.Unscoped().Model(existingUnscoped).Updates(map[string]interface{}{
				"token":        ba.Token,
				"token_hash":   db.HashToken(ba.Token),
				"role":         ba.Role,
				"display_name": ba.DisplayName,
				"phone":        ba.Phone,
				"access_hash":  ba.AccessHash,
				"enabled":      ba.Enabled,
				"deleted_at":   nil,
				"status":       db.StatusIdle,
			})
			accountsUpdated++
			continue
		}

		// Create new account
		token := ba.Token
		if token == "" {
			token = "backup-placeholder"
		}
		acct, err := s.database.CreateAccount(token, ba.Role, ba.BaleUserID)
		if err != nil {
			accountsFailed++
			continue
		}
		if ba.DisplayName != "" || ba.Phone != "" {
			s.database.UpdateAccountInfo(acct.ID, ba.DisplayName, ba.Phone, ba.AccessHash)
		}
		accountsCreated++
	}

	// 2. Restore pairings
	pairingsCreated := 0
	pairingsFailed := 0

	for _, bp := range backup.Pairings {
		if bp.ClientBaleUserID == 0 || bp.ServerBaleUserID == 0 {
			pairingsFailed++
			continue
		}

		clientAcct, _ := s.database.GetAccountByBaleUserID(bp.ClientBaleUserID)
		serverAcct, _ := s.database.GetAccountByBaleUserID(bp.ServerBaleUserID)
		if clientAcct == nil || serverAcct == nil {
			pairingsFailed++
			continue
		}

		// Check if pairing already exists
		existingPairings, _ := s.database.ListPairings()
		alreadyExists := false
		for _, p := range existingPairings {
			if p.ClientAccountID == clientAcct.ID && p.ServerAccountID == serverAcct.ID {
				alreadyExists = true
				break
			}
		}
		if alreadyExists {
			continue
		}

		_, err := s.database.CreatePairing(clientAcct.ID, serverAcct.ID, bp.OwnerID)
		if err != nil {
			pairingsFailed++
			continue
		}
		pairingsCreated++
	}

	// 3. Restore settings
	settingsRestored := 0
	for _, setting := range backup.Settings {
		if setting.Key != "" {
			s.database.SetSetting(setting.Key, setting.Value)
			settingsRestored++
		}
	}

	if accountsCreated > 0 || pairingsCreated > 0 {
		bumpDataVersion()
	}

	apiLog.Info("Backup restored: %d accounts created, %d updated, %d pairings, %d settings",
		accountsCreated, accountsUpdated, pairingsCreated, settingsRestored)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"accounts_created":  accountsCreated,
		"accounts_updated":  accountsUpdated,
		"accounts_failed":   accountsFailed,
		"pairings_created":  pairingsCreated,
		"pairings_failed":   pairingsFailed,
		"settings_restored": settingsRestored,
		"backup_version":    backup.Version,
		"backup_role":       backup.Role,
		"backup_created_at": backup.CreatedAt,
	})
}
