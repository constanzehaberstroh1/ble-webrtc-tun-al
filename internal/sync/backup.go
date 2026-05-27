package sync

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/salman/ble-webrtc-tun/internal/db"
	"github.com/salman/ble-webrtc-tun/internal/logger"
)

var backupLog = logger.New("backup")

// BackupWorker periodically backs up the remote server's database state
// and auto-restores it when the server restarts with an empty DB.
type BackupWorker struct {
	database   *db.Database
	serverURL  string
	authHeader string
	backupPath string
	stopCh     chan struct{}
	running    bool
}

// backupPayload mirrors the BackupData structure from the API.
type backupPayload struct {
	Accounts []backupAccount `json:"accounts"`
	Pairings []backupPairing `json:"pairings"`
}

type backupAccount struct {
	BaleUserID  int64  `json:"bale_user_id"`
	AccessHash  int64  `json:"access_hash"`
	Token       string `json:"token"`
	Role        string `json:"role"`
	DisplayName string `json:"display_name"`
	Phone       string `json:"phone"`
	Enabled     bool   `json:"enabled"`
}

type backupPairing struct {
	ClientBaleUserID int64  `json:"client_bale_user_id"`
	ServerBaleUserID int64  `json:"server_bale_user_id"`
	OwnerID          string `json:"owner_id"`
	Active           bool   `json:"active"`
}

// NewBackupWorker creates a backup worker that saves server state locally.
func NewBackupWorker(database *db.Database, serverURL, username, password string) *BackupWorker {
	auth := "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
	backupDir := "data"
	os.MkdirAll(backupDir, 0755)

	return &BackupWorker{
		database:   database,
		serverURL:  serverURL,
		authHeader: auth,
		backupPath: filepath.Join(backupDir, "server_backup.json"),
		stopCh:     make(chan struct{}),
	}
}

// Start begins the backup worker.
func (w *BackupWorker) Start() {
	if w.running || w.serverURL == "" {
		return
	}
	w.running = true

	go func() {
		// Wait for server to be up, then check if it needs restoring
		time.Sleep(5 * time.Second)
		w.checkAndRestore()

		// Periodic backup every 2 minutes
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-w.stopCh:
				return
			case <-ticker.C:
				w.doBackup()
			}
		}
	}()

	backupLog.Info("Backup worker started (path=%s)", w.backupPath)
}

// Stop stops the backup worker.
func (w *BackupWorker) Stop() {
	if !w.running {
		return
	}
	w.running = false
	close(w.stopCh)
}

// checkAndRestore checks if the server has an empty DB and restores from backup.
func (w *BackupWorker) checkAndRestore() {
	// First, check if server is alive and has data
	serverAccounts := w.fetchServerAccountCount()
	if serverAccounts < 0 {
		backupLog.Warn("Cannot reach server for restore check — will retry on next cycle")
		return
	}

	if serverAccounts > 0 {
		// Server has data — just do a backup
		backupLog.Info("Server has %d accounts — no restore needed, backing up", serverAccounts)
		w.doBackup()
		return
	}

	// Server has 0 accounts — check if we have a backup to restore
	backupLog.Info("Server has 0 accounts — checking for backup to restore...")

	backup, err := w.loadLocalBackup()
	if err != nil || len(backup.Accounts) == 0 {
		// No backup available — try pushing local accounts instead
		backupLog.Info("No backup file — pushing local accounts to server")
		w.pushLocalAccountsToServer()
		return
	}

	// We have a backup — restore it
	backupLog.Info("Restoring server from backup (%d accounts, %d pairings)",
		len(backup.Accounts), len(backup.Pairings))
	w.restoreToServer(backup)
}

// doBackup downloads the server's full state and saves locally.
func (w *BackupWorker) doBackup() {
	resp, err := w.doHTTPRequest("GET", "/api/db/backup", nil)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	// Verify it's valid JSON and has content
	var test backupPayload
	if json.Unmarshal(body, &test) != nil || len(test.Accounts) == 0 {
		return // Don't overwrite good backup with empty data
	}

	if err := os.WriteFile(w.backupPath, body, 0600); err != nil {
		backupLog.Warn("Failed to write backup: %v", err)
		return
	}

	backupLog.Info("Server backup saved (%d accounts, %d pairings)",
		len(test.Accounts), len(test.Pairings))
}

// loadLocalBackup reads the saved backup file.
func (w *BackupWorker) loadLocalBackup() (*backupPayload, error) {
	data, err := os.ReadFile(w.backupPath)
	if err != nil {
		return nil, err
	}

	var backup backupPayload
	if err := json.Unmarshal(data, &backup); err != nil {
		return nil, err
	}
	return &backup, nil
}

// restoreToServer pushes a backup to the server's restore endpoint.
func (w *BackupWorker) restoreToServer(backup *backupPayload) {
	body, _ := json.Marshal(backup)
	resp, err := w.doHTTPRequest("POST", "/api/db/restore", bytes.NewReader(body))
	if err != nil {
		backupLog.Warn("Failed to restore server: %v", err)
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	backupLog.Info("Server restore result: %s", string(respBody))
}

// pushLocalAccountsToServer pushes all local accounts to the server.
func (w *BackupWorker) pushLocalAccountsToServer() {
	localAccounts, err := w.database.ListAccounts("")
	if err != nil || len(localAccounts) == 0 {
		return
	}

	// Build a backup from local data
	backup := backupPayload{}
	for _, a := range localAccounts {
		backup.Accounts = append(backup.Accounts, backupAccount{
			BaleUserID:  a.BaleUserID,
			AccessHash:  a.AccessHash,
			Token:       a.Token,
			Role:        a.Role,
			DisplayName: a.DisplayName,
			Phone:       a.Phone,
			Enabled:     a.Enabled,
		})
	}

	// Include pairings
	pairings, _ := w.database.ListPairings()
	for _, p := range pairings {
		if p.ClientAccount != nil && p.ServerAccount != nil {
			backup.Pairings = append(backup.Pairings, backupPairing{
				ClientBaleUserID: p.ClientAccount.BaleUserID,
				ServerBaleUserID: p.ServerAccount.BaleUserID,
				OwnerID:          p.OwnerID,
				Active:           p.Active,
			})
		}
	}

	if len(backup.Accounts) > 0 {
		backupLog.Info("Pushing %d local accounts to server", len(backup.Accounts))
		w.restoreToServer(&backup)
	}
}

// fetchServerAccountCount returns the number of accounts on the server, or -1 on error.
func (w *BackupWorker) fetchServerAccountCount() int {
	resp, err := w.doHTTPRequest("GET", "/api/accounts", nil)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return -1
	}

	var accounts []json.RawMessage
	if json.NewDecoder(resp.Body).Decode(&accounts) != nil {
		return -1
	}
	return len(accounts)
}

// doHTTPRequest makes an authenticated HTTP request.
func (w *BackupWorker) doHTTPRequest(method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, w.serverURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", w.authHeader)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	return client.Do(req)
}
