package api

import (
	"encoding/json"
	"net/http"
	"os"
)

// handleMigrate triggers migration from .env.tokens to database.
// POST /api/migrate
func (s *Server) handleMigrate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}

	tokensFile := ".env.tokens"
	if _, err := os.Stat(tokensFile); os.IsNotExist(err) {
		writeError(w, http.StatusNotFound, ".env.tokens file not found")
		return
	}

	accounts, pairings, err := s.database.MigrateFromEnvTokens(tokensFile)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":  "Migration complete",
		"accounts": accounts,
		"pairings": pairings,
	})
}

// handleValidate checks database integrity.
// GET /api/validate
func (s *Server) handleValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}

	issues := []string{}

	accounts, _ := s.database.ListAccounts("")
	pairings, _ := s.database.ListPairings()

	clientCount := 0
	serverCount := 0
	accountMap := make(map[uint]bool)
	for _, a := range accounts {
		accountMap[a.ID] = true
		switch a.Role {
		case "CLIENT":
			clientCount++
		case "SERVER":
			serverCount++
		default:
			issues = append(issues, "Account "+string(rune(a.ID+'0'))+" has invalid role: "+a.Role)
		}
		if a.Token == "" {
			issues = append(issues, "Account has empty token")
		}
		if a.BaleUserID == 0 {
			issues = append(issues, "Account has BaleUserID=0")
		}
	}

	pairedIDs := make(map[uint]bool)
	for _, p := range pairings {
		pairedIDs[p.ClientAccountID] = true
		pairedIDs[p.ServerAccountID] = true
		if !accountMap[p.ClientAccountID] {
			issues = append(issues, "Pairing references missing client account")
		}
		if !accountMap[p.ServerAccountID] {
			issues = append(issues, "Pairing references missing server account")
		}
	}

	unpaired := 0
	for _, a := range accounts {
		if !pairedIDs[a.ID] && a.Enabled {
			unpaired++
		}
	}

	hasTokensFile := false
	if _, err := os.Stat(".env.tokens"); err == nil {
		hasTokensFile = true
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"valid":            len(issues) == 0,
		"issues":           issues,
		"total_accounts":   len(accounts),
		"client_accounts":  clientCount,
		"server_accounts":  serverCount,
		"total_pairings":   len(pairings),
		"unpaired":         unpaired,
		"has_tokens_file":  hasTokensFile,
	})
}

// handleSettings manages panel settings (key-value store in DB).
// GET /api/settings — list all
// POST /api/settings — set a key-value pair
func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		settings, err := s.database.ListSettings()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		result := make(map[string]string)
		for _, s := range settings {
			result[s.Key] = s.Value
		}
		writeJSON(w, http.StatusOK, result)

	case "POST":
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.Key == "" {
			writeError(w, http.StatusBadRequest, "key is required")
			return
		}
		if err := s.database.SetSetting(req.Key, req.Value); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeOK(w, "setting saved")

	default:
		writeError(w, http.StatusMethodNotAllowed, "GET or POST only")
	}
}
