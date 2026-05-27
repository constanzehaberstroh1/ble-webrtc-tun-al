package db

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"os"
	"strconv"
	"strings"
)

var migrateLog = logger.New("db")

// MigrateFromEnvTokens reads .env.tokens and seeds the database with accounts
// and pairings. This provides backward compatibility during the transition from
// static token files to database-driven management.
//
// Returns the number of accounts and pairings created.
func (d *Database) MigrateFromEnvTokens(path string) (accounts int, pairings int, err error) {
	envMap := loadEnvFileForMigration(path)
	if len(envMap) == 0 {
		return 0, 0, fmt.Errorf("no tokens found in %s", path)
	}

	migrateLog.Info(" Found %d entries in %s", len(envMap), path)

	// Helper to get value from file
	getVal := func(key string) string {
		if v := envMap[key]; v != "" {
			return v
		}
		return os.Getenv(key)
	}

	for i := 1; i <= 8; i++ {
		suffix := strconv.Itoa(i)

		clientToken := getVal("BALE_TOKEN_CLIENT_" + suffix)
		serverToken := getVal("BALE_TOKEN_SERVER_" + suffix)
		targetServer := getVal("BALE_TARGET_SERVER_" + suffix)

		if clientToken == "" && serverToken == "" {
			continue
		}

		var clientAcct, serverAcct *Account

		// Create client account
		if clientToken != "" {
			clientUserID := extractUserIDFromJWT(clientToken)
			if clientUserID == 0 {
				migrateLog.Info(" Pair %d: could not extract client user ID from JWT", i)
				continue
			}

			// Check if already exists
			existing, _ := d.GetAccountByBaleUserID(clientUserID)
			if existing != nil {
				migrateLog.Info(" Pair %d: client account %d already exists (ID=%d)", i, clientUserID, existing.ID)
				clientAcct = existing
			} else {
				acct, err := d.CreateAccount(clientToken, RoleClient, clientUserID)
				if err != nil {
					migrateLog.Info(" Pair %d: failed to create client account: %v", i, err)
					continue
				}
				clientAcct = acct
				accounts++
				migrateLog.Info(" Pair %d: ✅ Created client account %d (ID=%d)", i, clientUserID, acct.ID)
			}
		}

		// Create server account
		if serverToken != "" {
			serverUserID := extractUserIDFromJWT(serverToken)
			if serverUserID == 0 {
				// Try the target server ID as fallback
				if targetServer != "" {
					serverUserID, _ = strconv.ParseInt(targetServer, 10, 64)
				}
			}
			if serverUserID == 0 {
				migrateLog.Info(" Pair %d: could not determine server user ID", i)
				continue
			}

			existing, _ := d.GetAccountByBaleUserID(serverUserID)
			if existing != nil {
				migrateLog.Info(" Pair %d: server account %d already exists (ID=%d)", i, serverUserID, existing.ID)
				serverAcct = existing
			} else {
				acct, err := d.CreateAccount(serverToken, RoleServer, serverUserID)
				if err != nil {
					migrateLog.Info(" Pair %d: failed to create server account: %v", i, err)
					continue
				}
				serverAcct = acct
				accounts++
				migrateLog.Info(" Pair %d: ✅ Created server account %d (ID=%d)", i, serverUserID, acct.ID)
			}
		}

		// Create pairing if both accounts exist
		if clientAcct != nil && serverAcct != nil {
			// Check if pairing already exists
			existingPairing, _ := d.GetPairingByServerAccount(serverAcct.ID)
			if existingPairing != nil {
				migrateLog.Info(" Pair %d: pairing already exists (ID=%d)", i, existingPairing.ID)
				continue
			}

			pairing, err := d.CreatePairing(clientAcct.ID, serverAcct.ID, "")
			if err != nil {
				migrateLog.Info(" Pair %d: failed to create pairing: %v", i, err)
				continue
			}
			pairings++
			migrateLog.Info(" Pair %d: ✅ Created pairing (ID=%d) client=%d ↔ server=%d",
				i, pairing.ID, clientAcct.BaleUserID, serverAcct.BaleUserID)
		}
	}

	return accounts, pairings, nil
}

// extractUserIDFromJWT parses a Bale JWT token and extracts the user_id.
// Duplicated from config package to avoid circular imports.
func extractUserIDFromJWT(token string) int64 {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return 0
	}
	payload := parts[1]
	switch len(payload) % 4 {
	case 2:
		payload += "=="
	case 3:
		payload += "="
	}
	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return 0
	}
	var claims struct {
		Payload struct {
			UserID int64 `json:"user_id"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return 0
	}
	return claims.Payload.UserID
}

// loadEnvFileForMigration reads a KEY=VALUE file.
func loadEnvFileForMigration(path string) map[string]string {
	m := make(map[string]string)
	f, err := os.Open(path)
	if err != nil {
		return m
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			val = strings.Trim(val, "\"'")
			m[key] = val
		}
	}
	return m
}
