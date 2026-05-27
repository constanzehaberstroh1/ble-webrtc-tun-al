package accounts

import (
	"context"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"time"

	"github.com/salman/ble-webrtc-tun/internal/bale"
	"github.com/salman/ble-webrtc-tun/internal/db"
)

var accountsLog = logger.New("accounts")

// HealthCheckConfig controls health check behavior.
type HealthCheckConfig struct {
	Interval     time.Duration // How often to run checks (default: 60s)
	Timeout      time.Duration // How long to wait for WS connect (default: 10s)
	MaxRetries   int           // Mark ERROR after N consecutive failures (default: 3)
}

// DefaultHealthConfig returns sensible defaults for health checking.
func DefaultHealthConfig() HealthCheckConfig {
	return HealthCheckConfig{
		Interval:   60 * time.Second,
		Timeout:    10 * time.Second,
		MaxRetries: 3,
	}
}

// healthState tracks consecutive failures per account.
type healthState struct {
	failures int
}

// StartHealthCheck begins a background goroutine that periodically checks
// whether each enabled account's token is still valid by briefly connecting
// to the Bale WebSocket.
//
// Accounts that fail to connect are marked OFFLINE after MaxRetries.
// Accounts that were OFFLINE/ERROR and reconnect are restored to IDLE.
func (m *Manager) StartHealthCheck(ctx context.Context, cfg HealthCheckConfig) {
	if cfg.Interval <= 0 {
		cfg = DefaultHealthConfig()
	}

	go func() {
		accountsLog.Info("Starting health checks (interval=%v)", cfg.Interval)

		states := make(map[uint]*healthState)
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				accountsLog.Info("Health checks stopped")
				return
			case <-ticker.C:
				m.runHealthChecks(ctx, cfg, states)
			}
		}
	}()
}

// runHealthChecks checks all enabled accounts.
func (m *Manager) runHealthChecks(ctx context.Context, cfg HealthCheckConfig, states map[uint]*healthState) {
	accounts, err := m.database.ListEnabledAccounts("")
	if err != nil {
		accountsLog.Warn("Failed to list accounts: %v", err)
		return
	}

	for _, acct := range accounts {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Skip accounts that have an active client (they're already connected)
		if m.GetClient(acct.ID) != nil {
			if _, ok := states[acct.ID]; ok {
				states[acct.ID].failures = 0
			}
			// Update last_seen for active clients
			m.database.TouchAccount(acct.ID)
			continue
		}

		// Initialize state tracker
		if _, ok := states[acct.ID]; !ok {
			states[acct.ID] = &healthState{}
		}
		state := states[acct.ID]

		// Try a brief WS connection
		ok := m.checkToken(acct.Token, cfg.Timeout)

		if ok {
			state.failures = 0
			m.database.TouchAccount(acct.ID)

			// Restore accounts that were offline/error
			if acct.Status == db.StatusOffline || acct.Status == db.StatusError {
				m.database.SetAccountStatus(acct.ID, db.StatusIdle)
				accountsLog.Info("✅ Account %d (BaleID=%d) back online", acct.ID, acct.BaleUserID)

				m.database.AppendEvent(db.EventStatusChanged, m.database.Role(), db.AccountEventPayload{
					AccountID:  acct.ID,
					BaleUserID: acct.BaleUserID,
					Status:     db.StatusIdle,
					Reason:     "health_check_recovered",
				})
			}
		} else {
			state.failures++
			accountsLog.Warn("⚠️ Account %d (BaleID=%d) check failed (%d/%d)",
				acct.ID, acct.BaleUserID, state.failures, cfg.MaxRetries)

			if state.failures >= cfg.MaxRetries && acct.Status != db.StatusOffline {
				m.database.SetAccountStatus(acct.ID, db.StatusOffline)
				m.database.SetAccountError(acct.ID, "health check failed")
				accountsLog.Error("❌ Account %d marked OFFLINE after %d failures",
					acct.ID, state.failures)

				m.database.AppendEvent(db.EventHealthCheckFail, m.database.Role(), db.AccountEventPayload{
					AccountID:  acct.ID,
					BaleUserID: acct.BaleUserID,
					Status:     db.StatusOffline,
					Reason:     "consecutive_health_check_failures",
				})
			}
		}
	}
}

// checkToken briefly connects to the Bale WebSocket to verify the token is valid.
func (m *Manager) checkToken(token string, timeout time.Duration) bool {
	client := bale.NewClient(token)

	// Use a goroutine with timeout to prevent blocking
	done := make(chan bool, 1)
	go func() {
		err := client.Connect()
		if err != nil {
			done <- false
			return
		}
		// Successfully connected — token is valid
		client.Close()
		done <- true
	}()

	select {
	case result := <-done:
		return result
	case <-time.After(timeout):
		client.Close()
		return false
	}
}
