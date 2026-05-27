package accounts

import (
	"fmt"

	"github.com/salman/ble-webrtc-tun/internal/db"
)

// accountsLog is declared in health.go

// CreatePairing links a client account to a server account, owned by ownerID.
func (m *Manager) CreatePairing(clientAccountID, serverAccountID uint, ownerID string) (*db.Pairing, error) {
	pairing, err := m.database.CreatePairing(clientAccountID, serverAccountID, ownerID)
	if err != nil {
		return nil, err
	}

	accountsLog.Info("Created pairing ID=%d: client=%d ↔ server=%d (owner=%s)",
		pairing.ID, clientAccountID, serverAccountID, ownerID)

	m.database.AppendEvent(db.EventPairingCreated, m.database.Role(), db.PairingEventPayload{
		PairingID:       pairing.ID,
		ClientAccountID: clientAccountID,
		ServerAccountID: serverAccountID,
	})

	return pairing, nil
}

// RemovePairing removes a pairing.
func (m *Manager) RemovePairing(id uint) error {
	pairing, err := m.database.GetPairing(id)
	if err != nil {
		return fmt.Errorf("pairing %d not found: %w", id, err)
	}

	if err := m.database.DeletePairing(id); err != nil {
		return err
	}

	accountsLog.Info("Removed pairing ID=%d", id)

	m.database.AppendEvent(db.EventPairingRemoved, m.database.Role(), db.PairingEventPayload{
		PairingID:       id,
		ClientAccountID: pairing.ClientAccountID,
		ServerAccountID: pairing.ServerAccountID,
	})

	return nil
}

// ListPairings returns all pairings with associated accounts.
func (m *Manager) ListPairings() ([]db.Pairing, error) {
	return m.database.ListPairings()
}

// ListPairingsByOwner returns pairings belonging to a specific owner.
func (m *Manager) ListPairingsByOwner(ownerID string) ([]db.Pairing, error) {
	return m.database.ListPairingsByOwner(ownerID)
}

// ListActivePairings returns only active pairings.
func (m *Manager) ListActivePairings() ([]db.Pairing, error) {
	return m.database.ListActivePairings()
}

// ListActivePairingsByOwner returns active pairings for a specific owner.
func (m *Manager) ListActivePairingsByOwner(ownerID string) ([]db.Pairing, error) {
	return m.database.ListActivePairingsByOwner(ownerID)
}

// AutoPairUnmatched pairs all unpaired accounts for the given owner.
func (m *Manager) AutoPairUnmatched(ownerID string) (int, error) {
	count, err := m.database.AutoPairUnmatched(ownerID)
	if err != nil {
		return 0, err
	}
	if count > 0 {
		accountsLog.Info("Auto-paired %d accounts for owner %s", count, ownerID)
	}
	return count, nil
}

// GetPairingForServer finds the pairing for a given server account.
func (m *Manager) GetPairingForServer(serverAccountID uint) (*db.Pairing, error) {
	return m.database.GetPairingByServerAccount(serverAccountID)
}

// GetPairingForClient finds the pairing for a given client account.
func (m *Manager) GetPairingForClient(clientAccountID uint) (*db.Pairing, error) {
	return m.database.GetPairingByClientAccount(clientAccountID)
}

// SetPairingActive enables or disables a pairing.
func (m *Manager) SetPairingActive(id uint, active bool) error {
	return m.database.SetPairingActive(id, active)
}

// GetUnpairedAccounts returns accounts without active pairings.
func (m *Manager) GetUnpairedAccounts(role string) ([]db.Account, error) {
	return m.database.GetUnpairedAccounts(role)
}

// GetAvailableServerAccounts returns server accounts not paired by other owners.
func (m *Manager) GetAvailableServerAccounts(ownerID string) ([]db.Account, error) {
	return m.database.GetAvailableServerAccounts(ownerID)
}
