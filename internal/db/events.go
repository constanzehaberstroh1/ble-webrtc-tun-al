package db

import (
	"encoding/json"
	"fmt"
	"time"
)

// AppendEvent writes an event to the append-only event log.
// Returns the event with its auto-incremented ID (sequence number).
func (d *Database) AppendEvent(eventType, source string, payload interface{}) (*Event, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshaling event payload: %w", err)
	}

	event := &Event{
		Type:      eventType,
		Source:    source,
		Payload:   string(payloadJSON),
		Timestamp: time.Now(),
	}

	if err := d.DB.Create(event).Error; err != nil {
		return nil, fmt.Errorf("appending event: %w", err)
	}
	return event, nil
}

// GetEventsSince returns all events with ID greater than sinceID, ordered by ID.
// Used by the sync protocol: "give me everything since my last known sequence."
func (d *Database) GetEventsSince(sinceID uint, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 1000
	}
	var events []Event
	if err := d.DB.Where("id > ?", sinceID).
		Order("id ASC").
		Limit(limit).
		Find(&events).Error; err != nil {
		return nil, err
	}
	return events, nil
}

// GetLatestEventID returns the highest event ID (last sequence number).
func (d *Database) GetLatestEventID() (uint, error) {
	var event Event
	if err := d.DB.Order("id DESC").First(&event).Error; err != nil {
		return 0, nil // no events yet
	}
	return event.ID, nil
}

// GetEventsByType returns events of a specific type, optionally since a given ID.
func (d *Database) GetEventsByType(eventType string, sinceID uint, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 100
	}
	var events []Event
	query := d.DB.Where("type = ?", eventType)
	if sinceID > 0 {
		query = query.Where("id > ?", sinceID)
	}
	if err := query.Order("id ASC").Limit(limit).Find(&events).Error; err != nil {
		return nil, err
	}
	return events, nil
}

// ApplyEvent applies a remote event to the local database.
// This is called by the sync worker when receiving events from the other side.
func (d *Database) ApplyEvent(event Event) error {
	// Store the event in our log
	localEvent := &Event{
		Type:      event.Type,
		Source:    event.Source,
		Payload:   event.Payload,
		Timestamp: event.Timestamp,
	}
	return d.DB.Create(localEvent).Error
}

// PruneEvents removes events older than the given duration.
// Keeps the event log from growing indefinitely.
func (d *Database) PruneEvents(olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)
	result := d.DB.Where("timestamp < ?", cutoff).Delete(&Event{})
	return result.RowsAffected, result.Error
}

// ---- Event Payload Types ----

// AccountEventPayload is the JSON payload for account-related events.
type AccountEventPayload struct {
	AccountID  uint   `json:"account_id"`
	BaleUserID int64  `json:"bale_user_id"`
	Role       string `json:"role"`
	Status     string `json:"status,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// PairingEventPayload is the JSON payload for pairing-related events.
type PairingEventPayload struct {
	PairingID       uint `json:"pairing_id"`
	ClientAccountID uint `json:"client_account_id"`
	ServerAccountID uint `json:"server_account_id"`
}

// CallEventPayload is the JSON payload for call-related events.
type CallEventPayload struct {
	ConnectionLogID uint  `json:"connection_log_id"`
	ClientAcctID    uint  `json:"client_account_id"`
	ServerAcctID    uint  `json:"server_account_id"`
	CallID          int64 `json:"call_id"`
	RoomID          string `json:"room_id,omitempty"`
}
