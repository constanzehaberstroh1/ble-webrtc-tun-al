package db

import (
	"time"
)

// CreateConnectionLog records a new WebRTC session.
func (d *Database) CreateConnectionLog(clientAcctID, serverAcctID, pairingID uint, callID int64, roomID string) (*ConnectionLog, error) {
	log := &ConnectionLog{
		ClientAcctID: clientAcctID,
		ServerAcctID: serverAcctID,
		PairingID:    pairingID,
		CallID:       callID,
		RoomID:       roomID,
		StartTime:    time.Now(),
	}
	if err := d.DB.Create(log).Error; err != nil {
		return nil, err
	}
	return log, nil
}

// EndConnectionLog marks a session as ended with final stats.
func (d *Database) EndConnectionLog(id uint, bytesSent, bytesRecv int64, termination, errorDetail string) error {
	now := time.Now()
	return d.DB.Model(&ConnectionLog{}).Where("id = ?", id).Updates(map[string]interface{}{
		"end_time":       &now,
		"bytes_sent":     bytesSent,
		"bytes_received": bytesRecv,
		"termination":    termination,
		"error_detail":   errorDetail,
	}).Error
}

// UpdateConnectionStats updates the byte counters for an active session.
func (d *Database) UpdateConnectionStats(id uint, bytesSent, bytesRecv int64) error {
	return d.DB.Model(&ConnectionLog{}).Where("id = ?", id).Updates(map[string]interface{}{
		"bytes_sent":     bytesSent,
		"bytes_received": bytesRecv,
	}).Error
}

// GetActiveConnections returns sessions that haven't ended yet.
func (d *Database) GetActiveConnections() ([]ConnectionLog, error) {
	var logs []ConnectionLog
	if err := d.DB.Preload("ClientAccount").Preload("ServerAccount").
		Where("end_time IS NULL").
		Order("start_time DESC").
		Find(&logs).Error; err != nil {
		return nil, err
	}
	return logs, nil
}

// GetConnectionHistory returns recent ended sessions.
func (d *Database) GetConnectionHistory(limit int) ([]ConnectionLog, error) {
	if limit <= 0 {
		limit = 50
	}
	var logs []ConnectionLog
	if err := d.DB.Preload("ClientAccount").Preload("ServerAccount").
		Where("end_time IS NOT NULL").
		Order("start_time DESC").
		Limit(limit).
		Find(&logs).Error; err != nil {
		return nil, err
	}
	return logs, nil
}

// GetConnectionStats returns aggregate stats for the dashboard.
func (d *Database) GetConnectionStats() (*ConnectionStats, error) {
	stats := &ConnectionStats{}

	// Total sessions
	d.DB.Model(&ConnectionLog{}).Count(&stats.TotalSessions)

	// Active sessions
	d.DB.Model(&ConnectionLog{}).Where("end_time IS NULL").Count(&stats.ActiveSessions)

	// Total bytes
	var totalSent, totalRecv struct{ Total int64 }
	d.DB.Model(&ConnectionLog{}).Select("COALESCE(SUM(bytes_sent), 0) as total").Scan(&totalSent)
	d.DB.Model(&ConnectionLog{}).Select("COALESCE(SUM(bytes_received), 0) as total").Scan(&totalRecv)
	stats.TotalBytesSent = totalSent.Total
	stats.TotalBytesReceived = totalRecv.Total

	// Average session duration (for ended sessions)
	var avgDuration struct{ Avg float64 }
	d.DB.Model(&ConnectionLog{}).
		Where("end_time IS NOT NULL").
		Select("COALESCE(AVG(JULIANDAY(end_time) - JULIANDAY(start_time)) * 86400, 0) as avg").
		Scan(&avgDuration)
	stats.AvgSessionDurationSec = avgDuration.Avg

	// Sessions today
	today := time.Now().Truncate(24 * time.Hour)
	d.DB.Model(&ConnectionLog{}).Where("start_time >= ?", today).Count(&stats.SessionsToday)

	return stats, nil
}

// ConnectionStats holds aggregate connection statistics.
type ConnectionStats struct {
	TotalSessions         int64   `json:"total_sessions"`
	ActiveSessions        int64   `json:"active_sessions"`
	SessionsToday         int64   `json:"sessions_today"`
	TotalBytesSent        int64   `json:"total_bytes_sent"`
	TotalBytesReceived    int64   `json:"total_bytes_received"`
	AvgSessionDurationSec float64 `json:"avg_session_duration_sec"`
}

// GetSetting retrieves a setting value by key.
func (d *Database) GetSetting(key string) (string, error) {
	var setting Setting
	if err := d.DB.First(&setting, "key = ?", key).Error; err != nil {
		return "", err
	}
	return setting.Value, nil
}

// SetSetting creates or updates a setting.
func (d *Database) SetSetting(key, value string) error {
	setting := Setting{Key: key, Value: value, UpdatedAt: time.Now()}
	return d.DB.Save(&setting).Error
}

// ListSettings returns all settings.
func (d *Database) ListSettings() ([]Setting, error) {
	var settings []Setting
	if err := d.DB.Find(&settings).Error; err != nil {
		return nil, err
	}
	return settings, nil
}
