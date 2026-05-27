package db

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"gorm.io/gorm"
)

// hashPassword creates a SHA256 hash of the password.
// For a production system you'd use bcrypt, but we avoid CGO deps.
func hashPassword(password string) string {
	h := sha256.Sum256([]byte(password))
	return hex.EncodeToString(h[:])
}

// SeedAdminUser creates an admin user if they don't already exist.
// Safe to call multiple times — will not duplicate or overwrite.
func (d *Database) SeedAdminUser(username, password string) error {
	var existing AdminUser
	if err := d.DB.Where("username = ?", username).First(&existing).Error; err == nil {
		return nil // already exists
	}

	user := AdminUser{
		Username:     username,
		PasswordHash: hashPassword(password),
	}
	result := d.DB.Create(&user)
	if result.Error != nil {
		return result.Error
	}
	dbLog.Info("✅ Admin user '%s' created", username)
	return nil
}

// SeedAdminUsers creates multiple admin users if they don't already exist.
func (d *Database) SeedAdminUsers(users map[string]string) error {
	for username, password := range users {
		if err := d.SeedAdminUser(username, password); err != nil {
			return err
		}
	}
	return nil
}

// AuthenticateAdmin checks the provided username/password against the database.
// Returns the AdminUser if valid, error otherwise.
func (d *Database) AuthenticateAdmin(username, password string) (*AdminUser, error) {
	var user AdminUser
	result := d.DB.Where("username = ?", username).First(&user)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, errors.New("invalid credentials")
		}
		return nil, result.Error
	}

	if user.PasswordHash != hashPassword(password) {
		return nil, errors.New("invalid credentials")
	}
	return &user, nil
}

// UpdateAdminPassword changes the password for the given admin user.
func (d *Database) UpdateAdminPassword(username, newPassword string) error {
	return d.DB.Model(&AdminUser{}).Where("username = ?", username).
		Update("password_hash", hashPassword(newPassword)).Error
}

// ListAdminUsers returns all admin users.
func (d *Database) ListAdminUsers() ([]AdminUser, error) {
	var users []AdminUser
	result := d.DB.Find(&users)
	return users, result.Error
}
