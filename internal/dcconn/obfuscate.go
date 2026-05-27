// Package dcconn — obfuscate.go provides ChaCha20 payload obfuscation
// to prevent DPI/SFU from fingerprinting tunneled traffic within the DataChannel.
//
// The obfuscation uses XChaCha20-Poly1305 (AEAD) with a shared pre-shared key.
// Each message gets a random 24-byte nonce prepended. The overhead per message
// is 24 (nonce) + 16 (auth tag) = 40 bytes, which is negligible for tunnel frames
// that are typically 1KB–64KB.
//
// Wire format: [24-byte nonce][ciphertext + 16-byte tag]
package dcconn

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
)

// Obfuscator provides symmetric encrypt/decrypt using XChaCha20-Poly1305.
// Safe for concurrent use — the underlying AEAD is stateless.
type Obfuscator struct {
	aead   interface {
		Seal(dst, nonce, plaintext, additionalData []byte) []byte
		Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error)
		NonceSize() int
		Overhead() int
	}
	enabled bool
}

// NewObfuscator creates an obfuscator from a shared secret string.
// If secret is empty, obfuscation is disabled (passthrough mode).
func NewObfuscator(secret string) (*Obfuscator, error) {
	if secret == "" {
		return &Obfuscator{enabled: false}, nil
	}

	// Derive a 256-bit key from the shared secret using SHA-256
	key := sha256.Sum256([]byte(secret))

	aead, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		return nil, fmt.Errorf("creating XChaCha20-Poly1305: %w", err)
	}

	return &Obfuscator{aead: aead, enabled: true}, nil
}

// Encrypt obfuscates plaintext data. Returns ciphertext with prepended nonce.
// If obfuscation is disabled, returns a copy of the input unchanged.
func (o *Obfuscator) Encrypt(plaintext []byte) ([]byte, error) {
	if !o.enabled {
		// Return a copy to avoid aliasing the caller's buffer
		out := make([]byte, len(plaintext))
		copy(out, plaintext)
		return out, nil
	}

	nonce := make([]byte, o.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generating nonce: %w", err)
	}

	// Seal appends the ciphertext + tag to nonce
	ciphertext := o.aead.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt de-obfuscates ciphertext. Expects nonce prepended.
// If obfuscation is disabled, returns a copy of the input unchanged.
func (o *Obfuscator) Decrypt(ciphertext []byte) ([]byte, error) {
	if !o.enabled {
		out := make([]byte, len(ciphertext))
		copy(out, ciphertext)
		return out, nil
	}

	nonceSize := o.aead.NonceSize()
	if len(ciphertext) < nonceSize+o.aead.Overhead() {
		return nil, fmt.Errorf("ciphertext too short (%d bytes)", len(ciphertext))
	}

	nonce := ciphertext[:nonceSize]
	encrypted := ciphertext[nonceSize:]

	plaintext, err := o.aead.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return plaintext, nil
}

// Enabled returns whether obfuscation is active.
func (o *Obfuscator) Enabled() bool {
	return o.enabled
}

// Overhead returns the per-message overhead in bytes (nonce + tag).
// Returns 0 when disabled.
func (o *Obfuscator) Overhead() int {
	if !o.enabled {
		return 0
	}
	return o.aead.NonceSize() + o.aead.Overhead() // 24 + 16 = 40
}
