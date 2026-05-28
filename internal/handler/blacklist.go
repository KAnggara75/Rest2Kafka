package handler

import (
	"sync"
	"time"
)

// TokenBlacklist handles in-memory storage and cleanup of blacklisted JWT tokens.
type TokenBlacklist struct {
	mu     sync.RWMutex
	tokens map[string]time.Time
}

// NewTokenBlacklist initializes and returns a new TokenBlacklist.
func NewTokenBlacklist() *TokenBlacklist {
	tb := &TokenBlacklist{
		tokens: make(map[string]time.Time),
	}
	// Start background cleanup routine
	go tb.startCleanup(1 * time.Minute)
	return tb
}

// Add adds a token to the blacklist with its expiration time.
func (tb *TokenBlacklist) Add(token string, expiresAt time.Time) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.tokens[token] = expiresAt
}

// IsBlacklisted checks if a token has been blacklisted and is not yet expired.
func (tb *TokenBlacklist) IsBlacklisted(token string) bool {
	tb.mu.RLock()
	defer tb.mu.RUnlock()
	expiresAt, exists := tb.tokens[token]
	if !exists {
		return false
	}
	if time.Now().After(expiresAt) {
		return false
	}
	return true
}

func (tb *TokenBlacklist) startCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		tb.cleanup()
	}
}

func (tb *TokenBlacklist) cleanup() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	now := time.Now()
	for token, expiresAt := range tb.tokens {
		if now.After(expiresAt) {
			delete(tb.tokens, token)
		}
	}
}
