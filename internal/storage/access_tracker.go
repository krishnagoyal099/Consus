package storage

import (
	"sync"
	"time"
)

// StorageTier represents which tier a key currently resides in.
type StorageTier int

const (
	TierHot  StorageTier = iota // In-memory sync.Map
	TierWarm                    // Bitcask append-only log + hash index
	TierCold                    // Compressed on-disk blocks
)

// KeyAccessStats tracks per-key access patterns using Exponential Moving Average.
type KeyAccessStats struct {
	EMA           float64     // Exponential Moving Average of access frequency
	LastAccess    time.Time   // Last time this key was accessed
	TotalAccesses uint64      // Lifetime access count
	CurrentTier   StorageTier // Which tier currently holds this key
	SizeBytes     uint32      // Approximate size of the value
}

// AccessTracker monitors key access patterns for automatic tier classification.
type AccessTracker struct {
	mu          sync.RWMutex
	stats       map[string]*KeyAccessStats
	decayFactor float64 // 0.95 = 5% decay per interval
}

// NewAccessTracker creates a new access tracker with the given decay factor.
func NewAccessTracker(decayFactor float64) *AccessTracker {
	if decayFactor <= 0 || decayFactor >= 1 {
		decayFactor = 0.95
	}
	return &AccessTracker{
		stats:       make(map[string]*KeyAccessStats),
		decayFactor: decayFactor,
	}
}

// RecordAccess increments the EMA for a key and updates access metadata.
func (at *AccessTracker) RecordAccess(key string) {
	at.mu.Lock()
	defer at.mu.Unlock()

	stats, ok := at.stats[key]
	if !ok {
		stats = &KeyAccessStats{CurrentTier: TierHot}
		at.stats[key] = stats
	}
	stats.EMA += 1.0
	stats.LastAccess = time.Now()
	stats.TotalAccesses++
}

// RecordAccessWithSize records an access and updates the known size.
func (at *AccessTracker) RecordAccessWithSize(key string, size uint32) {
	at.mu.Lock()
	defer at.mu.Unlock()

	stats, ok := at.stats[key]
	if !ok {
		stats = &KeyAccessStats{CurrentTier: TierHot}
		at.stats[key] = stats
	}
	stats.EMA += 1.0
	stats.LastAccess = time.Now()
	stats.TotalAccesses++
	stats.SizeBytes = size
}

// GetEMA returns the current EMA for a key. Returns 0 if not tracked.
func (at *AccessTracker) GetEMA(key string) float64 {
	at.mu.RLock()
	defer at.mu.RUnlock()
	if stats, ok := at.stats[key]; ok {
		return stats.EMA
	}
	return 0
}

// GetStats returns a copy of the stats for a key, or nil if not tracked.
func (at *AccessTracker) GetStats(key string) *KeyAccessStats {
	at.mu.RLock()
	defer at.mu.RUnlock()
	if stats, ok := at.stats[key]; ok {
		cp := *stats
		return &cp
	}
	return nil
}

// SetTier updates the current tier for a key.
func (at *AccessTracker) SetTier(key string, tier StorageTier) {
	at.mu.Lock()
	defer at.mu.Unlock()
	if stats, ok := at.stats[key]; ok {
		stats.CurrentTier = tier
	}
}

// DecayAll applies exponential decay to all tracked keys.
// Should be called periodically (e.g., every 30 seconds).
func (at *AccessTracker) DecayAll() {
	at.mu.Lock()
	defer at.mu.Unlock()

	for _, stats := range at.stats {
		stats.EMA *= at.decayFactor
	}
}

// GetDemotionCandidates returns keys that should be demoted from hot or warm tiers.
func (at *AccessTracker) GetDemotionCandidates(warmThreshold float64, coldAge time.Duration) (fromHot []string, fromWarm []string) {
	at.mu.RLock()
	defer at.mu.RUnlock()

	now := time.Now()
	for key, stats := range at.stats {
		switch stats.CurrentTier {
		case TierHot:
			if stats.EMA < warmThreshold {
				fromHot = append(fromHot, key)
			}
		case TierWarm:
			if now.Sub(stats.LastAccess) > coldAge {
				fromWarm = append(fromWarm, key)
			}
		}
	}
	return
}

// Count returns the total number of tracked keys.
func (at *AccessTracker) Count() int {
	at.mu.RLock()
	defer at.mu.RUnlock()
	return len(at.stats)
}

// TierCounts returns the count of keys per tier.
func (at *AccessTracker) TierCounts() map[StorageTier]int {
	at.mu.RLock()
	defer at.mu.RUnlock()
	counts := map[StorageTier]int{
		TierHot:  0,
		TierWarm: 0,
		TierCold: 0,
	}
	for _, stats := range at.stats {
		counts[stats.CurrentTier]++
	}
	return counts
}

// Remove stops tracking a key entirely.
func (at *AccessTracker) Remove(key string) {
	at.mu.Lock()
	defer at.mu.Unlock()
	delete(at.stats, key)
}
