package storage

import (
	"log"
	"sync"
	"time"
)

// TieredStore orchestrates automatic data placement across Hot, Warm (Bitcask),
// and Cold (compressed) tiers based on access patterns tracked via EMA.
//
// Architecture:
//   HOT  (sync.Map, <1ms)  ←→  WARM (Bitcask, <10ms)  ←→  COLD (zlib, <100ms)
//
// New writes always enter the Hot tier. A background goroutine periodically
// demotes infrequently accessed keys to lower tiers. Reads auto-promote keys
// from lower tiers when access patterns indicate hotness.
type TieredStore struct {
	hot     *HotStore
	warm    *Bitcask    // existing Bitcask engine serves as the warm tier
	cold    *ColdStore
	tracker *AccessTracker

	// Tier thresholds (tunable)
	hotPromoteThreshold float64       // EMA > this → promote to hot
	warmDemoteThreshold float64       // EMA < this → demote from hot to warm
	coldAge             time.Duration // no access for this long → demote warm to cold
	hotCapacity         uint64        // max bytes in hot tier

	// Background management
	stopCh chan struct{}
	wg     sync.WaitGroup

	mu sync.RWMutex
}

// TieredStoreConfig holds tunable parameters for the tiered storage engine.
type TieredStoreConfig struct {
	DataDir             string
	HotPromoteThreshold float64       // default: 10.0
	WarmDemoteThreshold float64       // default: 1.0
	ColdAge             time.Duration // default: 1 hour
	HotCapacity         uint64        // default: 1GB
	DecayFactor         float64       // default: 0.95
	TierCheckInterval   time.Duration // default: 30 seconds
}

// DefaultTieredConfig returns sensible defaults for the tiered store.
func DefaultTieredConfig(dataDir string) TieredStoreConfig {
	return TieredStoreConfig{
		DataDir:             dataDir,
		HotPromoteThreshold: 10.0,
		WarmDemoteThreshold: 1.0,
		ColdAge:             1 * time.Hour,
		HotCapacity:         1024 * 1024 * 1024, // 1GB
		DecayFactor:         0.95,
		TierCheckInterval:   30 * time.Second,
	}
}

// NewTieredStore creates a new tiered storage engine wrapping the existing Bitcask.
func NewTieredStore(warmStore *Bitcask, config TieredStoreConfig) *TieredStore {
	ts := &TieredStore{
		hot:                 NewHotStore(),
		warm:                warmStore,
		cold:                NewColdStore(),
		tracker:             NewAccessTracker(config.DecayFactor),
		hotPromoteThreshold: config.HotPromoteThreshold,
		warmDemoteThreshold: config.WarmDemoteThreshold,
		coldAge:             config.ColdAge,
		hotCapacity:         config.HotCapacity,
		stopCh:              make(chan struct{}),
	}

	// Start background tier management
	ts.wg.Add(1)
	go ts.tierManagementLoop(config.TierCheckInterval)

	log.Printf("[TIERED-STORAGE] Initialized with hot capacity=%dMB, promote=%.1f, demote=%.1f",
		config.HotCapacity/(1024*1024), config.HotPromoteThreshold, config.WarmDemoteThreshold)

	return ts
}

// Get retrieves a value, searching tiers top-down and auto-promoting hot keys.
func (ts *TieredStore) Get(key string) ([]byte, error) {
	ts.tracker.RecordAccess(key)

	// Try hot tier first (fastest path, lock-free)
	if val, ok := ts.hot.Get(key); ok {
		return val, nil
	}

	// Try warm tier (Bitcask)
	val, err := ts.warm.Get(key)
	if err == nil {
		// Check if should promote to hot
		if ts.tracker.GetEMA(key) > ts.hotPromoteThreshold {
			ts.promoteToHot(key, val)
		}
		return val, nil
	}

	// Try cold tier (compressed)
	val, err = ts.cold.Get(key)
	if err == nil {
		// Always promote from cold to warm on access
		ts.promoteToWarm(key, val)
		return val, nil
	}

	return nil, ErrKeyNotFound
}

// Put writes a key-value pair. New writes always go to the hot tier (write-back).
// The Bitcask WAL provides crash safety since the warm tier persists to disk.
func (ts *TieredStore) Put(key string, value []byte) error {
	ts.tracker.RecordAccessWithSize(key, uint32(len(value)))

	// Always write to warm tier for durability (WAL-backed)
	if err := ts.warm.Put(key, value); err != nil {
		return err
	}

	// Also cache in hot tier for fast reads
	ts.hot.Put(key, value, 0)
	ts.tracker.SetTier(key, TierHot)

	return nil
}

// Delete removes a key from all tiers.
func (ts *TieredStore) Delete(key string) error {
	ts.hot.Delete(key)
	ts.cold.Delete(key)
	ts.tracker.Remove(key)
	return ts.warm.Delete(key)
}

// Close shuts down the tiered store and its background goroutines.
func (ts *TieredStore) Close() error {
	close(ts.stopCh)
	ts.wg.Wait()
	return ts.warm.Close()
}

// promoteToHot moves a value from a lower tier into the hot (in-memory) tier.
func (ts *TieredStore) promoteToHot(key string, value []byte) {
	ts.hot.Put(key, value, 0)
	ts.tracker.SetTier(key, TierHot)
}

// promoteToWarm writes cold data into the warm (Bitcask) tier.
func (ts *TieredStore) promoteToWarm(key string, value []byte) {
	if err := ts.warm.Put(key, value); err != nil {
		log.Printf("[TIERED-STORAGE] Failed to promote key '%s' to warm: %v", key, err)
		return
	}
	ts.cold.Delete(key)
	ts.tracker.SetTier(key, TierWarm)
}

// tierManagementLoop runs periodically to decay EMA and demote cold keys.
func (ts *TieredStore) tierManagementLoop(interval time.Duration) {
	defer ts.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ts.stopCh:
			return
		case <-ticker.C:
			ts.tracker.DecayAll()
			ts.demoteColdKeys()
		}
	}
}

// demoteColdKeys moves infrequently accessed keys to lower tiers.
func (ts *TieredStore) demoteColdKeys() {
	fromHot, fromWarm := ts.tracker.GetDemotionCandidates(ts.warmDemoteThreshold, ts.coldAge)

	demotedHot := 0
	for _, key := range fromHot {
		if val, ok := ts.hot.Get(key); ok {
			// Value already exists in warm (Bitcask), just remove from hot
			_ = val
			ts.hot.Delete(key)
			ts.tracker.SetTier(key, TierWarm)
			demotedHot++
		}
	}

	demotedWarm := 0
	for _, key := range fromWarm {
		val, err := ts.warm.Get(key)
		if err == nil {
			if err := ts.cold.Put(key, val); err != nil {
				log.Printf("[TIERED-STORAGE] Failed to demote key '%s' to cold: %v", key, err)
				continue
			}
			ts.tracker.SetTier(key, TierCold)
			demotedWarm++
		}
	}

	if demotedHot > 0 || demotedWarm > 0 {
		log.Printf("[TIERED-STORAGE] Demotion cycle: %d hot→warm, %d warm→cold", demotedHot, demotedWarm)
	}
}

// Stats returns current tier statistics for the dashboard.
func (ts *TieredStore) Stats() TieredStoreStats {
	tierCounts := ts.tracker.TierCounts()
	return TieredStoreStats{
		HotKeys:         tierCounts[TierHot],
		WarmKeys:        tierCounts[TierWarm],
		ColdKeys:        tierCounts[TierCold],
		HotSizeBytes:    ts.hot.Size(),
		ColdSizeBytes:   ts.cold.Size(),
		ColdOriginalBytes: ts.cold.OriginalSize(),
		TrackedKeys:     ts.tracker.Count(),
	}
}

// TieredStoreStats holds snapshot statistics about the storage tiers.
type TieredStoreStats struct {
	HotKeys           int    `json:"hotKeys"`
	WarmKeys          int    `json:"warmKeys"`
	ColdKeys          int    `json:"coldKeys"`
	HotSizeBytes      uint64 `json:"hotSizeBytes"`
	ColdSizeBytes     uint64 `json:"coldSizeBytes"`
	ColdOriginalBytes uint64 `json:"coldOriginalBytes"`
	TrackedKeys       int    `json:"trackedKeys"`
}

// Keys returns all live key names from the warm (Bitcask) tier.
func (ts *TieredStore) Keys() []string {
	return ts.warm.Keys()
}

// KeysMatch returns keys matching a pattern (delegates to Bitcask).
func (ts *TieredStore) KeysMatch(pattern string) []string {
	return ts.warm.KeysMatch(pattern)
}

// Exists checks if a key exists in any tier.
func (ts *TieredStore) Exists(key string) bool {
	if _, ok := ts.hot.Get(key); ok {
		return true
	}
	if ts.warm.Exists(key) {
		return true
	}
	return ts.cold.Has(key)
}

// KeyCount returns the total number of live keys.
func (ts *TieredStore) KeyCount() int {
	return ts.warm.KeyCount()
}

// TTL returns remaining time-to-live for a key (-1 = no TTL, -2 = not found).
func (ts *TieredStore) TTL(key string) time.Duration {
	return ts.warm.TTL(key)
}

// Expire sets a TTL on an existing key.
func (ts *TieredStore) Expire(key string, ttl time.Duration) bool {
	return ts.warm.Expire(key, ttl)
}

// PutWithTTL writes a key-value pair with an optional TTL.
func (ts *TieredStore) PutWithTTL(key string, value []byte, ttl time.Duration) error {
	ts.tracker.RecordAccessWithSize(key, uint32(len(value)))

	if err := ts.warm.PutWithTTL(key, value, ttl); err != nil {
		return err
	}

	ts.hot.Put(key, value, 0)
	ts.tracker.SetTier(key, TierHot)
	return nil
}
