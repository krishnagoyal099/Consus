package storage

import (
	"sync"
)

// HotEntry represents a single key-value pair in the hot (in-memory) tier.
type HotEntry struct {
	Value   []byte
	Version uint64
}

// HotStore provides ultra-fast in-memory key-value storage using sync.Map
// for lock-free concurrent reads. This is the fastest tier with sub-microsecond access.
type HotStore struct {
	data sync.Map // key(string) → *HotEntry
	size uint64   // approximate total bytes stored
	mu   sync.Mutex
}

// NewHotStore creates a new in-memory hot store.
func NewHotStore() *HotStore {
	return &HotStore{}
}

// Get retrieves a value from the hot store. Returns nil, false if not found.
func (hs *HotStore) Get(key string) ([]byte, bool) {
	entry, ok := hs.data.Load(key)
	if !ok {
		return nil, false
	}
	hotEntry := entry.(*HotEntry)
	return hotEntry.Value, true
}

// Put stores a key-value pair in the hot store.
func (hs *HotStore) Put(key string, value []byte, version uint64) {
	// Make a copy of the value to prevent external mutation
	valCopy := make([]byte, len(value))
	copy(valCopy, value)

	hs.data.Store(key, &HotEntry{
		Value:   valCopy,
		Version: version,
	})

	hs.mu.Lock()
	hs.size += uint64(len(key) + len(value))
	hs.mu.Unlock()
}

// Delete removes a key from the hot store.
func (hs *HotStore) Delete(key string) {
	if entry, ok := hs.data.LoadAndDelete(key); ok {
		hotEntry := entry.(*HotEntry)
		hs.mu.Lock()
		freed := uint64(len(key) + len(hotEntry.Value))
		if hs.size >= freed {
			hs.size -= freed
		} else {
			hs.size = 0
		}
		hs.mu.Unlock()
	}
}

// Size returns the approximate total bytes in the hot store.
func (hs *HotStore) Size() uint64 {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	return hs.size
}

// Count returns the number of keys in the hot store.
func (hs *HotStore) Count() int {
	count := 0
	hs.data.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// Keys returns all keys currently in the hot store.
func (hs *HotStore) Keys() []string {
	keys := make([]string, 0)
	hs.data.Range(func(key, _ interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})
	return keys
}
