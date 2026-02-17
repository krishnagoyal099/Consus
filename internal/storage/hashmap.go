package storage

import (
	"sync"
)

// LogPointer describes where a value is located on disk.
type LogPointer struct {
	FileID    uint32
	Offset    int64
	ValueSize int64
	Timestamp uint64
	ExpiresAt int64 // Unix nano; 0 = no expiry
}

// IsExpired returns true if this pointer has a TTL that has passed.
func (lp LogPointer) IsExpired(nowNano int64) bool {
	return lp.ExpiresAt > 0 && nowNano > lp.ExpiresAt
}

// HashMap wraps a thread-safe map for the in-memory index.
type HashMap struct {
	mu   sync.RWMutex
	data map[string]LogPointer
}

// NewHashMap initializes the in-memory index.
func NewHashMap() *HashMap {
	return &HashMap{
		data: make(map[string]LogPointer),
	}
}

// Get retrieves a pointer to the disk location.
func (h *HashMap) Get(key string) (LogPointer, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ptr, ok := h.data[key]
	return ptr, ok
}

// Put stores or updates the pointer for a key.
func (h *HashMap) Put(key string, ptr LogPointer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.data[key] = ptr
}

// Delete removes a key from the index.
func (h *HashMap) Delete(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.data, key)
}

// Count returns the number of keys in the index.
func (h *HashMap) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.data)
}

// Exists checks if a key exists in the index.
func (h *HashMap) Exists(key string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.data[key]
	return ok
}

// Keys returns all key names in the index.
func (h *HashMap) Keys() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	keys := make([]string, 0, len(h.data))
	for k := range h.data {
		keys = append(keys, k)
	}
	return keys
}

// Iter returns a copy of the map for iteration (used during compaction).
func (h *HashMap) Iter() map[string]LogPointer {
	h.mu.RLock()
	defer h.mu.RUnlock()
	cpy := make(map[string]LogPointer, len(h.data))
	for k, v := range h.data {
		cpy[k] = v
	}
	return cpy
}