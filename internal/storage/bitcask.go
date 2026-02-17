package storage

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrKeyNotFound = errKeyNotFound{}
	ErrKeyExpired  = errKeyExpired{}
)

type errKeyNotFound struct{}

func (errKeyNotFound) Error() string { return "key not found" }

type errKeyExpired struct{}

func (errKeyExpired) Error() string { return "key expired" }

// Bitcask represents the storage engine with crash recovery, TTL, and file rotation.
type Bitcask struct {
	mu       sync.RWMutex
	index    *HashMap
	wal      *WAL
	oldWALs  []*WAL // read-only handles to older data files
	path     string
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// Open initializes the Bitcask store. It scans existing WAL files to rebuild
// the in-memory index (crash recovery), then opens the latest file for writing.
func Open(path string) (*Bitcask, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	bc := &Bitcask{
		index:  NewHashMap(),
		path:   path,
		stopCh: make(chan struct{}),
	}

	// --- Crash Recovery ---
	fileIDs, err := FindWALFiles(path)
	if err != nil {
		// No existing files, start fresh
		fileIDs = nil
	}

	recoveredKeys := 0

	if len(fileIDs) > 0 {
		for _, fid := range fileIDs {
			wal, err := NewWAL(path, fid)
			if err != nil {
				continue
			}

			entries, err := wal.ScanEntries()
			if err != nil {
				wal.Close()
				continue
			}

			for _, e := range entries {
				if len(e.Entry.Value) == 0 {
					// Tombstone — key was deleted
					bc.index.Delete(e.Entry.Key)
				} else {
					bc.index.Put(e.Entry.Key, LogPointer{
						FileID:    fid,
						Offset:    e.Offset,
						ValueSize: e.Size,
						Timestamp: e.Entry.Timestamp,
					})
					recoveredKeys++
				}
			}

			bc.oldWALs = append(bc.oldWALs, wal)
		}

		// Use latest file ID + 1 for new writes (or reuse latest if it's small)
		latestID := fileIDs[len(fileIDs)-1]
		latestWAL := bc.oldWALs[len(bc.oldWALs)-1]

		if latestWAL.Size() < MaxFileSize {
			// Reuse the latest file for new writes
			bc.wal = latestWAL
			bc.oldWALs = bc.oldWALs[:len(bc.oldWALs)-1]
		} else {
			// Rotate to a new file
			newWAL, err := NewWAL(path, latestID+1)
			if err != nil {
				return nil, err
			}
			bc.wal = newWAL
		}

		if recoveredKeys > 0 {
			log.Printf("[BITCASK] Crash recovery: restored %d keys from %d WAL files", recoveredKeys, len(fileIDs))
		}
	} else {
		// Fresh start
		wal, err := NewWAL(path, 1)
		if err != nil {
			return nil, err
		}
		bc.wal = wal
	}

	// Start background TTL reaper
	bc.wg.Add(1)
	go bc.ttlReaper()

	return bc, nil
}

// Put writes a key-value pair with no expiry.
func (bc *Bitcask) Put(key string, value []byte) error {
	return bc.PutWithTTL(key, value, 0)
}

// PutWithTTL writes a key-value pair with an optional TTL.
// If ttl is 0, the key never expires.
func (bc *Bitcask) PutWithTTL(key string, value []byte, ttl time.Duration) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// File rotation check
	if bc.wal.Size() >= MaxFileSize {
		if err := bc.rotateFile(); err != nil {
			log.Printf("[BITCASK] File rotation failed: %v", err)
		}
	}

	ts := uint64(time.Now().UnixNano())
	offset, size, err := bc.wal.Write(key, value, ts)
	if err != nil {
		return err
	}

	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}

	bc.index.Put(key, LogPointer{
		FileID:    bc.wal.FileID(),
		Offset:    offset,
		ValueSize: size,
		Timestamp: ts,
		ExpiresAt: expiresAt,
	})

	return nil
}

// Get retrieves a value. Returns ErrKeyNotFound if missing, ErrKeyExpired if TTL passed.
func (bc *Bitcask) Get(key string) ([]byte, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	ptr, ok := bc.index.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Lazy TTL expiry
	if ptr.IsExpired(time.Now().UnixNano()) {
		// Don't delete under RLock — just report not found
		return nil, ErrKeyNotFound
	}

	// Find the right WAL handle for this file ID
	wal := bc.walForFileID(ptr.FileID)
	if wal == nil {
		return nil, ErrKeyNotFound
	}

	entry, err := wal.ReadAt(ptr.Offset)
	if err != nil {
		return nil, err
	}

	return entry.Value, nil
}

// Delete removes a key by writing a tombstone entry.
func (bc *Bitcask) Delete(key string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	ts := uint64(time.Now().UnixNano())
	_, _, err := bc.wal.Write(key, []byte{}, ts)
	if err != nil {
		return err
	}

	bc.index.Delete(key)
	return nil
}

// Exists checks if a key exists and is not expired.
func (bc *Bitcask) Exists(key string) bool {
	ptr, ok := bc.index.Get(key)
	if !ok {
		return false
	}
	return !ptr.IsExpired(time.Now().UnixNano())
}

// Keys returns all non-expired key names.
func (bc *Bitcask) Keys() []string {
	allKeys := bc.index.Keys()
	now := time.Now().UnixNano()
	live := make([]string, 0, len(allKeys))
	for _, k := range allKeys {
		ptr, ok := bc.index.Get(k)
		if ok && !ptr.IsExpired(now) {
			live = append(live, k)
		}
	}
	sort.Strings(live)
	return live
}

// KeysMatch returns keys matching a simple pattern (* = all, prefix* = prefix match).
func (bc *Bitcask) KeysMatch(pattern string) []string {
	if pattern == "*" || pattern == "" {
		return bc.Keys()
	}

	allKeys := bc.Keys()
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		matched := make([]string, 0)
		for _, k := range allKeys {
			if strings.HasPrefix(k, prefix) {
				matched = append(matched, k)
			}
		}
		return matched
	}

	// Exact match
	for _, k := range allKeys {
		if k == pattern {
			return []string{k}
		}
	}
	return nil
}

// KeyCount returns the number of live (non-expired) keys.
func (bc *Bitcask) KeyCount() int {
	return len(bc.Keys())
}

// TTL returns remaining time-to-live for a key. Returns -1 if no TTL, -2 if not found.
func (bc *Bitcask) TTL(key string) time.Duration {
	ptr, ok := bc.index.Get(key)
	if !ok {
		return -2
	}
	if ptr.ExpiresAt == 0 {
		return -1 // no expiry
	}
	remaining := time.Duration(ptr.ExpiresAt - time.Now().UnixNano())
	if remaining <= 0 {
		return -2 // expired
	}
	return remaining
}

// Expire sets a TTL on an existing key. Returns false if key not found.
func (bc *Bitcask) Expire(key string, ttl time.Duration) bool {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	ptr, ok := bc.index.Get(key)
	if !ok {
		return false
	}
	ptr.ExpiresAt = time.Now().Add(ttl).UnixNano()
	bc.index.Put(key, ptr)
	return true
}

// Merge performs compaction — rewrites live data to a new file, removes tombstones and expired keys.
func (bc *Bitcask) Merge() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	newFileID := bc.wal.FileID() + 1
	newWAL, err := NewWAL(bc.path, newFileID)
	if err != nil {
		return err
	}

	now := time.Now().UnixNano()
	liveData := bc.index.Iter()
	merged := 0

	for key, ptr := range liveData {
		// Skip expired keys
		if ptr.IsExpired(now) {
			bc.index.Delete(key)
			continue
		}

		wal := bc.walForFileID(ptr.FileID)
		if wal == nil {
			continue
		}

		entry, err := wal.ReadAt(ptr.Offset)
		if err != nil {
			continue
		}

		newOffset, newSize, err := newWAL.Write(key, entry.Value, entry.Timestamp)
		if err != nil {
			return err
		}

		bc.index.Put(key, LogPointer{
			FileID:    newFileID,
			Offset:    newOffset,
			ValueSize: newSize,
			Timestamp: entry.Timestamp,
			ExpiresAt: ptr.ExpiresAt,
		})
		merged++
	}

	// Close and remove old files
	oldFile := bc.wal.file.Name()
	bc.wal.Close()
	for _, old := range bc.oldWALs {
		name := old.file.Name()
		old.Close()
		go os.Remove(name)
	}
	go os.Remove(oldFile)

	bc.wal = newWAL
	bc.oldWALs = nil

	log.Printf("[BITCASK] Merge complete: %d live keys compacted", merged)
	return nil
}

// Close shuts down the storage engine.
func (bc *Bitcask) Close() error {
	close(bc.stopCh)
	bc.wg.Wait()

	bc.mu.Lock()
	defer bc.mu.Unlock()

	for _, old := range bc.oldWALs {
		old.Close()
	}
	return bc.wal.Close()
}

// walForFileID finds the WAL handle for a given file ID.
func (bc *Bitcask) walForFileID(fileID uint32) *WAL {
	if bc.wal.FileID() == fileID {
		return bc.wal
	}
	for _, old := range bc.oldWALs {
		if old.FileID() == fileID {
			return old
		}
	}
	return nil
}

// rotateFile creates a new WAL file and moves the current one to oldWALs.
func (bc *Bitcask) rotateFile() error {
	newFileID := bc.wal.FileID() + 1
	newWAL, err := NewWAL(bc.path, newFileID)
	if err != nil {
		return err
	}

	bc.oldWALs = append(bc.oldWALs, bc.wal)
	bc.wal = newWAL
	log.Printf("[BITCASK] Rotated to new file %d", newFileID)
	return nil
}

// ttlReaper periodically removes expired keys from the index.
func (bc *Bitcask) ttlReaper() {
	defer bc.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-bc.stopCh:
			return
		case <-ticker.C:
			now := time.Now().UnixNano()
			allData := bc.index.Iter()
			expired := 0
			for key, ptr := range allData {
				if ptr.IsExpired(now) {
					bc.index.Delete(key)
					expired++
				}
			}
			if expired > 0 {
				log.Printf("[BITCASK] TTL reaper: removed %d expired keys", expired)
			}
		}
	}
}

// DataDir returns the path to the data directory.
func (bc *Bitcask) DataDir() string {
	return bc.path
}

// ListDataFiles returns a list of data file paths.
func (bc *Bitcask) ListDataFiles() []string {
	pattern := filepath.Join(bc.path, "*.data")
	matches, _ := filepath.Glob(pattern)
	return matches
}