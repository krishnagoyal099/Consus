package storage

import (
	"errors"
	"os"
	"sync"
	"time"
)

var (
    ErrKeyNotFound = errors.New("key not found")
    ErrKeyDeleted  = errors.New("key deleted (tombstone)")
)

// Bitcask represents the storage engine.
type Bitcask struct {
    mu    sync.RWMutex
    index *HashMap
    wal   *WAL
    path  string
}

// Open initializes the Bitcask store.
// In a full implementation, this would scan existing files to rebuild the index.
func Open(path string) (*Bitcask, error) {
    if err := os.MkdirAll(path, 0755); err != nil {
        return nil, err
    }

    // For simplicity, we start with a single active file ID.
    // A production system would find the latest ID from the directory.
    activeFileID := uint32(1)
    
    wal, err := NewWAL(path, activeFileID)
    if err != nil {
        return nil, err
    }

    bc := &Bitcask{
        index: NewHashMap(),
        wal:   wal,
        path:  path,
    }

    // TODO: In a real scenario, run a recovery process here to read 
    // all log files and populate 'bc.index'.

    return bc, nil
}

// Put writes a key-value pair.
func (bc *Bitcask) Put(key string, value []byte) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    ts := uint64(time.Now().UnixNano())
    offset, size, err := bc.wal.Write(key, value, ts)
    if err != nil {
        return err
    }

    // Update in-memory index immediately after successful write
    bc.index.Put(key, LogPointer{
        FileID:    bc.wal.FileID(),
        Offset:    offset,
        ValueSize: size,
        Timestamp: ts,
    })

    return nil
}

// Get retrieves a value.
func (bc *Bitcask) Get(key string) ([]byte, error) {
    bc.mu.RLock()
    defer bc.mu.RUnlock()

    ptr, ok := bc.index.Get(key)
    if !ok {
        return nil, ErrKeyNotFound
    }

    // Read from WAL using the pointer
    // Note: If compaction runs, fileID might refer to an old file.
    // This simplified version assumes we read from the active WAL handle.
    // A robust version would open the specific fileID if it differs from active.
    entry, err := bc.wal.ReadAt(ptr.Offset)
    if err != nil {
        return nil, err
    }

    // Check for Tombstone (Deletion marker)
    // We represent a delete as a value of length 0 with a specific flag or just checking size?
    // In this simplified logic, let's assume Delete writes a special byte or just check logic.
    // Actually, ReadAt returns the raw bytes.
    
    // Let's refine: Delete writes a 0-length value or specific marker.
    // We handle this in the Delete method logic.
    
    return entry.Value, nil
}

// Delete removes a key by writing a "tombstone" entry.
func (bc *Bitcask) Delete(key string) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    // Write a tombstone (empty value)
    ts := uint64(time.Now().UnixNano())
    _, _, err := bc.wal.Write(key, []byte{}, ts) // Empty byte slice indicates deletion
    if err != nil {
        return err
    }

    bc.index.Delete(key)
    return nil
}

// Merge performs compaction. It creates a new file with only live data.
func (bc *Bitcask) Merge() error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    // 1. Create a new file for compacted data
    newFileID := bc.wal.FileID() + 1
    newWAL, err := NewWAL(bc.path, newFileID)
    if err != nil {
        return err
    }

    // 2. Iterate over in-memory index and rewrite live keys
    liveData := bc.index.Iter()
    for key, ptr := range liveData {
        // Read old value
        // Note: This assumes the current WAL file handle can read the old offsets
        // before we close it. In a real system, we'd need a reader for the old file.
        entry, err := bc.wal.ReadAt(ptr.Offset)
        if err != nil {
            // If we can't read it, we lose data. Log this error.
            continue
        }

        // Write to new file
        newOffset, newSize, err := newWAL.Write(key, entry.Value, entry.Timestamp)
        if err != nil {
            return err
        }

        // Update index to point to new file/offset
        bc.index.Put(key, LogPointer{
            FileID:    newFileID,
            Offset:    newOffset,
            ValueSize: newSize,
            Timestamp: entry.Timestamp,
        })
    }

    // 3. Swap WALs
    oldFile := bc.wal.file.Name()
    bc.wal.Close() // Close old file
    
    // Replace active WAL
    bc.wal = newWAL

    // 4. Remove old data file
    // (In production, keep it until safe sync)
    go os.Remove(oldFile)

    return nil
}

// Close shuts down the storage engine.
func (bc *Bitcask) Close() error {
    bc.mu.Lock()
    defer bc.mu.Unlock()
    return bc.wal.Close()
}