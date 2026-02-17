package storage

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io"
	"sync"
)

// CompressedBlock holds a zlib-compressed key-value entry for the cold tier.
type CompressedBlock struct {
	ID             uint32
	StartKey       string
	EndKey         string
	CompressedData []byte
	OriginalSize   uint64
	EntryCount     uint32
}

// ColdIndexEntry maps a key to its compressed block.
type ColdIndexEntry struct {
	BlockID uint32
}

// ColdStore provides compressed on-disk storage for infrequently accessed data.
// Keys are stored in individually compressed blocks with a sparse index for lookup.
type ColdStore struct {
	mu          sync.RWMutex
	blocks      []*CompressedBlock
	sparseIndex map[string]ColdIndexEntry // key → block location
}

// NewColdStore creates a new compressed cold storage tier.
func NewColdStore() *ColdStore {
	return &ColdStore{
		sparseIndex: make(map[string]ColdIndexEntry),
	}
}

// Put compresses and stores a key-value pair in the cold tier.
func (cs *ColdStore) Put(key string, value []byte) error {
	var buf bytes.Buffer

	// Write key length + key + value length + value
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}
	buf.Write([]byte(key))
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}
	buf.Write(value)

	// Compress with zlib
	var compressed bytes.Buffer
	w := zlib.NewWriter(&compressed)
	if _, err := w.Write(buf.Bytes()); err != nil {
		w.Close()
		return err
	}
	w.Close()

	cs.mu.Lock()
	defer cs.mu.Unlock()

	block := &CompressedBlock{
		ID:             uint32(len(cs.blocks)),
		StartKey:       key,
		EndKey:         key,
		CompressedData: compressed.Bytes(),
		OriginalSize:   uint64(buf.Len()),
		EntryCount:     1,
	}
	cs.blocks = append(cs.blocks, block)
	cs.sparseIndex[key] = ColdIndexEntry{BlockID: block.ID}

	return nil
}

// Get retrieves and decompresses a value from the cold tier.
func (cs *ColdStore) Get(key string) ([]byte, error) {
	cs.mu.RLock()
	idx, ok := cs.sparseIndex[key]
	if !ok {
		cs.mu.RUnlock()
		return nil, ErrKeyNotFound
	}

	if int(idx.BlockID) >= len(cs.blocks) {
		cs.mu.RUnlock()
		return nil, ErrKeyNotFound
	}
	block := cs.blocks[idx.BlockID]
	cs.mu.RUnlock()

	// Decompress
	r, err := zlib.NewReader(bytes.NewReader(block.CompressedData))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var decompressed bytes.Buffer
	if _, err := io.Copy(&decompressed, r); err != nil {
		return nil, err
	}
	data := decompressed.Bytes()

	// Parse: key_len(4) + key + value_len(4) + value
	if len(data) < 4 {
		return nil, ErrKeyNotFound
	}
	offset := 0
	keyLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if offset+int(keyLen) > len(data) {
		return nil, ErrKeyNotFound
	}
	storedKey := string(data[offset : offset+int(keyLen)])
	offset += int(keyLen)

	if storedKey != key {
		return nil, ErrKeyNotFound
	}

	if offset+4 > len(data) {
		return nil, ErrKeyNotFound
	}
	valueLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if offset+int(valueLen) > len(data) {
		return nil, ErrKeyNotFound
	}
	value := make([]byte, valueLen)
	copy(value, data[offset:offset+int(valueLen)])

	return value, nil
}

// Delete removes a key from the cold tier index.
func (cs *ColdStore) Delete(key string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.sparseIndex, key)
}

// Count returns the number of keys in the cold tier.
func (cs *ColdStore) Count() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.sparseIndex)
}

// Size returns the total compressed bytes stored.
func (cs *ColdStore) Size() uint64 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	var total uint64
	for _, block := range cs.blocks {
		total += uint64(len(block.CompressedData))
	}
	return total
}

// OriginalSize returns the total uncompressed bytes stored.
func (cs *ColdStore) OriginalSize() uint64 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	var total uint64
	for _, block := range cs.blocks {
		total += block.OriginalSize
	}
	return total
}

// Has checks if a key exists in the cold tier.
func (cs *ColdStore) Has(key string) bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	_, ok := cs.sparseIndex[key]
	return ok
}
