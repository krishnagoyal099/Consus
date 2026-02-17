package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	HeaderSize  = 4 + 8 + 4 + 4 // CRC(4) + Timestamp(8) + KeySize(4) + ValueSize(4)
	MaxFileSize = 64 * 1024 * 1024 // 64MB file rotation threshold
)

// Entry represents a record stored in the log.
type Entry struct {
	Timestamp uint64
	Key       string
	Value     []byte
}

// WAL (Write-Ahead Log) manages the file handle for the active data file.
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	fileID   uint32
	filePath string
	offset   int64 // tracked internally for correct offset reporting
}

// NewWAL creates or opens a WAL file.
func NewWAL(path string, fileID uint32) (*WAL, error) {
	fileName := filepath.Join(path, strconv.Itoa(int(fileID))+".data")
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Get actual file size for correct offset tracking
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &WAL{
		file:     f,
		writer:   bufio.NewWriter(f),
		fileID:   fileID,
		filePath: path,
		offset:   info.Size(),
	}, nil
}

// Write appends an entry to the log.
// Returns the offset where it was written, size on disk, and error.
func (w *WAL) Write(key string, value []byte, timestamp uint64) (int64, int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	kBytes := []byte(key)
	kSize := uint32(len(kBytes))
	vSize := uint32(len(value))

	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint64(header[4:12], timestamp)
	binary.BigEndian.PutUint32(header[12:16], kSize)
	binary.BigEndian.PutUint32(header[16:20], vSize)

	// CRC over header (sans CRC field) + key + value
	crcData := append(header[4:], kBytes...)
	crcData = append(crcData, value...)
	crc := crc32.ChecksumIEEE(crcData)
	binary.BigEndian.PutUint32(header[0:4], crc)

	// Record offset BEFORE writing (using tracked position, not Seek)
	writeOffset := w.offset

	if _, err := w.writer.Write(header); err != nil {
		return 0, 0, err
	}
	if _, err := w.writer.Write(kBytes); err != nil {
		return 0, 0, err
	}
	if _, err := w.writer.Write(value); err != nil {
		return 0, 0, err
	}

	if err := w.writer.Flush(); err != nil {
		return 0, 0, err
	}

	entrySize := int64(HeaderSize) + int64(kSize) + int64(vSize)
	w.offset += entrySize

	return writeOffset, entrySize, nil
}

// Sync forces data to disk.
func (w *WAL) Sync() error {
	return w.file.Sync()
}

// Close the file handle.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writer != nil {
		w.writer.Flush()
	}
	return w.file.Close()
}

// Size returns the current file size (tracked offset).
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

// ReadAt reads an entry from a specific offset.
func (w *WAL) ReadAt(offset int64) (*Entry, error) {
	header := make([]byte, HeaderSize)
	_, err := w.file.ReadAt(header, offset)
	if err != nil {
		return nil, err
	}

	crc := binary.BigEndian.Uint32(header[0:4])
	timestamp := binary.BigEndian.Uint64(header[4:12])
	kSize := binary.BigEndian.Uint32(header[12:16])
	vSize := binary.BigEndian.Uint32(header[16:20])

	payloadOffset := offset + HeaderSize
	payload := make([]byte, kSize+vSize)
	_, err = w.file.ReadAt(payload, payloadOffset)
	if err != nil {
		return nil, err
	}

	// Verify CRC
	crcData := append(header[4:], payload...)
	if crc32.ChecksumIEEE(crcData) != crc {
		return nil, fmt.Errorf("CRC mismatch at offset %d: data corrupted", offset)
	}

	return &Entry{
		Timestamp: timestamp,
		Key:       string(payload[:kSize]),
		Value:     payload[kSize:],
	}, nil
}

// ScanEntries reads ALL entries from this WAL file sequentially.
// Used for crash recovery to rebuild the in-memory index.
func (w *WAL) ScanEntries() ([]struct {
	Entry  *Entry
	Offset int64
	Size   int64
}, error) {
	info, err := w.file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()

	var results []struct {
		Entry  *Entry
		Offset int64
		Size   int64
	}

	var pos int64
	for pos < fileSize {
		entry, err := w.ReadAt(pos)
		if err != nil {
			// Corrupted entry — stop scanning (truncated write)
			break
		}

		kSize := int64(len(entry.Key))
		vSize := int64(len(entry.Value))
		entrySize := int64(HeaderSize) + kSize + vSize

		results = append(results, struct {
			Entry  *Entry
			Offset int64
			Size   int64
		}{
			Entry:  entry,
			Offset: pos,
			Size:   entrySize,
		})

		pos += entrySize
	}

	return results, nil
}

// FilePath returns the directory path.
func (w *WAL) FilePath() string {
	return w.filePath
}

// FileID returns the current active file ID.
func (w *WAL) FileID() uint32 {
	return w.fileID
}

// FindWALFiles returns sorted file IDs of all .data files in a directory.
func FindWALFiles(path string) ([]uint32, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var ids []uint32
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".data") {
			idStr := strings.TrimSuffix(name, ".data")
			id, err := strconv.ParseUint(idStr, 10, 32)
			if err != nil {
				continue
			}
			ids = append(ids, uint32(id))
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}