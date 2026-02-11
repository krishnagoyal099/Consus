package storage

import (
    "bufio"
    "encoding/binary"
    "hash/crc32"
    "io"
    "os"
    "path/filepath"
    "strconv"
    "sync"
)

const (
    HeaderSize = 4 + 8 + 4 + 4 // CRC + Timestamp + KeySize + ValueSize
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
}

// NewWAL creates or opens a WAL file.
func NewWAL(path string, fileID uint32) (*WAL, error) {
    fileName := filepath.Join(path, strconv.Itoa(int(fileID))+".data")
    f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
    if err != nil {
        return nil, err
    }

    return &WAL{
        file:     f,
        writer:   bufio.NewWriter(f),
        fileID:   fileID,
        filePath: path,
    }, nil
}

// Write appends an entry to the log.
// Returns the offset where it was written, size on disk, and error.
func (w *WAL) Write(key string, value []byte, timestamp uint64) (int64, int64, error) {
    w.mu.Lock()
    defer w.mu.Unlock()

    // 1. Prepare header
    kBytes := []byte(key)
    kSize := uint32(len(kBytes))
    vSize := uint32(len(value))

    header := make([]byte, HeaderSize)
    binary.BigEndian.PutUint64(header[4:12], timestamp)
    binary.BigEndian.PutUint32(header[12:16], kSize)
    binary.BigEndian.PutUint32(header[16:20], vSize)

    // CRC is calculated over header (without crc field) + key + value
    crcData := append(header[4:], kBytes...)
    crcData = append(crcData, value...)
    crc := crc32.ChecksumIEEE(crcData)
    binary.BigEndian.PutUint32(header[0:4], crc)

    // 2. Get current offset (before write)
    offset, err := w.file.Seek(0, io.SeekCurrent)
    if err != nil {
        return 0, 0, err
    }

    // 3. Write to buffer
    if _, err := w.writer.Write(header); err != nil {
        return 0, 0, err
    }
    if _, err := w.writer.Write(kBytes); err != nil {
        return 0, 0, err
    }
    if _, err := w.writer.Write(value); err != nil {
        return 0, 0, err
    }

    // 4. Flush to OS buffer (fsync handled periodically or explicitly)
    if err := w.writer.Flush(); err != nil {
        return 0, 0, err
    }

    entrySize := int64(HeaderSize + kSize + vSize)
    return offset, entrySize, nil
}

// Sync forces data to disk.
func (w *WAL) Sync() error {
    return w.file.Sync()
}

// Close the file handle.
func (w *WAL) Close() error {
    return w.file.Close()
}

// ReadAt reads an entry from a specific file ID and offset.
// Note: This implementation assumes we are reading from the active file.
// A full implementation would manage multiple file handles for older files.
func (w *WAL) ReadAt(offset int64) (*Entry, error) {
    // Read header
    header := make([]byte, HeaderSize)
    _, err := w.file.ReadAt(header, offset)
    if err != nil {
        return nil, err
    }

    crc := binary.BigEndian.Uint32(header[0:4])
    timestamp := binary.BigEndian.Uint64(header[4:12])
    kSize := binary.BigEndian.Uint32(header[12:16])
    vSize := binary.BigEndian.Uint32(header[16:20])

    // Read Key and Value
    payloadOffset := offset + HeaderSize
    payload := make([]byte, kSize+vSize)
    _, err = w.file.ReadAt(payload, payloadOffset)
    if err != nil {
        return nil, err
    }

    key := string(payload[:kSize])
    value := payload[kSize:]

    // Verify CRC
    crcData := append(header[4:], payload...)
    if crc32.ChecksumIEEE(crcData) != crc {
        return nil, io.ErrUnexpectedEOF // Corrupted data
    }

    return &Entry{
        Timestamp: timestamp,
        Key:       key,
        Value:     value,
    }, nil
}

// FilePath returns the directory path.
func (w *WAL) FilePath() string {
    return w.filePath
}

// FileID returns the current active file ID.
func (w *WAL) FileID() uint32 {
    return w.fileID
}