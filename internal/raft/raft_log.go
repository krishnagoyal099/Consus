package raft

import (
	"sync"
	"time"
)

// RaftLogEntry represents a single entry in the lane-aware Raft log.
type RaftLogEntry struct {
	GlobalIndex uint64
	Term        uint64
	LaneID      int // -1 = serial/transaction path
	Ops         []KVOp
	Timestamp   int64
	Committed   bool
}

// KVOp represents a single key-value operation within a log entry.
type KVOp struct {
	Type  OpType
	Key   string
	Value string
}

// OpType enumerates the operations that can be proposed to Raft.
type OpType int

const (
	OpPut    OpType = iota // Create or update a key
	OpDelete               // Remove a key
	OpCAS                  // Compare-And-Swap (future)
)

func (o OpType) String() string {
	return [...]string{"PUT", "DELETE", "CAS"}[o]
}

// ParallelRaftLog supports lane-aware parallel appends with a global ordering index.
//
// Each commit lane has its own sub-log for parallel append without contention.
// A global mutex serializes only the global index assignment, which is a
// fast atomic-like increment — not a full blocking operation.
type ParallelRaftLog struct {
	// Global sequential index for Raft correctness
	globalIndex uint64

	// Per-lane logs for parallel append without lock contention
	laneLogs [NumLanes][]*RaftLogEntry
	laneMu   [NumLanes]sync.Mutex

	// Merged ordered log for snapshotting and recovery
	mergedLog []*RaftLogEntry
	mu        sync.Mutex
}

// NewParallelRaftLog creates a new lane-aware Raft log.
func NewParallelRaftLog() *ParallelRaftLog {
	return &ParallelRaftLog{}
}

// AppendParallel appends an entry to a lane-specific log and assigns a global index.
// The lane lock allows parallel appends across different lanes; only the global
// index assignment requires the shared lock (very brief critical section).
func (rl *ParallelRaftLog) AppendParallel(entry *RaftLogEntry) uint64 {
	// Lane-level lock for the append itself
	if entry.LaneID >= 0 && entry.LaneID < NumLanes {
		rl.laneMu[entry.LaneID].Lock()
		rl.laneLogs[entry.LaneID] = append(rl.laneLogs[entry.LaneID], entry)
		rl.laneMu[entry.LaneID].Unlock()
	}

	// Global index assignment (only serialization point)
	rl.mu.Lock()
	rl.globalIndex++
	entry.GlobalIndex = rl.globalIndex
	entry.Timestamp = time.Now().UnixNano()
	rl.mergedLog = append(rl.mergedLog, entry)
	rl.mu.Unlock()

	return entry.GlobalIndex
}

// GetEntry returns the entry at the given global index (1-based).
func (rl *ParallelRaftLog) GetEntry(index uint64) *RaftLogEntry {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if index == 0 || int(index) > len(rl.mergedLog) {
		return nil
	}
	return rl.mergedLog[index-1]
}

// LastIndex returns the last global index.
func (rl *ParallelRaftLog) LastIndex() uint64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.globalIndex
}

// EntriesSince returns all entries from startIndex (inclusive) onward.
func (rl *ParallelRaftLog) EntriesSince(startIndex uint64) []*RaftLogEntry {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if startIndex == 0 || int(startIndex) > len(rl.mergedLog) {
		return nil
	}

	result := make([]*RaftLogEntry, len(rl.mergedLog)-int(startIndex)+1)
	copy(result, rl.mergedLog[startIndex-1:])
	return result
}

// Len returns the total number of entries in the merged log.
func (rl *ParallelRaftLog) Len() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return len(rl.mergedLog)
}

// LaneLen returns the number of entries in a specific lane.
func (rl *ParallelRaftLog) LaneLen(laneID int) int {
	if laneID < 0 || laneID >= NumLanes {
		return 0
	}
	rl.laneMu[laneID].Lock()
	defer rl.laneMu[laneID].Unlock()
	return len(rl.laneLogs[laneID])
}
