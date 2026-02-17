package shard

import (
	"time"
)

// ShardState represents the lifecycle state of a shard.
type ShardState int

const (
	ShardActive       ShardState = iota // Normal operation
	ShardSplitting                      // Currently splitting into two
	ShardMerging                        // Currently merging with adjacent shard
	ShardTransferring                   // Leader transfer in progress
)

func (s ShardState) String() string {
	return [...]string{"Active", "Splitting", "Merging", "Transferring"}[s]
}

// ActionType represents shard management operations.
type ActionType int

const (
	ActionSplit          ActionType = iota // Split overloaded shard
	ActionMerge                           // Merge underused adjacent shards
	ActionTransferLeader                  // Move leader to balance load
	ActionAddReplica                      // Add a new replica
	ActionRemoveReplica                   // Remove a replica
)

func (a ActionType) String() string {
	return [...]string{"Split", "Merge", "TransferLeader", "AddReplica", "RemoveReplica"}[a]
}

// ShardMetadata describes a single shard (one Raft group).
type ShardMetadata struct {
	ID        uint64
	StartKey  string     // inclusive lower bound
	EndKey    string     // exclusive upper bound (empty = infinity)
	State     ShardState
	Leader    string     // node ID of the current leader
	Replicas  []string   // all node IDs holding this shard
	Size      uint64     // approximate data size in bytes
	QPS       uint64     // queries per second (rolling average)
	CreatedAt time.Time
	Epoch     uint64     // incremented on split/merge to detect stale routes
}

// ShardAction represents a management decision made by the shard manager.
type ShardAction struct {
	Type       ActionType
	ShardID    uint64
	TargetID   uint64 // for merge: the adjacent shard
	TargetNode string // for leader transfer: destination node
	Reason     string
}

// NodeState tracks the health and load of a cluster node.
type NodeState struct {
	ID            string
	LeaderCount   int
	ShardCount    int
	LastHeartbeat time.Time
	Alive         bool
}
