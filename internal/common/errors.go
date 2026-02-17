package common

import "errors"

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrNotLeader      = errors.New("not the leader")
	ErrLaneOverloaded = errors.New("commit lane overloaded")
	ErrNoShardForKey  = errors.New("no shard found for key")
	ErrShardSplitting = errors.New("shard is currently splitting")
	ErrShardMerging   = errors.New("shard is currently merging")
)
