package shard

import (
	"log"
	"sort"
	"sync"
	"time"
)

// Manager handles adaptive sharding decisions: auto-split overloaded shards,
// auto-merge underused shards, and leader rebalancing across nodes.
//
// Unlike TiKV which requires an external Placement Driver (PD), Consus embeds
// this logic directly — making it a true single-binary deployment.
type Manager struct {
	mu         sync.RWMutex
	shards     map[uint64]*ShardMetadata
	sortedKeys []*ShardMetadata // sorted by StartKey for binary search

	// Thresholds (tunable)
	splitSizeThreshold uint64 // bytes, triggers split
	splitQPSThreshold  uint64 // QPS, triggers split
	mergeSizeThreshold uint64 // merge if combined size < this
	mergeQPSThreshold  uint64 // merge if combined QPS < this

	// Cluster state
	nodes map[string]*NodeState

	// Background evaluation
	stopCh   chan struct{}
	wg       sync.WaitGroup
	actionCh chan ShardAction // emits actions for external consumers

	nextShardID uint64
}

// ManagerConfig holds tunable parameters for the shard manager.
type ManagerConfig struct {
	SplitSizeThreshold uint64        // default: 64MB
	SplitQPSThreshold  uint64        // default: 10000
	MergeSizeThreshold uint64        // default: 16MB
	MergeQPSThreshold  uint64        // default: 100
	EvalInterval       time.Duration // default: 10 seconds
}

// DefaultManagerConfig returns sensible defaults.
func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		SplitSizeThreshold: 64 * 1024 * 1024, // 64MB
		SplitQPSThreshold:  10000,
		MergeSizeThreshold: 16 * 1024 * 1024, // 16MB
		MergeQPSThreshold:  100,
		EvalInterval:       10 * time.Second,
	}
}

// NewManager creates and starts a new adaptive shard manager.
func NewManager(config ManagerConfig) *Manager {
	sm := &Manager{
		shards:             make(map[uint64]*ShardMetadata),
		nodes:              make(map[string]*NodeState),
		splitSizeThreshold: config.SplitSizeThreshold,
		splitQPSThreshold:  config.SplitQPSThreshold,
		mergeSizeThreshold: config.MergeSizeThreshold,
		mergeQPSThreshold:  config.MergeQPSThreshold,
		stopCh:             make(chan struct{}),
		actionCh:           make(chan ShardAction, 64),
		nextShardID:        1,
	}

	// Start background evaluation loop
	sm.wg.Add(1)
	go sm.evaluationLoop(config.EvalInterval)

	log.Printf("[SHARD-MGR] Initialized: splitSize=%dMB, splitQPS=%d, mergeSize=%dMB",
		config.SplitSizeThreshold/(1024*1024), config.SplitQPSThreshold,
		config.MergeSizeThreshold/(1024*1024))

	return sm
}

// AddShard registers a new shard with the manager.
func (sm *Manager) AddShard(meta *ShardMetadata) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if meta.ID == 0 {
		meta.ID = sm.nextShardID
		sm.nextShardID++
	}
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now()
	}
	meta.Epoch = 1

	sm.shards[meta.ID] = meta
	sm.rebuildSortedKeys()
}

// RemoveShard removes a shard from the manager.
func (sm *Manager) RemoveShard(shardID uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.shards, shardID)
	sm.rebuildSortedKeys()
}

// RegisterNode adds or updates a node in the cluster topology.
func (sm *Manager) RegisterNode(nodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, exists := sm.nodes[nodeID]; !exists {
		sm.nodes[nodeID] = &NodeState{
			ID:            nodeID,
			LastHeartbeat: time.Now(),
			Alive:         true,
		}
	}
}

// UpdateNodeHeartbeat updates the last heartbeat for a node.
func (sm *Manager) UpdateNodeHeartbeat(nodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if ns, ok := sm.nodes[nodeID]; ok {
		ns.LastHeartbeat = time.Now()
		ns.Alive = true
	}
}

// UpdateShardStats updates the size and QPS metrics for a shard.
func (sm *Manager) UpdateShardStats(shardID uint64, size uint64, qps uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if s, ok := sm.shards[shardID]; ok {
		s.Size = size
		s.QPS = qps
	}
}

// RouteLookup finds which shard owns a given key using binary search on sorted ranges.
func (sm *Manager) RouteLookup(key string) (*ShardMetadata, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if len(sm.sortedKeys) == 0 {
		return nil, errNoShardForKey
	}

	// Binary search for the shard whose range contains this key
	idx := sort.Search(len(sm.sortedKeys), func(i int) bool {
		endKey := sm.sortedKeys[i].EndKey
		if endKey == "" { // empty = infinity
			return true
		}
		return endKey > key
	})

	if idx >= len(sm.sortedKeys) {
		return nil, errNoShardForKey
	}

	shard := sm.sortedKeys[idx]
	if key >= shard.StartKey {
		cp := *shard
		return &cp, nil
	}

	return nil, errNoShardForKey
}

// EvaluateShards checks all shards and returns recommended actions.
func (sm *Manager) EvaluateShards() []ShardAction {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var actions []ShardAction

	for _, shard := range sm.shards {
		if shard.State != ShardActive {
			continue
		}

		// CHECK SPLIT CONDITIONS
		if shard.Size > sm.splitSizeThreshold || shard.QPS > sm.splitQPSThreshold {
			reason := "size exceeded threshold"
			if shard.QPS > sm.splitQPSThreshold {
				reason = "QPS exceeded threshold"
			}
			if shard.Size > sm.splitSizeThreshold && shard.QPS > sm.splitQPSThreshold {
				reason = "both size and QPS exceeded thresholds"
			}
			actions = append(actions, ShardAction{
				Type:    ActionSplit,
				ShardID: shard.ID,
				Reason:  reason,
			})
			continue
		}

		// CHECK MERGE CONDITIONS
		adjacent := sm.findAdjacentShard(shard)
		if adjacent != nil && adjacent.State == ShardActive {
			combinedSize := shard.Size + adjacent.Size
			combinedQPS := shard.QPS + adjacent.QPS
			if combinedSize < sm.mergeSizeThreshold && combinedQPS < sm.mergeQPSThreshold {
				actions = append(actions, ShardAction{
					Type:     ActionMerge,
					ShardID:  shard.ID,
					TargetID: adjacent.ID,
					Reason:   "combined size and QPS below merge threshold",
				})
			}
		}
	}

	// CHECK LEADER BALANCE
	actions = append(actions, sm.evaluateLeaderBalance()...)

	return actions
}

// ActionCh returns the channel that emits shard management actions.
func (sm *Manager) ActionCh() <-chan ShardAction {
	return sm.actionCh
}

// GetShards returns a copy of all shard metadata.
func (sm *Manager) GetShards() []*ShardMetadata {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make([]*ShardMetadata, 0, len(sm.shards))
	for _, s := range sm.shards {
		cp := *s
		result = append(result, &cp)
	}
	return result
}

// GetNodes returns a copy of all node states.
func (sm *Manager) GetNodes() []*NodeState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	result := make([]*NodeState, 0, len(sm.nodes))
	for _, n := range sm.nodes {
		cp := *n
		result = append(result, &cp)
	}
	return result
}

// Stop shuts down the shard manager.
func (sm *Manager) Stop() {
	close(sm.stopCh)
	sm.wg.Wait()
}

// --- Internal methods ---

func (sm *Manager) rebuildSortedKeys() {
	sm.sortedKeys = make([]*ShardMetadata, 0, len(sm.shards))
	for _, s := range sm.shards {
		sm.sortedKeys = append(sm.sortedKeys, s)
	}
	sort.Slice(sm.sortedKeys, func(i, j int) bool {
		return sm.sortedKeys[i].StartKey < sm.sortedKeys[j].StartKey
	})
}

func (sm *Manager) findAdjacentShard(shard *ShardMetadata) *ShardMetadata {
	for i, s := range sm.sortedKeys {
		if s.ID == shard.ID && i+1 < len(sm.sortedKeys) {
			return sm.sortedKeys[i+1]
		}
	}
	return nil
}

func (sm *Manager) evaluateLeaderBalance() []ShardAction {
	var actions []ShardAction

	// Count leaders per node
	leaderCount := make(map[string]int)
	for _, node := range sm.nodes {
		if node.Alive {
			leaderCount[node.ID] = 0
		}
	}
	for _, shard := range sm.shards {
		if shard.Leader != "" {
			leaderCount[shard.Leader]++
		}
	}

	if len(leaderCount) < 2 {
		return actions
	}

	// Find max and min
	var maxNode, minNode string
	maxCount, minCount := 0, int(^uint(0)>>1)
	for id, count := range leaderCount {
		if count > maxCount {
			maxCount = count
			maxNode = id
		}
		if count < minCount {
			minCount = count
			minNode = id
		}
	}

	// Rebalance if imbalance > 2
	if maxCount-minCount > 2 {
		for _, shard := range sm.shards {
			if shard.Leader == maxNode && containsNode(shard.Replicas, minNode) {
				actions = append(actions, ShardAction{
					Type:       ActionTransferLeader,
					ShardID:    shard.ID,
					TargetNode: minNode,
					Reason:     "leader rebalancing",
				})
				break
			}
		}
	}

	return actions
}

func (sm *Manager) evaluationLoop(interval time.Duration) {
	defer sm.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.stopCh:
			return
		case <-ticker.C:
			actions := sm.EvaluateShards()
			for _, action := range actions {
				select {
				case sm.actionCh <- action:
					log.Printf("[SHARD-MGR] Action: %s shard=%d reason=%q", action.Type, action.ShardID, action.Reason)
				default:
					// Channel full, skip
				}
			}
		}
	}
}

func containsNode(nodes []string, target string) bool {
	for _, n := range nodes {
		if n == target {
			return true
		}
	}
	return false
}

var errNoShardForKey = &shardError{"no shard found for key"}

type shardError struct {
	msg string
}

func (e *shardError) Error() string { return e.msg }
