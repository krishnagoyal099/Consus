package cluster

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Ring implements consistent hashing for key distribution across nodes.
type Ring struct {
	mu           sync.RWMutex
	nodes        map[string]bool   // physical node IDs
	ring         []uint32          // sorted hash ring
	hashMap      map[uint32]string // hash -> node ID
	virtualNodes int
}

// NewRing creates a new consistent hash ring.
func NewRing(virtualNodes int) *Ring {
	return &Ring{
		nodes:        make(map[string]bool),
		ring:         make([]uint32, 0),
		hashMap:      make(map[uint32]string),
		virtualNodes: virtualNodes,
	}
}

// AddNode adds a physical node to the ring with virtual nodes.
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nodes[nodeID] = true
	for i := 0; i < r.virtualNodes; i++ {
		key := nodeID + "-" + strconv.Itoa(i)
		hash := crc32.ChecksumIEEE([]byte(key))
		r.ring = append(r.ring, hash)
		r.hashMap[hash] = nodeID
	}
	sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] })
}

// RemoveNode removes a node and its virtual nodes from the ring.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.nodes, nodeID)
	newRing := make([]uint32, 0)
	for _, hash := range r.ring {
		if r.hashMap[hash] != nodeID {
			newRing = append(newRing, hash)
		} else {
			delete(r.hashMap, hash)
		}
	}
	r.ring = newRing
}

// GetNode returns the node responsible for the given key.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= hash })
	if idx >= len(r.ring) {
		idx = 0
	}
	return r.hashMap[r.ring[idx]]
}

// Nodes returns a list of all physical node IDs.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}
