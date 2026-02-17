package common

// NodeID uniquely identifies a node in the cluster.
type NodeID = string

// ShardID uniquely identifies a shard (Raft group).
type ShardID = uint64

// CompareBytes compares two byte slices lexicographically.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func CompareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// ContainsString checks if a string slice contains a target value.
func ContainsString(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}

// MinUint64 returns the smaller of two uint64 values.
func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
