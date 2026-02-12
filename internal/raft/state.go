package raft

// StateString returns the current state as a string.
func (n *Node) StateString() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state.String()
}
