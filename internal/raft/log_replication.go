package raft

import (
    "context"
    "log"
    "time"

    "github.com/consus/consus/proto"
)

// runLeader manages the heartbeat and log replication.
func (n *Node) runLeader() {
    // Initialize leader state
    n.mu.Lock()
    lastLogIndex := uint64(len(n.log))
    for peerID := range n.Peers {
        n.nextIndex[peerID] = lastLogIndex + 1
        n.matchIndex[peerID] = 0
    }
    n.mu.Unlock()

    ticker := time.NewTicker(50 * time.Millisecond) // Heartbeat interval
    defer ticker.Stop()

    for {
        select {
        case <-n.shutdownCh:
            return
        case <-ticker.C:
            n.replicateLogs()
        }

        // Check if we are still leader
        n.mu.RLock()
        isLeader := n.state == Leader
        n.mu.RUnlock()
        if !isLeader {
            return
        }
    }
}

// replicateLogs sends AppendEntries to all peers.
func (n *Node) replicateLogs() {
    n.mu.RLock()
    currentTerm := n.currentTerm
    peers := n.Peers
    commitIndex := n.commitIndex
    n.mu.RUnlock()

    for peerID, peer := range peers {
        go func(id string, p Peer) {
            n.mu.Lock()
            nextIdx := n.nextIndex[id]
            prevLogIndex := nextIdx - 1
            var prevLogTerm uint64
            
            if prevLogIndex > 0 && int(prevLogIndex) <= len(n.log) {
                prevLogTerm = n.log[prevLogIndex-1].Term
            }

            // Prepare entries to send
            var entries []*proto.LogEntry
            if int(nextIdx) <= len(n.log) {
                for i := nextIdx; int(i) <= len(n.log); i++ {
                    entry := n.log[i-1]
                    entries = append(entries, &proto.LogEntry{
                        Term:    entry.Term,
                        Index:   entry.Index,
                        Command: entry.Command,
                    })
                }
            }
            n.mu.Unlock()

            req := &proto.AppendEntriesRequest{
                Term:         currentTerm,
                LeaderId:     n.ID,
                PrevLogIndex: prevLogIndex,
                PrevLogTerm:  prevLogTerm,
                LeaderCommit: commitIndex,
                Entries:      entries,
            }

            ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
            defer cancel()

            resp, err := p.AppendEntries(ctx, req)
            if err != nil {
                return
            }

            n.mu.Lock()
            defer n.mu.Unlock()

            // If term > currentTerm, step down
            if resp.Term > n.currentTerm {
                n.currentTerm = resp.Term
                n.state = Follower
                n.votedFor = ""
                return
            }

            if resp.Success {
                // Update nextIndex and matchIndex
                if len(entries) > 0 {
                    lastEntryIdx := entries[len(entries)-1].Index
                    n.nextIndex[id] = lastEntryIdx + 1
                    n.matchIndex[id] = lastEntryIdx
                }
                
                // Try to commit
                n.updateCommitIndex()
            } else {
                // Decrement nextIndex and retry next tick
                if n.nextIndex[id] > 1 {
                    n.nextIndex[id]--
                }
            }
        }(peerID, peer)
    }
}

// updateCommitIndex calculates the highest index committed by a majority.
func (n *Node) updateCommitIndex() {
    // Only leader can commit
    if n.state != Leader {
        return
    }

    for idx := uint64(len(n.log)); idx > n.commitIndex; idx-- {
        if n.log[idx-1].Term != n.currentTerm {
            continue // Only commit entries from current term (Raft safety)
        }

        count := 1 // Self
        for peerID := range n.Peers {
            if n.matchIndex[peerID] >= idx {
                count++
            }
        }

        // Quorum?
        if count > (len(n.Peers)+1)/2 {
            n.commitIndex = idx
            n.applyCommittedEntries()
            break
        }
    }
}

// applyCommittedEntries sends committed commands to the KV store via ApplyCh.
func (n *Node) applyCommittedEntries() {
    for n.lastApplied < n.commitIndex {
        n.lastApplied++
        entry := n.log[n.lastApplied-1] // 1-based index
        log.Printf("[RAFT] Applying committed log index %d: %s", entry.Index, entry.Command)
        n.ApplyCh <- entry.Command
    }
}

// AppendEntries handles incoming AppendEntries RPC (Called by Transport Layer).
func (n *Node) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
    n.mu.Lock()
    defer n.mu.Unlock()

    resp := &proto.AppendEntriesResponse{
        Term:    n.currentTerm,
        Success: false,
    }

    // Rule 1: Reply false if term < currentTerm
    if req.Term < n.currentTerm {
        return resp, nil
    }

    // Rule 2: If term > currentTerm, step down
    if req.Term > n.currentTerm {
        n.currentTerm = req.Term
        n.votedFor = ""
    }

    // Valid Leader found
    n.state = Follower
    n.leaderId = req.LeaderId
    n.lastHeartbeat = time.Now()

    // Rule 3: Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
    if req.PrevLogIndex > 0 {
        if uint64(len(n.log)) < req.PrevLogIndex {
            return resp, nil
        }
        if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
            return resp, nil
        }
    }

    // Rule 4: Append new entries
    if len(req.Entries) > 0 {
        // Delete conflicting entries and append new ones
        // Simple approach: Truncate log from PrevLogIndex+1 and append
        n.log = n.log[:req.PrevLogIndex] 
        for _, e := range req.Entries {
            n.log = append(n.log, LogEntry{
                Term:    e.Term,
                Index:   e.Index,
                Command: e.Command,
            })
        }
    }

    // Rule 5: Update commitIndex
    if req.LeaderCommit > n.commitIndex {
        n.commitIndex = min(req.LeaderCommit, uint64(len(n.log)))
        n.applyCommittedEntries()
    }

    resp.Success = true
    resp.MatchIndex = uint64(len(n.log))
    return resp, nil
}

func min(a, b uint64) uint64 {
    if a < b {
        return a
    }
    return b
}