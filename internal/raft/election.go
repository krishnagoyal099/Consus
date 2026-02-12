package raft

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/consus/consus/proto"
)

var ErrNotLeader = errors.New("not the leader")

// runFollower waits for heartbeats. If timeout occurs, transitions to Candidate.
func (n *Node) runFollower() {
    timeout := n.electionTimeout
    for {
        select {
        case <-n.shutdownCh:
            return
        default:
        }

        n.mu.Lock()
        last := n.lastHeartbeat
        n.mu.Unlock()

        if time.Since(last) > timeout {
            // Transition to Candidate
            n.mu.Lock()
            n.state = Candidate
            n.mu.Unlock()
            log.Printf("[RAFT] Election timeout. Becoming Candidate.")
            return
        }
        time.Sleep(10 * time.Millisecond)
    }
}

// runCandidate manages the election process.
func (n *Node) runCandidate() {
    n.mu.Lock()
    n.currentTerm++
    n.votedFor = n.ID
    currentTerm := n.currentTerm
    n.mu.Unlock()

    log.Printf("[RAFT-CANDIDATE] Starting election for term %d", currentTerm)

    // Vote for self
    var votesReceived int32 = 1
    totalNodes := int32(len(n.Peers) + 1)
    votesNeeded := totalNodes/2 + 1 // Strict majority

    log.Printf("[RAFT-CANDIDATE] Need %d votes out of %d nodes", votesNeeded, totalNodes)

    // Request votes from peers
    for peerID, peer := range n.Peers {
        go func(id string, p Peer) {
            n.mu.RLock()
            lastLogIndex := uint64(len(n.log))
            var lastLogTerm uint64
            if len(n.log) > 0 {
                lastLogTerm = n.log[len(n.log)-1].Term
            }
            n.mu.RUnlock()

            req := &proto.RequestVoteRequest{
                Term:         currentTerm,
                CandidateId:  n.ID,
                LastLogIndex: lastLogIndex,
                LastLogTerm:  lastLogTerm,
            }

            ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
            defer cancel()

            resp, err := p.RequestVote(ctx, req)
            if err != nil {
                log.Printf("[RAFT-CANDIDATE] Vote request to %s failed: %v", id, err)
                return
            }

            n.mu.Lock()
            defer n.mu.Unlock()

            // If term is higher, step down
            if resp.Term > n.currentTerm {
                n.currentTerm = resp.Term
                n.state = Follower
                n.votedFor = ""
                return
            }

            if resp.VoteGranted && n.state == Candidate && n.currentTerm == currentTerm {
                votes := atomic.AddInt32(&votesReceived, 1)
                log.Printf("[RAFT-CANDIDATE] Got vote from %s (%d/%d)", id, votes, votesNeeded)
                if votes >= votesNeeded {
                    // Won Election
                    n.state = Leader
                    n.leaderId = n.ID
                    log.Printf("[RAFT-LEADER] Won election for term %d", n.currentTerm)
                    return
                }
            }
        }(peerID, peer)
    }

    // Wait for election outcome or timeout
    timeout := randomTimeout(300, 500) // Fresh random timeout per election
    start := time.Now()
    for {
        n.mu.RLock()
        state := n.state
        n.mu.RUnlock()

        if state == Leader || state == Follower {
            return // State changed
        }

        if time.Since(start) > timeout {
            // Election failed, new term needed
            log.Printf("[RAFT-CANDIDATE] Election timed out for term %d", currentTerm)
            n.mu.Lock()
            n.state = Follower
            n.lastHeartbeat = time.Now() // Reset to wait before next try
            n.mu.Unlock()
            return
        }
        time.Sleep(10 * time.Millisecond)
    }
}

// RequestVote handles incoming vote requests (Called by Transport Layer).
func (n *Node) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
    n.mu.Lock()
    defer n.mu.Unlock()

    resp := &proto.RequestVoteResponse{
        Term:        n.currentTerm,
        VoteGranted: false,
    }

    // Rule 1: If term < currentTerm, reject
    if req.Term < n.currentTerm {
        return resp, nil
    }

    // Rule 2: If term > currentTerm, step down
    if req.Term > n.currentTerm {
        n.currentTerm = req.Term
        n.state = Follower
        n.votedFor = ""
        n.lastHeartbeat = time.Now()
    }

    // Rule 3: If votedFor is null or candidateId, and log is up-to-date...
    if n.votedFor == "" || n.votedFor == req.CandidateId {
        // Check log up-to-date
        if n.isLogUpToDate(req.LastLogTerm, req.LastLogIndex) {
            n.votedFor = req.CandidateId
            resp.VoteGranted = true
            n.lastHeartbeat = time.Now() // Reset timer on vote grant
            log.Printf("[RAFT] Voted for %s in term %d", req.CandidateId, req.Term)
        }
    }

    return resp, nil
}

func (n *Node) isLogUpToDate(term uint64, index uint64) bool {
    myLastIndex := uint64(len(n.log))
    var myLastTerm uint64
    if len(n.log) > 0 {
        myLastTerm = n.log[len(n.log)-1].Term
    }

    // Raft logic: If the logs have last entries with different terms, then the log with the later term is more up-to-date.
    // If logs end with the same term, then whichever log is longer is more up-to-date.
    if term != myLastTerm {
        return term > myLastTerm
    }
    return index >= myLastIndex
}