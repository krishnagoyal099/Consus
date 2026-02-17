package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/krishnagoyal099/Consus/proto"
)

// State Type
type State int

const (
    Follower State = iota
    Candidate
    Leader
)

func (s State) String() string {
    return [...]string{"Follower", "Candidate", "Leader"}[s]
}

// Peer interface abstracts the network call to other nodes.
type Peer interface {
    RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error)
    AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error)
}

// LogEntry is the internal representation of a log entry.
type LogEntry struct {
    Term    uint64
    Index   uint64
    Command string
}

// Node represents a Raft consensus node.
type Node struct {
    mu sync.RWMutex

    // Configuration
    ID      string
    Peers   map[string]Peer
    ApplyCh chan string // Channel to send committed commands to the KV store

    // Persistent State
    currentTerm uint64
    votedFor    string
    log         []LogEntry

    // Volatile State
    commitIndex uint64
    lastApplied uint64

    // Leader Volatile State
    nextIndex  map[string]uint64
    matchIndex map[string]uint64

    // Internal State
    state          State
    leaderId       string
    lastHeartbeat time.Time

    // Timeouts
    electionTimeout time.Duration
    heartbeatTicker *time.Ticker

    // Shutdown
    shutdownCh chan struct{}
}

// NewNode creates a new Raft node.
func NewNode(id string, applyCh chan string) *Node {
    rand.Seed(time.Now().UnixNano())
    return &Node{
        ID:              id,
        Peers:           make(map[string]Peer),
        log:             make([]LogEntry, 0),
        ApplyCh:         applyCh,
        state:           Follower,
        lastHeartbeat:   time.Now(), // Prevents immediate election on startup
        electionTimeout: randomTimeout(300, 500), // 300-500ms for stable elections
        shutdownCh:      make(chan struct{}),
        nextIndex:       make(map[string]uint64),
        matchIndex:      make(map[string]uint64),
    }
}

// AddPeer adds a peer to the cluster configuration.
func (n *Node) AddPeer(id string, peer Peer) {
    n.mu.Lock()
    defer n.mu.Unlock()
    n.Peers[id] = peer
}

// Run starts the Raft loop.
func (n *Node) Run() {
    for {
        select {
        case <-n.shutdownCh:
            return
        default:
        }

        n.mu.RLock()
        state := n.state
        n.mu.RUnlock()

        switch state {
        case Follower:
            n.runFollower()
        case Candidate:
            n.runCandidate()
        case Leader:
            n.runLeader()
        }
    }
}

// Stop shuts down the node.
func (n *Node) Stop() {
    close(n.shutdownCh)
}

// Helper to get random timeout
func randomTimeout(min, max int) time.Duration {
    return time.Duration(min+rand.Intn(max-min)) * time.Millisecond
}

// GetTerm and GetState helpers for Transport layer
func (n *Node) GetTerm() uint64 {
    n.mu.RLock()
    defer n.mu.RUnlock()
    return n.currentTerm
}

// IsLeader checks if current node is leader
func (n *Node) IsLeader() bool {
    n.mu.RLock()
    defer n.mu.RUnlock()
    return n.state == Leader
}

// LeaderAddr returns the known leader ID
func (n *Node) LeaderAddr() string {
    n.mu.RLock()
    defer n.mu.RUnlock()
    return n.leaderId
}

// Propose is called by the KV service to submit a command.
func (n *Node) Propose(command string) error {
    n.mu.Lock()
    defer n.mu.Unlock()

    if n.state != Leader {
        return ErrNotLeader
    }

    // Append to local log
    entry := LogEntry{
        Term:    n.currentTerm,
        Index:   uint64(len(n.log) + 1),
        Command: command,
    }
    n.log = append(n.log, entry)
    log.Printf("[RAFT-LEADER] Proposed new entry at index %d", entry.Index)
    
    // We commit after replication is confirmed in the heartbeat/replication loop.
    // For immediate feedback (simplified), we might wait, but here we just return nil
    // (Async commit for now to keep logic simple).
    return nil
}