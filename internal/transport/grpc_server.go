package transport

import (
	"context"
	"encoding/json"

	"github.com/consus/consus/internal/storage"
	"github.com/consus/consus/proto"
)

// RaftNode is the interface required by the Transport layer to interact with the Consensus module.
// This decouples the network layer from the specific Raft implementation.
type RaftNode interface {
    RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error)
    AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error)
    
    // Propose submits a command to the Raft log. It blocks until committed or error.
    Propose(command string) error
    
    // IsLeader checks if this node is the current leader.
    IsLeader() bool
    
    // LeaderAddr returns the address of the current leader.
    LeaderAddr() string
}

// Server struct holds the dependencies.
type Server struct {
    proto.UnimplementedKVServer
    proto.UnimplementedRaftServer

    Store *storage.Bitcask // Pointer to storage layer
    Raft  RaftNode
}

// NewServer creates a new gRPC server instance.
func NewServer(store *storage.Bitcask, raft RaftNode) *Server {
    return &Server{
        Store: store,
        Raft:  raft,
    }
}

// --- KV Service Implementation ---

func (s *Server) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
    // For linearizable reads, we should ideally verify leadership via a ReadIndex.
    // For this phase, we perform a local read.
    val, err := s.Store.Get(req.Key)
    if err != nil {
        if err == storage.ErrKeyNotFound {
            return &proto.GetResponse{Found: false}, nil
        }
        return &proto.GetResponse{Found: false, Error: err.Error()}, nil
    }

    return &proto.GetResponse{
        Value: string(val),
        Found: true,
    }, nil
}

func (s *Server) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
    if !s.Raft.IsLeader() {
        return &proto.PutResponse{
            Success: false, 
            Error: "not leader: redirect to " + s.Raft.LeaderAddr(),
        }, nil
    }

    // Serialize command to store in Raft log
    cmd := map[string]string{"op": "PUT", "key": req.Key, "value": req.Value}
    data, _ := json.Marshal(cmd)

    // Propose to Raft
    if err := s.Raft.Propose(string(data)); err != nil {
        return &proto.PutResponse{Success: false, Error: err.Error()}, nil
    }

    return &proto.PutResponse{Success: true}, nil
}

func (s *Server) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
    if !s.Raft.IsLeader() {
        return &proto.DeleteResponse{
            Success: false,
            Error:   "not leader",
        }, nil
    }

    cmd := map[string]string{"op": "DELETE", "key": req.Key}
    data, _ := json.Marshal(cmd)

    if err := s.Raft.Propose(string(data)); err != nil {
        return &proto.DeleteResponse{Success: false, Error: err.Error()}, nil
    }

    return &proto.DeleteResponse{Success: true}, nil
}

// --- Raft Service Implementation (Delegates to Raft Logic) ---

func (s *Server) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
    return s.Raft.RequestVote(ctx, req)
}

func (s *Server) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
    return s.Raft.AppendEntries(ctx, req)
}