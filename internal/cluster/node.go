package cluster

import (
	"context"
	"time"

	"github.com/krishnagoyal099/Consus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Node represents a remote node in the cluster.
type Node struct {
    ID      string
    Address string // host:port
    
    // gRPC client connection
    conn   *grpc.ClientConn
    client proto.RaftClient
}

// NewNode creates a new node representation and establishes a gRPC connection.
func NewNode(id, address string) (*Node, error) {
    conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, err
    }

    return &Node{
        ID:      id,
        Address: address,
        conn:    conn,
        client:  proto.NewRaftClient(conn),
    }, nil
}

// RequestVote implements the raft.Peer interface.
func (n *Node) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
    ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
    defer cancel()
    return n.client.RequestVote(ctx, req)
}

// AppendEntries implements the raft.Peer interface.
func (n *Node) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
    ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
    defer cancel()
    return n.client.AppendEntries(ctx, req)
}

// Close closes the gRPC connection.
func (n *Node) Close() error {
    return n.conn.Close()
}