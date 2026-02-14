package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "net"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "syscall"

    "github.com/consus/consus/api"
    "github.com/consus/consus/internal/cluster"
    "github.com/consus/consus/internal/raft"
    "github.com/consus/consus/internal/storage"
    "github.com/consus/consus/internal/transport"
    "github.com/consus/consus/proto"
    "google.golang.org/grpc"
)

func main() {
    // 1. Parse Flags
    id := flag.String("id", "node1", "Unique node ID")
    port := flag.Int("port", 50051, "gRPC Port")
    httpPort := flag.Int("http-port", 8080, "HTTP Dashboard Port")
    clusterStr := flag.String("cluster", "node1=localhost:50051,node2=localhost:50052,node3=localhost:50053", "Cluster config: id=addr,id=addr")
    dataDir := flag.String("data", "./data/"+*id, "Data directory")
    flag.Parse()

    log.Printf("Starting Consus Node [%s]...", *id)

    // 2. Initialize Storage
    store, err := storage.Open(*dataDir)
    if err != nil {
        log.Fatalf("Failed to open storage: %v", err)
    }

    // 3. Initialize Raft
    applyCh := make(chan string, 100)
    raftNode := raft.NewNode(*id, applyCh)
    
    // Applier Loop: Apply committed entries to the Storage Engine
    go func() {
        for cmd := range applyCh {
            var command map[string]string
            if err := json.Unmarshal([]byte(cmd), &command); err != nil {
                log.Printf("Error unmarshaling command: %v", err)
                continue
            }

            op := command["op"]
            key := command["key"]
            val := command["value"]

            switch op {
            case "PUT":
                if err := store.Put(key, []byte(val)); err != nil {
                    log.Printf("Error applying PUT: %v", err)
                }
            case "DELETE":
                if err := store.Delete(key); err != nil {
                    log.Printf("Error applying DELETE: %v", err)
                }
            }
        }
    }()

    // 4. Initialize Cluster & Peers
    ring := cluster.NewRing(50) // 50 virtual nodes per physical node
    
    // Parse cluster config
    peers := strings.Split(*clusterStr, ",")
    peerMap := make(map[string]raft.Peer)

    for _, p := range peers {
        parts := strings.Split(p, "=")
        if len(parts) != 2 {
            continue
        }
        peerID := parts[0]
        peerAddr := parts[1]

        ring.AddNode(peerID)

        // Add peer to Raft if it's not self
        if peerID != *id {
            pNode, err := cluster.NewNode(peerID, peerAddr)
            if err != nil {
                log.Printf("Warning: Could not connect to peer %s: %v", peerID, err)
            }
            peerMap[peerID] = pNode
            raftNode.AddPeer(peerID, pNode)
        }
    }

    // Start Raft background processes
    go raftNode.Run()

    // 5. Initialize gRPC Server
    // Create the transport server which links Store and Raft
    grpcSrv := transport.NewServer(store, raftNode)
    
    // Create a wrapper for the Ring to handle sharding/forwarding logic if needed
    // For simplicity in this phase, we pass the ring to the server logic internally or assume
    // clients call the correct node. Let's wrap the server logic:
    wrappedSrv := &ShardingServer{
        Server: grpcSrv,
        Ring:   ring,
        ID:     *id,
    }

    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    grpcServer := grpc.NewServer()
    proto.RegisterKVServer(grpcServer, wrappedSrv)
    proto.RegisterRaftServer(grpcServer, wrappedSrv) // wrappedSrv delegates Raft calls

    go func() {
        log.Printf("gRPC Server listening on port %d", *port)
        if err := grpcServer.Serve(listener); err != nil {
            log.Fatalf("Failed to serve gRPC: %v", err)
        }
    }()

    // 6. Initialize HTTP Dashboard Server
    // We pass the ring and raftNode to the dashboard for visualization
    httpHandler := api.NewDashboardHandler(ring, raftNode, store, *id)
    go func() {
        log.Printf("HTTP Dashboard listening on port %d", *httpPort)
        if err := http.ListenAndServe(fmt.Sprintf(":%d", *httpPort), httpHandler); err != nil {
            log.Printf("HTTP Server error: %v", err)
        }
    }()

    // 7. Graceful Shutdown
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh

    log.Println("Shutting down...")
    grpcServer.Stop()
    raftNode.Stop()
    store.Close()
}

// ShardingServer wraps the transport server to enforce sharding logic.
type ShardingServer struct {
    *transport.Server
    Ring *cluster.Ring
    ID   string
}

// In a real production system, Put would check the ring here.
// If key belongs to another node, forward or return error.
// For this project, we rely on the client or coordinator to talk to the correct node,
// or the ring map to direct traffic.
// We override the interface just to show the logic location.
/*
func (s *ShardingServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
    owner := s.Ring.GetNode(req.Key)
    if owner != s.ID {
        return &proto.PutResponse{Success: false, Error: "wrong node: belongs to " + owner}, nil
    }
    return s.Server.Put(ctx, req)
}
*/