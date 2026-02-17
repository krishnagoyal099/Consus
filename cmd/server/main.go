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

	"github.com/krishnagoyal099/Consus/api"
	"github.com/krishnagoyal099/Consus/internal/chaos"
	"github.com/krishnagoyal099/Consus/internal/cluster"
	"github.com/krishnagoyal099/Consus/internal/raft"
	"github.com/krishnagoyal099/Consus/internal/shard"
	"github.com/krishnagoyal099/Consus/internal/storage"
	"github.com/krishnagoyal099/Consus/internal/transport"
	"github.com/krishnagoyal099/Consus/proto"
	"google.golang.org/grpc"
)

func main() {
	// 1. Parse Flags
	id := flag.String("id", "node1", "Unique node ID")
	port := flag.Int("port", 50051, "gRPC Port")
	httpPort := flag.Int("http-port", 8080, "HTTP Dashboard Port")
	clusterStr := flag.String("cluster", "node1=localhost:50051,node2=localhost:50052,node3=localhost:50053", "Cluster config: id=addr,id=addr")
	dataDir := flag.String("data", "./data/node1", "Data directory")
	flag.Parse()

	log.Printf("╔══════════════════════════════════════╗")
	log.Printf("║    CONSUS — Distributed KV Store     ║")
	log.Printf("║  Multi-Raft · Parallel · Tiered      ║")
	log.Printf("╚══════════════════════════════════════╝")
	log.Printf("Starting Node [%s]...", *id)

	// 2. Initialize Storage (Bitcask warm tier + TieredStore wrapper)
	bitcask, err := storage.Open(*dataDir)
	if err != nil {
		log.Fatalf("Failed to open storage: %v", err)
	}

	tieredConfig := storage.DefaultTieredConfig(*dataDir)
	tieredStore := storage.NewTieredStore(bitcask, tieredConfig)

	// 3. Initialize Raft
	applyCh := make(chan string, 100)
	raftNode := raft.NewNode(*id, applyCh)

	// 4. Initialize Parallel Raft Engine
	parallelEngine := raft.NewParallelRaftEngine(*id, raftNode)

	// 5. Initialize Shard Manager
	shardConfig := shard.DefaultManagerConfig()
	shardMgr := shard.NewManager(shardConfig)

	// Add initial shard covering the full key space
	shardMgr.AddShard(&shard.ShardMetadata{
		StartKey: "",
		EndKey:   "",
		Leader:   *id,
		Replicas: []string{*id},
	})

	// Applier Loop: Apply committed entries to the TieredStore
	go func() {
		for cmd := range applyCh {
			applyCommand(cmd, tieredStore)
		}
	}()

	// 6. Initialize Cluster & Peers
	ring := cluster.NewRing(50) // 50 virtual nodes per physical node
	peerAddrs := make(map[string]string)

	// Parse cluster config
	peers := strings.Split(*clusterStr, ",")

	for _, p := range peers {
		parts := strings.Split(p, "=")
		if len(parts) != 2 {
			continue
		}
		peerID := parts[0]
		peerAddr := parts[1]

		ring.AddNode(peerID)
		shardMgr.RegisterNode(peerID)
		peerAddrs[peerID] = peerAddr

		if peerID != *id {
			pNode, err := cluster.NewNode(peerID, peerAddr)
			if err != nil {
				log.Printf("Warning: Could not connect to peer %s: %v", peerID, err)
			}
			raftNode.AddPeer(peerID, pNode)
		}
	}

	// Start Raft background processes
	go raftNode.Run()

	// 7. Initialize gRPC Server
	grpcSrv := transport.NewServer(bitcask, raftNode)

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
	proto.RegisterRaftServer(grpcServer, wrappedSrv)

	go func() {
		log.Printf("gRPC Server listening on port %d", *port)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// 8. Initialize Chaos Engine (built-in — not an external tool)
	chaosEngine := chaos.NewEngine(nil) // No live cluster interface for now

	// 9. Initialize HTTP Dashboard with all subsystems
	httpHandler := api.NewDashboardHandler(
		ring, raftNode, tieredStore, *id,
		api.WithShardManager(shardMgr),
		api.WithParallelEngine(parallelEngine),
		api.WithChaosEngine(chaosEngine),
		api.WithPeerAddresses(peerAddrs),
	)

	go func() {
		log.Printf("HTTP Dashboard listening on http://localhost:%d", *httpPort)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", *httpPort), httpHandler); err != nil {
			log.Printf("HTTP Server error: %v", err)
		}
	}()

	// 10. Shard Action Consumer (handles split/merge/transfer decisions)
	go func() {
		for action := range shardMgr.ActionCh() {
			log.Printf("[MAIN] Shard action: %s shard=%d reason=%q", action.Type, action.ShardID, action.Reason)
		}
	}()

	log.Printf("✅ All systems initialized. Node %s is ready.", *id)

	// 11. Graceful Shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	grpcServer.Stop()
	parallelEngine.Stop()
	shardMgr.Stop()
	raftNode.Stop()
	tieredStore.Close()
}

// applyCommand processes a committed Raft log entry against the storage engine.
func applyCommand(cmd string, store *storage.TieredStore) {
	// Try single command first
	var command map[string]string
	if err := json.Unmarshal([]byte(cmd), &command); err == nil {
		executeOp(command, store)
		return
	}

	// Try batch command
	var batch struct {
		Batch []map[string]string `json:"batch"`
	}
	if err := json.Unmarshal([]byte(cmd), &batch); err == nil && len(batch.Batch) > 0 {
		for _, op := range batch.Batch {
			executeOp(op, store)
		}
		return
	}

	log.Printf("[APPLY] Error: could not parse command: %s", cmd)
}

func executeOp(command map[string]string, store *storage.TieredStore) {
	switch command["op"] {
	case "PUT":
		if err := store.Put(command["key"], []byte(command["value"])); err != nil {
			log.Printf("[APPLY] PUT error: %v", err)
		}
	case "DELETE":
		if err := store.Delete(command["key"]); err != nil {
			log.Printf("[APPLY] DELETE error: %v", err)
		}
	}
}

// ShardingServer wraps the transport server to enforce sharding logic.
type ShardingServer struct {
	*transport.Server
	Ring *cluster.Ring
	ID   string
}