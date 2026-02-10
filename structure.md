consus/
├── cmd/
│   └── server/
│       └── main.go           # Entry point: initializes Storage, Raft, gRPC, and HTTP servers.
├── internal/
│   ├── storage/
│   │   ├── bitcask.go        # Core storage engine: Put, Get, Delete.
│   │   ├── wal.go            # Append-only Write Ahead Log handling.
│   │   └── hashmap.go        # In-memory index (key -> file offset).
│   ├── raft/
│   │   ├── node.go           # Raft state machine (Follower, Candidate, Leader).
│   │   ├── election.go       # Election timeout logic and voting.
│   │   └── log_replication.go # AppendEntries RPC handling and log matching.
│   ├── cluster/
│   │   ├── ring.go           # Consistent Hashing logic (Sharding).
│   │   └── node.go           # Remote node peer representation.
│   └── transport/
│       └── grpc_server.go    # gRPC service implementation bridging Raft/Storage.
├── api/
│   └── dashboard_handler.go  # HTTP handlers for the Web UI and API endpoints.
├── proto/
│   └── consus.proto          # Protobuf definitions for KV operations and Raft RPCs.
├── web/
│   └── index.html            # Frontend dashboard (HTML/JS).
├── go.mod                    # Module definition.
├── go.sum                    # Dependency checksums.
└── Makefile                  # Build and run scripts.