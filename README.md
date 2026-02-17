<p align="center">
  <h1 align="center">Consus</h1>
  <p align="center">A high-performance distributed key-value store with Parallel Raft consensus, adaptive sharding, tiered storage, and built-in chaos testing — all in a single Go binary.</p>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#quickstart">Quickstart</a> •
  <a href="#cli">CLI</a> •
  <a href="#go-sdk">Go SDK</a> •
  <a href="#dashboard">Dashboard</a> •
  <a href="#docker">Docker</a> •
  <a href="#architecture">Architecture</a>
</p>

---

## Features

| Feature | Description |
|---------|-------------|
| **Parallel Raft** | 16 concurrent commit lanes — 4-8x write throughput over standard Raft |
| **Adaptive Sharding** | Auto-split hot shards, auto-merge cold ones, leader rebalancing |
| **Tiered Storage** | Hot (memory) → Warm (Bitcask) → Cold (compressed) → Archive (S3) |
| **Built-in Chaos Testing** | 6 fault scenarios with invariant verification |
| **Embedded Dashboard** | Real-time cluster health, metrics, shard map at `:8080` |
| **Zero Dependencies** | Single binary, no Zookeeper/etcd/PD required |

## Quickstart

### Build from source

```bash
git clone https://github.com/krishnagoyal099/Consus.git
cd Consus
make build
```

### Run a single node

```bash
./bin/consus-server --id node1 --port 50051 --http-port 8080 --data ./data/node1
```

Open **http://localhost:8080** for the dashboard.

### Run a 3-node cluster

```bash
make dev-cluster
```

## CLI

```bash
# Install
go install github.com/krishnagoyal099/Consus/cmd/consus-cli@latest

# Store and retrieve
consus-cli put user:1001 '{"name":"Alice","age":30}'
consus-cli get user:1001
consus-cli delete user:1001

# Cluster info
consus-cli cluster status
```

## Go SDK

```bash
go get github.com/krishnagoyal099/Consus/pkg/client
```

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/krishnagoyal099/Consus/pkg/client"
)

func main() {
    c, _ := client.NewClient(client.Config{
        Endpoints: []string{"http://localhost:8080"},
        Timeout:   5 * time.Second,
    })
    defer c.Close()

    ctx := context.Background()
    c.Put(ctx, "hello", []byte("world"))

    val, _ := c.Get(ctx, "hello")
    fmt.Println(string(val)) // "world"
}
```

## Dashboard

The built-in dashboard at `:8080` shows:

- **Cluster health** with node status cards (CPU, disk, replication lag)
- **Live throughput** sparklines (writes/sec, reads/sec)
- **Latency percentiles** (P50, P99, P999)
- **Shard distribution** with hot shard alerts
- **Storage tier** visualization (hot/warm/cold/archive)
- **Chaos test** runner with one-click fault injection

## Docker

### Single command cluster

```bash
docker-compose up -d
```

Dashboards available at `localhost:8081`, `localhost:8082`, `localhost:8083`.

### Build image

```bash
docker build -t consus:latest .
```

## Kubernetes

```bash
kubectl apply -f deploy/k8s/consus-statefulset.yaml
```

Creates a 3-replica StatefulSet with persistent storage, health probes, and a LoadBalancer service.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Client Layer:  CLI  |  REST API  |  Go SDK  |  gRPC   │
├─────────────────────────────────────────────────────────┤
│  Consensus:     Parallel Raft (16 lanes)                │
│                 Adaptive Shard Manager (embedded)        │
├─────────────────────────────────────────────────────────┤
│  Storage:       HOT (sync.Map)  →  WARM (Bitcask)       │
│                 →  COLD (zlib)  →  ARCHIVE (S3)          │
├─────────────────────────────────────────────────────────┤
│  Reliability:   Built-in Chaos Engine (6 scenarios)      │
│                 Embedded Dashboard (zero deps)            │
└─────────────────────────────────────────────────────────┘
```

## Project Structure

```
consus/
├── cmd/
│   ├── server/          # Server binary
│   └── consus-cli/      # CLI client
├── pkg/client/          # Go SDK
├── api/                 # HTTP dashboard + handler
├── internal/
│   ├── raft/            # Raft + Parallel Raft
│   ├── shard/           # Adaptive shard manager
│   ├── storage/         # Tiered storage engine
│   ├── cluster/         # Consistent hash ring
│   ├── chaos/           # Chaos testing engine
│   └── transport/       # gRPC transport
├── proto/               # Protobuf definitions
├── deploy/k8s/          # Kubernetes manifests
├── Dockerfile
├── docker-compose.yml
└── Makefile
```

## Comparison

| Feature | etcd | TiKV | Cassandra | **Consus** |
|---------|------|------|-----------|------------|
| Parallel Raft | ✗ | ✗ | N/A | ✓ (16 lanes) |
| Auto-split shards | ✗ | ✓* | ✗ | ✓ (embedded) |
| Tiered storage | ✗ | ✗ | partial | ✓ (4-tier) |
| Built-in chaos | ✗ | ✗ | ✗ | ✓ (6 scenarios) |
| Single binary | ✓ | ✗ | ✗ | ✓ |

*\*TiKV requires external Placement Driver (PD)*

## License

[MIT](LICENSE)
