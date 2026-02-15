.PHONY: all build clean proto run-cluster

# Default target
all: proto build

# 1. Generate Protobuf Code
proto:
	@echo "Generating Protobuf..."
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/consus.proto

# 2. Build the binary
build:
	@echo "Building Consus..."
	go build -o bin/consus cmd/server/main.go

# 3. Run a local 3-node cluster
run-cluster: build
	@echo "Starting 3-Node Cluster..."
	@echo "Open http://localhost:8081 for Dashboard"
	start /b go run cmd/server/main.go --id=node1 --port=50051 --http-port=8081 --cluster="node1=localhost:50051,node2=localhost:50052,node3=localhost:50053"
	start /b go run cmd/server/main.go --id=node2 --port=50052 --http-port=8082 --cluster="node1=localhost:50051,node2=localhost:50052,node3=localhost:50053"
	start /b go run cmd/server/main.go --id=node3 --port=50053 --http-port=8083 --cluster="node1=localhost:50051,node2=localhost:50052,node3=localhost:50053"

clean:
	rmdir /s /q bin 2>nul || true
	rmdir /s /q data 2>nul || true