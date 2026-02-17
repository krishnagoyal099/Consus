.PHONY: all build clean proto run-cluster dev-cluster test docker

# Default target
all: proto build

# 1. Generate Protobuf Code
proto:
	@echo "Generating Protobuf..."
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/consus.proto

# 2. Build all binaries
build:
	@echo "Building Consus..."
	go build -o bin/consus-server.exe ./cmd/server/
	go build -o bin/consus.exe ./cmd/consus/
	@echo "  bin/consus-server.exe"
	@echo "  bin/consus.exe"
	@echo "Build complete."

# 2b. Install 'consus' CLI globally
install:
	go install ./cmd/consus/
	@echo "Installed 'consus' to GOPATH/bin"

# 3. Run a local 3-node cluster (Windows)
dev-cluster: build
	@echo "Starting 3-Node Consus Cluster..."
	@echo "Dashboards: http://localhost:8081  http://localhost:8082  http://localhost:8083"
	start /b bin\consus-server.exe --id=node1 --port=50051 --http-port=8081 --data=data/node1 --cluster="node1=localhost:50051,node2=localhost:50052,node3=localhost:50053"
	start /b bin\consus-server.exe --id=node2 --port=50052 --http-port=8082 --data=data/node2 --cluster="node1=localhost:50051,node2=localhost:50052,node3=localhost:50053"
	start /b bin\consus-server.exe --id=node3 --port=50053 --http-port=8083 --data=data/node3 --cluster="node1=localhost:50051,node2=localhost:50052,node3=localhost:50053"
	@echo "Cluster started. Use 'bin\consus-cli.exe cluster status' to check."

# Alias
run-cluster: dev-cluster

# 4. Run tests
test:
	go test ./... -v -count=1

# 5. Build Docker image
docker:
	docker build -t consus:latest .

# 6. Run Docker Compose cluster
docker-cluster:
	docker-compose up -d
	@echo "Cluster dashboards:"
	@echo "  Node 1: http://localhost:8081"
	@echo "  Node 2: http://localhost:8082"
	@echo "  Node 3: http://localhost:8083"

# 7. Clean
clean:
	rmdir /s /q bin 2>nul || true
	rmdir /s /q data 2>nul || true