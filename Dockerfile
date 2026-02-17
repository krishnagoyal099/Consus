# ─── Stage 1: Build ───
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build both binaries
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/consus-server ./cmd/server/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/consus-cli ./cmd/consus-cli/main.go

# ─── Stage 2: Runtime ───
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /bin/consus-server /usr/local/bin/consus-server
COPY --from=builder /bin/consus-cli /usr/local/bin/consus-cli

# Data directory
RUN mkdir -p /data
VOLUME ["/data"]

# gRPC + HTTP dashboard
EXPOSE 50051 8080

ENTRYPOINT ["consus-server"]
CMD ["--id=node1", "--port=50051", "--http-port=8080", "--data=/data"]
