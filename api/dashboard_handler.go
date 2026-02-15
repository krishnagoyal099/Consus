package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/consus/consus/internal/cluster"
	"github.com/consus/consus/internal/raft"
	"github.com/consus/consus/internal/storage"
)

// DashboardHandler serves the frontend and API.
type DashboardHandler struct {
    Ring  *cluster.Ring
    Raft  *raft.Node
    Store *storage.Bitcask
    ID    string

    // Simple log buffer for the UI
    logs []string
    mu   sync.Mutex
}

// NewDashboardHandler initializes the handler.
func NewDashboardHandler(ring *cluster.Ring, r *raft.Node, s *storage.Bitcask, id string) *DashboardHandler {
    return &DashboardHandler{
        Ring:  ring,
        Raft:  r,
        Store: s,
        ID:    id,
        logs:  make([]string, 0, 100),
    }
}

// ServeHTTP routes requests to the index.html or API.
func (h *DashboardHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if r.URL.Path == "/" {
        h.serveIndex(w)
    } else if r.URL.Path == "/api/state" {
        h.handleState(w)
    } else if r.URL.Path == "/api/put" {
        h.handlePut(w, r)
    } else if r.URL.Path == "/api/get" {
        h.handleGet(w, r)
    } else if r.URL.Path == "/api/logs" {
        h.handleLogs(w)
    } else {
        http.NotFound(w, r)
    }
}

// serveIndex returns the HTML file.
func (h *DashboardHandler) serveIndex(w http.ResponseWriter) {
    w.Header().Set("Content-Type", "text/html")
    w.WriteHeader(http.StatusOK)
    io.WriteString(w, indexHTML)
}

// logEvent adds a timestamped message to the log buffer.
func (h *DashboardHandler) logEvent(msg string) {
    h.mu.Lock()
    defer h.mu.Unlock()
    t := time.Now().Format("15:04:05")
    entry := fmt.Sprintf("[%s] %s", t, msg)
    h.logs = append(h.logs, entry)
    // Keep only last 50 logs
    if len(h.logs) > 50 {
        h.logs = h.logs[1:]
    }
}

// handleState returns the current cluster topology and leadership status.
func (h *DashboardHandler) handleState(w http.ResponseWriter) {
    // In a real system, we'd query peers for their state. 
    // Here, we simulate it based on the known ring and local state.
    nodes := make([]map[string]interface{}, 0)
    
    // We iterate over the ring's known nodes (conceptually).
    // Since we don't have a full list exposed in Ring easily, we mock the list based on ID convention.
    // Assume 3 nodes for the demo: node1, node2, node3.
    
    knownIDs := []string{"node1", "node2", "node3"} 
    
    leaderID := h.Raft.LeaderAddr()
    if leaderID == "" && h.Raft.IsLeader() {
        leaderID = h.ID
    }

    for _, id := range knownIDs {
        isLeader := (id == leaderID)
        state := "Follower"
        if isLeader {
            state = "LEADER"
        }
        
        // If this is the current node, we know the state for sure. 
        // For remote nodes, we assume Follower unless they are the known Leader.
        if id == h.ID {
            state = h.Raft.StateString() // Get actual local state
        }

        nodes = append(nodes, map[string]interface{}{
            "id":      id,
            "state":   state,
            "isSelf":  (id == h.ID),
        })
    }

    resp := map[string]interface{}{
        "nodes": nodes,
        "term":  h.Raft.GetTerm(),
    }
    json.NewEncoder(w).Encode(resp)
}

// handlePut processes a write request from the dashboard.
func (h *DashboardHandler) handlePut(w http.ResponseWriter, r *http.Request) {
    key := r.URL.Query().Get("key")
    val := r.URL.Query().Get("value")
    
    if key == "" || val == "" {
        http.Error(w, "missing key or value", 400)
        return
    }

    h.logEvent(fmt.Sprintf("Received PUT '%s' via Dashboard", key))

    // Dashboard writes directly to local store for demo visibility.
    // In production, this would go through the gRPC → Raft → Apply pipeline.
    if err := h.Store.Put(key, []byte(val)); err != nil {
        h.logEvent(fmt.Sprintf("Error writing '%s': %v", key, err))
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    h.logEvent(fmt.Sprintf("Committed '%s' = '%s'", key, val))
    
    w.WriteHeader(http.StatusOK)
    fmt.Fprintf(w, "OK")
}

// handleGet processes a read request.
func (h *DashboardHandler) handleGet(w http.ResponseWriter, r *http.Request) {
    key := r.URL.Query().Get("key")
    h.logEvent(fmt.Sprintf("Received GET '%s'", key))
    
    val, err := h.Store.Get(key)
    if err != nil {
        h.logEvent(fmt.Sprintf("Get '%s': Not Found", key))
        http.Error(w, "Not Found", 404)
        return
    }
    
    h.logEvent(fmt.Sprintf("Get '%s': Found", key))
    w.Write(val)
}

// handleLogs streams the event log.
func (h *DashboardHandler) handleLogs(w http.ResponseWriter) {
    h.mu.Lock()
    copyLogs := make([]string, len(h.logs))
    copy(copyLogs, h.logs)
    h.mu.Unlock()

    json.NewEncoder(w).Encode(copyLogs)
}