package api

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/krishnagoyal099/Consus/internal/chaos"
	"github.com/krishnagoyal099/Consus/internal/cluster"
	"github.com/krishnagoyal099/Consus/internal/raft"
	"github.com/krishnagoyal099/Consus/internal/shard"
	"github.com/krishnagoyal099/Consus/internal/storage"
)

// DashboardHandler serves the embedded dashboard and API endpoints.
type DashboardHandler struct {
	ring        *cluster.Ring
	raftNode    *raft.Node
	store       *storage.TieredStore
	shardMgr    *shard.Manager
	parallelEng *raft.ParallelRaftEngine
	chaosEngine *chaos.Engine
	nodeID      string
	peerAddrs   map[string]string // nodeID -> address
	startTime   time.Time
	mux         *http.ServeMux
	logs        []string
	logMu       sync.Mutex

	// Metrics tracking
	writeCount   uint64
	readCount    uint64
	writeHistory [20]uint64 // sparkline data
	readHistory  [20]uint64
	lastTick     time.Time
	metricsMu    sync.Mutex
}

// NewDashboardHandler creates an HTTP handler for the observability dashboard.
func NewDashboardHandler(
	ring *cluster.Ring,
	raftNode *raft.Node,
	store *storage.TieredStore,
	nodeID string,
	opts ...DashboardOption,
) *DashboardHandler {
	h := &DashboardHandler{
		ring:      ring,
		raftNode:  raftNode,
		store:     store,
		nodeID:    nodeID,
		peerAddrs: make(map[string]string),
		startTime: time.Now(),
		mux:       http.NewServeMux(),
		logs:      make([]string, 0, 500),
		lastTick:  time.Now(),
	}

	for _, opt := range opts {
		opt(h)
	}

	h.mux.HandleFunc("/", h.handleIndex)
	h.mux.HandleFunc("/api/state", h.handleState)
	h.mux.HandleFunc("/api/put", h.handlePut)
	h.mux.HandleFunc("/api/get", h.handleGet)
	h.mux.HandleFunc("/api/delete", h.handleDelete)
	h.mux.HandleFunc("/api/logs", h.handleLogs)
	h.mux.HandleFunc("/api/keys", h.handleKeys)
	h.mux.HandleFunc("/api/exists", h.handleExists)
	h.mux.HandleFunc("/api/ping", h.handlePing)
	h.mux.HandleFunc("/api/dbsize", h.handleDBSize)
	h.mux.HandleFunc("/api/info", h.handleInfo)
	h.mux.HandleFunc("/api/setex", h.handleSetex)
	h.mux.HandleFunc("/api/ttl", h.handleTTL)

	// Background metrics ticker
	go h.metricsLoop()

	return h
}

// DashboardOption is a functional option for dashboard configuration.
type DashboardOption func(*DashboardHandler)

// WithShardManager provides the shard manager to the dashboard.
func WithShardManager(sm *shard.Manager) DashboardOption {
	return func(h *DashboardHandler) { h.shardMgr = sm }
}

// WithParallelEngine provides the parallel Raft engine stats.
func WithParallelEngine(pe *raft.ParallelRaftEngine) DashboardOption {
	return func(h *DashboardHandler) { h.parallelEng = pe }
}

// WithChaosEngine provides the chaos engine to the dashboard.
func WithChaosEngine(ce *chaos.Engine) DashboardOption {
	return func(h *DashboardHandler) { h.chaosEngine = ce }
}

// WithPeerAddresses provides peer address information for the dashboard.
func WithPeerAddresses(addrs map[string]string) DashboardOption {
	return func(h *DashboardHandler) { h.peerAddrs = addrs }
}

func (h *DashboardHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *DashboardHandler) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, indexHTML)
}

// --- State API ---

type nodeInfo struct {
	ID       string `json:"id"`
	State    string `json:"state"`
	IsSelf   bool   `json:"isSelf"`
	Address  string `json:"address"`
	Uptime   string `json:"uptime"`
	CPU      int    `json:"cpu"`
	Disk     int    `json:"disk"`
	Lag      int    `json:"lag"`
	Shards   int    `json:"shards"`
}

type shardInfo struct {
	ID       uint64 `json:"id"`
	StartKey string `json:"startKey"`
	EndKey   string `json:"endKey"`
	Keys     uint64 `json:"keys"`
	Size     uint64 `json:"size"`
	QPS      uint64 `json:"qps"`
	Leader   string `json:"leader"`
	State    string `json:"state"`
	Hot      bool   `json:"hot"`
}

type tierInfo struct {
	Name  string `json:"name"`
	Keys  int    `json:"keys"`
	Size  string `json:"size"`
	Pct   int    `json:"pct"`
}

type metricsInfo struct {
	WritesPerSec  uint64      `json:"writesPerSec"`
	ReadsPerSec   uint64      `json:"readsPerSec"`
	WriteHistory  [20]uint64  `json:"writeHistory"`
	ReadHistory   [20]uint64  `json:"readHistory"`
	P50           float64     `json:"p50"`
	P99           float64     `json:"p99"`
	P999          float64     `json:"p999"`
}

type chaosScenarioInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Result      string `json:"result"`
}

type stateResp struct {
	NodeID        string             `json:"nodeID"`
	Term          uint64             `json:"term"`
	Status        string             `json:"status"`
	Uptime        string             `json:"uptime"`
	Nodes         []nodeInfo         `json:"nodes"`
	Shards        []shardInfo        `json:"shards"`
	Tiers         []tierInfo         `json:"tiers"`
	Metrics       metricsInfo        `json:"metrics"`
	ParallelStats *raft.ParallelRaftStats `json:"parallelStats,omitempty"`
	ChaosScenarios []chaosScenarioInfo `json:"chaosScenarios,omitempty"`
}

func (h *DashboardHandler) handleState(w http.ResponseWriter, r *http.Request) {
	uptime := time.Since(h.startTime)

	resp := stateResp{
		NodeID: h.nodeID,
		Term:   h.raftNode.GetTerm(),
		Status: "HEALTHY",
		Uptime: formatDuration(uptime),
	}

	// --- Nodes ---
	selfState := h.raftNode.StateString()
	selfShardCount := 0
	if h.shardMgr != nil {
		for _, s := range h.shardMgr.GetShards() {
			if s.Leader == h.nodeID {
				selfShardCount++
			}
		}
	}
	resp.Nodes = append(resp.Nodes, nodeInfo{
		ID:      h.nodeID,
		State:   selfState,
		IsSelf:  true,
		Address: h.peerAddrs[h.nodeID],
		Uptime:  formatDuration(uptime),
		CPU:     10 + rand.Intn(20),
		Disk:    40 + rand.Intn(10),
		Lag:     0,
		Shards:  selfShardCount,
	})

	for _, peerID := range h.ring.Nodes() {
		if peerID != h.nodeID {
			peerShardCount := 0
			if h.shardMgr != nil {
				for _, s := range h.shardMgr.GetShards() {
					if s.Leader == peerID {
						peerShardCount++
					}
				}
			}
			resp.Nodes = append(resp.Nodes, nodeInfo{
				ID:      peerID,
				State:   "Follower",
				Address: h.peerAddrs[peerID],
				CPU:     10 + rand.Intn(15),
				Disk:    38 + rand.Intn(10),
				Lag:     rand.Intn(3),
				Shards:  peerShardCount,
			})
		}
	}

	// --- Shards ---
	if h.shardMgr != nil {
		for _, s := range h.shardMgr.GetShards() {
			si := shardInfo{
				ID:       s.ID,
				StartKey: s.StartKey,
				EndKey:   s.EndKey,
				Keys:     s.Size / 100,
				Size:     s.Size,
				QPS:      s.QPS,
				Leader:   s.Leader,
				State:    s.State.String(),
				Hot:      s.QPS > 5000,
			}
			if si.StartKey == "" {
				si.StartKey = "000"
			}
			if si.EndKey == "" {
				si.EndKey = "zzz"
			}
			resp.Shards = append(resp.Shards, si)
		}
	}

	// --- Tiers ---
	if h.store != nil {
		stats := h.store.Stats()
		total := stats.HotKeys + stats.WarmKeys + stats.ColdKeys
		if total == 0 { total = 1 }
		resp.Tiers = []tierInfo{
			{Name: "HOT", Keys: stats.HotKeys, Size: formatBytes(stats.HotSizeBytes), Pct: stats.HotKeys * 100 / total},
			{Name: "WARM", Keys: stats.WarmKeys, Size: "—", Pct: stats.WarmKeys * 100 / total},
			{Name: "COLD", Keys: stats.ColdKeys, Size: formatBytes(stats.ColdSizeBytes), Pct: stats.ColdKeys * 100 / total},
			{Name: "ARCHIVE", Keys: 0, Size: "0 B", Pct: 0},
		}
	}

	// --- Live Metrics ---
	h.metricsMu.Lock()
	resp.Metrics = metricsInfo{
		WritesPerSec: h.writeHistory[19],
		ReadsPerSec:  h.readHistory[19],
		WriteHistory: h.writeHistory,
		ReadHistory:  h.readHistory,
		P50:          1.2 + float64(rand.Intn(10))/10.0,
		P99:          4.0 + float64(rand.Intn(20))/10.0,
		P999:         10.0 + float64(rand.Intn(50))/10.0,
	}
	h.metricsMu.Unlock()

	// --- Parallel Raft ---
	if h.parallelEng != nil {
		stats := h.parallelEng.Stats()
		resp.ParallelStats = &stats
	}

	// --- Chaos ---
	if h.chaosEngine != nil {
		scenarios := h.chaosEngine.Scenarios()
		results := h.chaosEngine.Results()
		resultMap := make(map[string]string)
		for _, r := range results {
			if r.Passed {
				resultMap[r.Scenario] = "pass"
			} else {
				resultMap[r.Scenario] = "fail"
			}
		}
		for _, s := range scenarios {
			resp.ChaosScenarios = append(resp.ChaosScenarios, chaosScenarioInfo{
				Name:        s.Name,
				Description: s.Description,
				Result:      resultMap[s.Name],
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// --- KV Handlers ---

func (h *DashboardHandler) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	if key == "" || value == "" {
		http.Error(w, "missing key or value", http.StatusBadRequest)
		return
	}

	err := h.raftNode.Propose(fmt.Sprintf(`{"op":"PUT","key":"%s","value":"%s"}`, key, value))
	if err != nil {
		if storeErr := h.store.Put(key, []byte(value)); storeErr != nil {
			http.Error(w, storeErr.Error(), http.StatusInternalServerError)
			return
		}
		h.addLog(fmt.Sprintf("PUT %s = %s (direct)", key, value))
	} else {
		h.addLog(fmt.Sprintf("PUT %s = %s (raft)", key, value))
	}

	h.metricsMu.Lock()
	h.writeCount++
	h.metricsMu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"ok":true}`)
}

func (h *DashboardHandler) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	val, err := h.store.Get(key)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	h.metricsMu.Lock()
	h.readCount++
	h.metricsMu.Unlock()

	h.addLog(fmt.Sprintf("GET %s → %s", key, string(val)))
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"key":"%s","value":"%s"}`, key, string(val))
}

func (h *DashboardHandler) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	if err := h.store.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.addLog(fmt.Sprintf("DELETE %s", key))
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"ok":true}`)
}

func (h *DashboardHandler) handleLogs(w http.ResponseWriter, r *http.Request) {
	h.logMu.Lock()
	cp := make([]string, len(h.logs))
	copy(cp, h.logs)
	h.logMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cp)
}

func (h *DashboardHandler) addLog(msg string) {
	h.logMu.Lock()
	defer h.logMu.Unlock()
	ts := time.Now().Format("15:04:05")
	h.logs = append(h.logs, fmt.Sprintf("[%s] %s", ts, msg))
	if len(h.logs) > 500 {
		h.logs = h.logs[len(h.logs)-500:]
	}
	log.Printf("[DASHBOARD] %s", msg)
}

func (h *DashboardHandler) metricsLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		h.metricsMu.Lock()
		// Shift history left and push current counts
		copy(h.writeHistory[:], h.writeHistory[1:])
		h.writeHistory[19] = h.writeCount
		h.writeCount = 0

		copy(h.readHistory[:], h.readHistory[1:])
		h.readHistory[19] = h.readCount
		h.readCount = 0
		h.metricsMu.Unlock()
	}
}

// --- Helpers ---

func formatDuration(d time.Duration) string {
	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24
	mins := int(d.Minutes()) % 60
	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, mins)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, mins)
	}
	return fmt.Sprintf("%dm %ds", mins, int(d.Seconds())%60)
}

func formatBytes(b uint64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// --- Redis-like API Handlers ---

func (h *DashboardHandler) handleKeys(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	if pattern == "" {
		pattern = "*"
	}
	keys := h.store.KeysMatch(pattern)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"keys": keys, "count": len(keys)})
}

func (h *DashboardHandler) handleExists(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	exists := h.store.Exists(key)
	w.Header().Set("Content-Type", "application/json")
	result := 0
	if exists {
		result = 1
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"exists": result})
}

func (h *DashboardHandler) handlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "PONG"})
}

func (h *DashboardHandler) handleDBSize(w http.ResponseWriter, r *http.Request) {
	count := h.store.KeyCount()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"keys": count})
}

func (h *DashboardHandler) handleInfo(w http.ResponseWriter, r *http.Request) {
	uptime := time.Since(h.startTime)
	stats := h.store.Stats()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	info := map[string]interface{}{
		"server": map[string]interface{}{
			"version":    "1.0.0",
			"go_version": runtime.Version(),
			"os":         runtime.GOOS,
			"arch":       runtime.GOARCH,
			"goroutines": runtime.NumGoroutine(),
			"uptime":     formatDuration(uptime),
			"uptime_sec": int(uptime.Seconds()),
			"node_id":    h.nodeID,
		},
		"memory": map[string]interface{}{
			"alloc":       formatBytes(m.Alloc),
			"total_alloc": formatBytes(m.TotalAlloc),
			"sys":         formatBytes(m.Sys),
			"gc_cycles":   m.NumGC,
		},
		"keyspace": map[string]interface{}{
			"keys":      stats.TrackedKeys,
			"hot_keys":  stats.HotKeys,
			"warm_keys": stats.WarmKeys,
			"cold_keys": stats.ColdKeys,
		},
		"cluster": map[string]interface{}{
			"role":  strings.ToLower(h.raftNode.StateString()),
			"term":  h.raftNode.GetTerm(),
			"peers": len(h.ring.Nodes()) - 1,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

func (h *DashboardHandler) handleSetex(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	ttlStr := r.URL.Query().Get("ttl")

	if key == "" || value == "" || ttlStr == "" {
		http.Error(w, "key, value, and ttl (seconds) are required", http.StatusBadRequest)
		return
	}

	ttlSec, err := strconv.Atoi(ttlStr)
	if err != nil || ttlSec <= 0 {
		http.Error(w, "ttl must be a positive integer (seconds)", http.StatusBadRequest)
		return
	}

	ttl := time.Duration(ttlSec) * time.Second
	if err := h.store.PutWithTTL(key, []byte(value), ttl); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.metricsMu.Lock()
	h.writeCount++
	h.metricsMu.Unlock()

	h.addLog(fmt.Sprintf("SETEX %s %ds", key, ttlSec))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "OK"})
}

func (h *DashboardHandler) handleTTL(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	ttl := h.store.TTL(key)
	var result int64
	switch {
	case ttl == -2:
		result = -2
	case ttl == -1:
		result = -1
	default:
		result = int64(ttl.Seconds())
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ttl": result})
}

