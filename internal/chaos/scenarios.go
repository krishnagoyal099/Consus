package chaos

import (
	"fmt"
	"time"
)

// FaultType enumerates the kinds of faults that can be injected.
type FaultType int

const (
	FaultNodeCrash        FaultType = iota // Kill a node process
	FaultNetworkPartition                  // Isolate a node from the cluster
	FaultSlowDisk                          // Inject disk latency
	FaultLeaderKill                        // Specifically kill the Raft leader
	FaultClockSkew                         // Shift system clock
	FaultCorruptPacket                     // Flip bits in network packets
)

func (f FaultType) String() string {
	return [...]string{"NodeCrash", "NetworkPartition", "SlowDisk", "LeaderKill", "ClockSkew", "CorruptPacket"}[f]
}

// NodeSelector determines which node(s) to target with a fault.
type NodeSelector int

const (
	SelectRandom   NodeSelector = iota // Random node
	SelectLeader                       // Current Raft leader
	SelectFollower                     // A follower node
	SelectMajority                     // Majority of nodes (should stop the cluster)
	SelectMinority                     // Minority of nodes (cluster should survive)
	SelectSpecific                     // Target a specific node ID
)

// Invariant represents a system property that must hold during/after chaos.
type Invariant int

const (
	InvariantLinearizability Invariant = iota // Reads reflect latest committed write
	InvariantDurability                       // Committed data survives faults
	InvariantAvailability                     // System responds within timeout
	InvariantConsistency                      // All nodes agree after healing
	InvariantNoSplitBrain                     // Only one leader per term
)

func (inv Invariant) String() string {
	return [...]string{"Linearizability", "Durability", "Availability", "Consistency", "NoSplitBrain"}[inv]
}

// FaultInjection describes a single fault to inject.
type FaultInjection struct {
	Type        FaultType
	Target      NodeSelector
	Duration    time.Duration
	Params      map[string]interface{}
	DelayBefore time.Duration
}

// Scenario describes a complete chaos test with faults and expected invariants.
type Scenario struct {
	Name        string
	Description string
	Faults      []FaultInjection
	Invariants  []Invariant
	Duration    time.Duration
	Concurrent  bool // Run faults simultaneously?
}

// Result holds the outcome of running a chaos scenario.
type Result struct {
	Scenario     string               `json:"scenario"`
	StartTime    time.Time            `json:"startTime"`
	EndTime      time.Time            `json:"endTime"`
	Passed       bool                 `json:"passed"`
	Violations   []InvariantViolation `json:"violations"`
	OpsPerformed uint64               `json:"opsPerformed"`
	OpsSucceeded uint64               `json:"opsSucceeded"`
	OpsFailed    uint64               `json:"opsFailed"`
	RecoveryTime time.Duration        `json:"recoveryTime"`
}

// InvariantViolation records when a system invariant was broken.
type InvariantViolation struct {
	Invariant   string                 `json:"invariant"`
	Description string                 `json:"description"`
	Timestamp   time.Time              `json:"timestamp"`
	Details     map[string]interface{} `json:"details,omitempty"`
}

// TestDataEntry holds a known key-value pair used for durability verification.
type TestDataEntry struct {
	Key   string
	Value string
}

// BuiltInScenarios returns the 6 pre-built chaos test scenarios.
func BuiltInScenarios() []Scenario {
	return []Scenario{
		{
			Name:        "leader-failover",
			Description: "Kill the Raft leader during active writes. Verify new leader elected within 5s and no data loss.",
			Faults: []FaultInjection{
				{Type: FaultLeaderKill, Target: SelectLeader, Duration: 30 * time.Second},
			},
			Invariants: []Invariant{InvariantDurability, InvariantLinearizability, InvariantAvailability},
			Duration:   60 * time.Second,
		},
		{
			Name:        "network-partition",
			Description: "Create a network partition isolating the leader. Verify split-brain does NOT occur.",
			Faults: []FaultInjection{
				{
					Type: FaultNetworkPartition, Target: SelectLeader, Duration: 20 * time.Second,
					Params: map[string]interface{}{"partition_type": "isolate_leader"},
				},
			},
			Invariants: []Invariant{InvariantNoSplitBrain, InvariantConsistency, InvariantDurability},
			Duration:   45 * time.Second,
		},
		{
			Name:        "rolling-restart",
			Description: "Restart nodes one at a time with continuous writes. Zero downtime expected.",
			Faults: []FaultInjection{
				{Type: FaultNodeCrash, Target: SelectSpecific, Duration: 5 * time.Second,
					Params: map[string]interface{}{"node": 0}, DelayBefore: 0},
				{Type: FaultNodeCrash, Target: SelectSpecific, Duration: 5 * time.Second,
					Params: map[string]interface{}{"node": 1}, DelayBefore: 10 * time.Second},
				{Type: FaultNodeCrash, Target: SelectSpecific, Duration: 5 * time.Second,
					Params: map[string]interface{}{"node": 2}, DelayBefore: 20 * time.Second},
			},
			Invariants: []Invariant{InvariantAvailability, InvariantDurability},
			Duration:   60 * time.Second,
			Concurrent: false,
		},
		{
			Name:        "cascading-failure",
			Description: "Kill minority of nodes, then inject slow disk on remaining. Verify graceful degradation.",
			Faults: []FaultInjection{
				{Type: FaultNodeCrash, Target: SelectMinority, Duration: 30 * time.Second},
				{Type: FaultSlowDisk, Target: SelectFollower, Duration: 20 * time.Second,
					Params: map[string]interface{}{"latency_ms": 500}, DelayBefore: 5 * time.Second},
			},
			Invariants: []Invariant{InvariantDurability, InvariantConsistency},
			Duration:   60 * time.Second,
			Concurrent: true,
		},
		{
			Name:        "slow-follower",
			Description: "One follower has 200ms disk latency. Verify it doesn't slow the cluster.",
			Faults: []FaultInjection{
				{Type: FaultSlowDisk, Target: SelectFollower, Duration: 30 * time.Second,
					Params: map[string]interface{}{"latency_ms": 200}},
			},
			Invariants: []Invariant{InvariantAvailability},
			Duration:   45 * time.Second,
		},
		{
			Name:        "byzantine-clock",
			Description: "Skew one node's clock by 30s. Verify Raft term/election logic isn't affected.",
			Faults: []FaultInjection{
				{Type: FaultClockSkew, Target: SelectFollower, Duration: 20 * time.Second,
					Params: map[string]interface{}{"skew_seconds": 30}},
			},
			Invariants: []Invariant{InvariantNoSplitBrain, InvariantConsistency},
			Duration:   40 * time.Second,
		},
	}
}

// FormatResult returns a human-readable summary of a chaos test result.
func FormatResult(r Result) string {
	status := "✅ PASSED"
	if !r.Passed {
		status = "❌ FAILED"
	}
	s := fmt.Sprintf("%s: %s (recovery: %v, ops: %d/%d)",
		status, r.Scenario, r.RecoveryTime, r.OpsSucceeded, r.OpsPerformed)
	for _, v := range r.Violations {
		s += fmt.Sprintf("\n   Violation [%s]: %s", v.Invariant, v.Description)
	}
	return s
}
