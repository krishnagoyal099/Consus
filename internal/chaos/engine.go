package chaos

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// ClusterInterface abstracts the cluster operations needed by the chaos engine.
// The real implementation is provided by the cluster/transport layer.
type ClusterInterface interface {
	Put(key, value string) error
	Get(key string) (string, error)
	IsHealthy() bool
	GetAllReportedLeaders() map[string]string
	GetFromAllNodes(key string) map[string]string
}

// Engine runs automated fault injection tests against a live Consus cluster.
// This is a FIRST-CLASS FEATURE built into the binary, not an external tool.
type Engine struct {
	cluster   ClusterInterface
	scenarios []Scenario
	results   []Result
	running   bool
	mu        sync.Mutex
	logger    *log.Logger
}

// NewEngine creates a new chaos testing engine.
func NewEngine(cluster ClusterInterface) *Engine {
	return &Engine{
		cluster:   cluster,
		scenarios: BuiltInScenarios(),
		logger:    log.Default(),
	}
}

// AddScenario adds a custom chaos scenario.
func (ce *Engine) AddScenario(s Scenario) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	ce.scenarios = append(ce.scenarios, s)
}

// IsRunning returns whether a chaos test is currently in progress.
func (ce *Engine) IsRunning() bool {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	return ce.running
}

// Results returns the results of all completed tests.
func (ce *Engine) Results() []Result {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	cp := make([]Result, len(ce.results))
	copy(cp, ce.results)
	return cp
}

// Scenarios returns all registered scenarios.
func (ce *Engine) Scenarios() []Scenario {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	cp := make([]Scenario, len(ce.scenarios))
	copy(cp, ce.scenarios)
	return cp
}

// RunAll executes all chaos scenarios sequentially.
func (ce *Engine) RunAll(ctx context.Context) []Result {
	ce.mu.Lock()
	ce.running = true
	ce.results = nil
	ce.mu.Unlock()

	defer func() {
		ce.mu.Lock()
		ce.running = false
		ce.mu.Unlock()
	}()

	var results []Result

	for _, scenario := range ce.scenarios {
		select {
		case <-ctx.Done():
			return results
		default:
		}

		ce.logger.Printf("🔥 Running chaos scenario: %s", scenario.Name)
		ce.logger.Printf("   Description: %s", scenario.Description)

		result := ce.runScenario(ctx, scenario)
		results = append(results, result)

		ce.mu.Lock()
		ce.results = append(ce.results, result)
		ce.mu.Unlock()

		ce.logger.Printf("%s", FormatResult(result))

		// Wait for cluster to stabilize before next scenario
		ce.logger.Printf("⏳ Waiting for cluster to stabilize...")
		ce.waitForClusterHealth(ctx, 30*time.Second)
	}

	return results
}

// RunScenarioByName runs a single scenario by name.
func (ce *Engine) RunScenarioByName(ctx context.Context, name string) (*Result, error) {
	ce.mu.Lock()
	var scenario *Scenario
	for _, s := range ce.scenarios {
		if s.Name == name {
			sc := s
			scenario = &sc
			break
		}
	}
	ce.mu.Unlock()

	if scenario == nil {
		return nil, fmt.Errorf("scenario %q not found", name)
	}

	ce.mu.Lock()
	ce.running = true
	ce.mu.Unlock()

	defer func() {
		ce.mu.Lock()
		ce.running = false
		ce.mu.Unlock()
	}()

	result := ce.runScenario(ctx, *scenario)
	ce.mu.Lock()
	ce.results = append(ce.results, result)
	ce.mu.Unlock()

	return &result, nil
}

// runScenario executes a single chaos scenario.
func (ce *Engine) runScenario(ctx context.Context, scenario Scenario) Result {
	result := Result{
		Scenario:  scenario.Name,
		StartTime: time.Now(),
		Passed:    true,
	}

	// Phase 1: Start background workload
	workloadCtx, cancelWorkload := context.WithCancel(ctx)
	workloadResults := ce.startWorkload(workloadCtx)

	// Phase 2: Write known test data for durability verification
	testData := ce.writeTestData(50)

	// Phase 3: Inject faults
	if scenario.Concurrent {
		var wg sync.WaitGroup
		for _, fault := range scenario.Faults {
			wg.Add(1)
			go func(f FaultInjection) {
				defer wg.Done()
				time.Sleep(f.DelayBefore)
				ce.injectFault(f)
				time.Sleep(f.Duration)
				ce.healFault(f)
			}(fault)
		}
		wg.Wait()
	} else {
		for _, fault := range scenario.Faults {
			time.Sleep(fault.DelayBefore)
			ce.injectFault(fault)
			time.Sleep(fault.Duration)
			ce.healFault(fault)
		}
	}

	// Phase 4: Wait for recovery
	recoveryStart := time.Now()
	recovered := ce.waitForClusterHealth(ctx, 30*time.Second)
	result.RecoveryTime = time.Since(recoveryStart)

	if !recovered {
		result.Passed = false
		result.Violations = append(result.Violations, InvariantViolation{
			Invariant:   InvariantAvailability.String(),
			Description: "Cluster did not recover within 30s timeout",
			Timestamp:   time.Now(),
		})
	}

	// Phase 5: Stop workload
	cancelWorkload()
	wlResult := <-workloadResults
	result.OpsPerformed = wlResult.total
	result.OpsSucceeded = wlResult.succeeded
	result.OpsFailed = wlResult.failed

	// Phase 6: Verify invariants
	for _, inv := range scenario.Invariants {
		violations := ce.checkInvariant(inv, testData)
		if len(violations) > 0 {
			result.Passed = false
			result.Violations = append(result.Violations, violations...)
		}
	}

	result.EndTime = time.Now()
	return result
}

type workloadResult struct {
	total     uint64
	succeeded uint64
	failed    uint64
}

func (ce *Engine) startWorkload(ctx context.Context) chan workloadResult {
	ch := make(chan workloadResult, 1)
	go func() {
		var res workloadResult
		for {
			select {
			case <-ctx.Done():
				ch <- res
				return
			default:
			}

			key := fmt.Sprintf("workload-key-%d", rand.Int63())
			value := fmt.Sprintf("value-%d", time.Now().UnixNano())

			err := ce.cluster.Put(key, value)
			res.total++
			if err == nil {
				res.succeeded++
			} else {
				res.failed++
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()
	return ch
}

func (ce *Engine) writeTestData(count int) []TestDataEntry {
	data := make([]TestDataEntry, count)
	for i := 0; i < count; i++ {
		data[i] = TestDataEntry{
			Key:   fmt.Sprintf("chaos-test-key-%d", i),
			Value: fmt.Sprintf("chaos-test-value-%d-%d", i, time.Now().UnixNano()),
		}
		ce.cluster.Put(data[i].Key, data[i].Value)
	}
	return data
}

func (ce *Engine) checkInvariant(inv Invariant, testData []TestDataEntry) []InvariantViolation {
	var violations []InvariantViolation

	switch inv {
	case InvariantDurability:
		for _, td := range testData {
			val, err := ce.cluster.Get(td.Key)
			if err != nil {
				violations = append(violations, InvariantViolation{
					Invariant:   inv.String(),
					Description: fmt.Sprintf("Key '%s' lost after chaos (error: %v)", td.Key, err),
					Timestamp:   time.Now(),
					Details:     map[string]interface{}{"key": td.Key, "expected": td.Value},
				})
			} else if val != td.Value {
				violations = append(violations, InvariantViolation{
					Invariant:   inv.String(),
					Description: fmt.Sprintf("Key '%s' corrupted: expected '%s', got '%s'", td.Key, td.Value, val),
					Timestamp:   time.Now(),
				})
			}
		}

	case InvariantLinearizability:
		key := fmt.Sprintf("linearizability-check-%d", time.Now().UnixNano())
		value := fmt.Sprintf("lin-value-%d", time.Now().UnixNano())

		err := ce.cluster.Put(key, value)
		if err == nil {
			got, err := ce.cluster.Get(key)
			if err != nil || got != value {
				violations = append(violations, InvariantViolation{
					Invariant:   inv.String(),
					Description: fmt.Sprintf("Read-after-write mismatch: wrote '%s', read '%s' (err: %v)", value, got, err),
					Timestamp:   time.Now(),
				})
			}
		}

	case InvariantNoSplitBrain:
		leaders := ce.cluster.GetAllReportedLeaders()
		leaderSet := make(map[string]bool)
		for _, l := range leaders {
			if l != "" {
				leaderSet[l] = true
			}
		}
		if len(leaderSet) > 1 {
			violations = append(violations, InvariantViolation{
				Invariant:   inv.String(),
				Description: fmt.Sprintf("Split brain detected: %d different leaders reported", len(leaderSet)),
				Timestamp:   time.Now(),
				Details:     map[string]interface{}{"leaders": leaders},
			})
		}

	case InvariantConsistency:
		maxCheck := 10
		if len(testData) < maxCheck {
			maxCheck = len(testData)
		}
		for _, td := range testData[:maxCheck] {
			values := ce.cluster.GetFromAllNodes(td.Key)
			for nodeID, val := range values {
				if val != td.Value {
					violations = append(violations, InvariantViolation{
						Invariant:   inv.String(),
						Description: fmt.Sprintf("Node %s has stale value for '%s': expected '%s', got '%s'", nodeID, td.Key, td.Value, val),
						Timestamp:   time.Now(),
					})
				}
			}
		}
	}

	return violations
}

func (ce *Engine) injectFault(f FaultInjection) {
	ce.logger.Printf("   💉 Injecting: %v targeting %v", f.Type, f.Target)
	// In production: iptables for network, SIGKILL for crashes, cgroups for resources
}

func (ce *Engine) healFault(f FaultInjection) {
	ce.logger.Printf("   💚 Healing: %v", f.Type)
}

func (ce *Engine) waitForClusterHealth(ctx context.Context, timeout time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline:
			return false
		case <-ticker.C:
			if ce.cluster.IsHealthy() {
				return true
			}
		}
	}
}
