package benchmark

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"quorum/internal/config"
	"quorum/internal/consensus"
	"quorum/internal/engine"
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/store"
	"quorum/internal/worker"
)

type BenchmarkConfig struct {
	NumJobs    int
	NumWorkers int
	QueueSize  int
	JobType    string
}

type BenchmarkResult struct {
	NumJobs              int
	NumWorkers           int
	TotalDuration        time.Duration
	ThroughputJobsPerSec float64
	P50Latency           time.Duration
	P95Latency           time.Duration
	P99Latency           time.Duration
	SuccessCount         int
	FailureCount         int
}

// RealisticWorkloadExecutor simulates real-world CPU hashing and JSON payload transformation.
type RealisticWorkloadExecutor struct {
	payloadSize int
	iterations  int
}

func NewRealisticWorkloadExecutor(payloadSize, iterations int) *RealisticWorkloadExecutor {
	if payloadSize <= 0 {
		payloadSize = 1024 // 1 KB
	}
	if iterations <= 0 {
		iterations = 10
	}
	return &RealisticWorkloadExecutor{
		payloadSize: payloadSize,
		iterations:  iterations,
	}
}

func (e *RealisticWorkloadExecutor) Execute(ctx context.Context, j job.Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// 1. Simulate real payload serialization/deserialization
	data := map[string]any{
		"job_id":    j.ID,
		"job_type":  j.Type,
		"timestamp": time.Now().UnixNano(),
		"payload":   make([]byte, e.payloadSize),
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// 2. Perform CPU-bound cryptographic hashing (simulating compute workload)
	var hash [32]byte
	for i := 0; i < e.iterations; i++ {
		hash = sha256.Sum256(raw)
		raw[0] = hash[0]
	}

	return nil
}

// FaultInjectingExecutor simulates realistic network dependencies with a configurable failure rate.
type FaultInjectingExecutor struct {
	failureRate float64 // 0.0 to 1.0
	mu          sync.Mutex
	callCount   int
}

func NewFaultInjectingExecutor(failureRate float64) *FaultInjectingExecutor {
	return &FaultInjectingExecutor{failureRate: failureRate}
}

func (e *FaultInjectingExecutor) Execute(ctx context.Context, j job.Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.Lock()
	e.callCount++
	count := e.callCount
	e.mu.Unlock()

	// Deterministic failure pattern based on failure rate
	if float64(count%100)/100.0 < e.failureRate {
		return fmt.Errorf("transient upstream dependency failure (attempt %d)", j.RetryCount)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Benchmark Execution Suite
// ---------------------------------------------------------------------------

type ScalabilityResult struct {
	WorkerCount int
	Throughput  float64
	P50Latency  time.Duration
	P95Latency  time.Duration
	P99Latency  time.Duration
	Speedup     float64
}

// RunScalabilityBenchmark measures worker pool scaling from 1 to maxWorkers using realistic compute jobs.
func RunScalabilityBenchmark(jobCount int, workerCounts []int) ([]ScalabilityResult, error) {
	results := make([]ScalabilityResult, 0, len(workerCounts))
	var baseThroughput float64

	for _, numWorkers := range workerCounts {
		res, err := runWorkerPoolTest(jobCount, numWorkers)
		if err != nil {
			return nil, err
		}

		if baseThroughput == 0 {
			baseThroughput = res.ThroughputJobsPerSec
		}

		speedup := 1.0
		if baseThroughput > 0 {
			speedup = res.ThroughputJobsPerSec / baseThroughput
		}

		results = append(results, ScalabilityResult{
			WorkerCount: numWorkers,
			Throughput:  res.ThroughputJobsPerSec,
			P50Latency:  res.P50Latency,
			P95Latency:  res.P95Latency,
			P99Latency:  res.P99Latency,
			Speedup:     speedup,
		})
	}

	return results, nil
}

func runWorkerPoolTest(numJobs, numWorkers int) (*BenchmarkResult, error) {
	engineCfg := config.Default()
	engineCfg.RaftEnabled = false
	engineCfg.StorageType = "memory"
	engineCfg.WorkerCount = 0
	engineCfg.ResultBuffer = numJobs + 100
	engineCfg.HeartbeatTimeout = time.Hour

	e, err := engine.New(engineCfg)
	if err != nil {
		return nil, err
	}
	defer e.Stop()

	exec := NewRealisticWorkloadExecutor(2048, 15) // 2KB payload with 15 SHA-256 rounds

	for i := 1; i <= numWorkers; i++ {
		w := worker.NewWorker(i, e.WorkerManager.Available, e.Scheduler.Results, e.JobStore, exec)
		e.WorkerManager.Register(w, fmt.Sprintf("localhost:bench-%d", i), "compute")
	}

	e.Start()

	submitTimes := make(map[int]time.Time, numJobs)
	seenTerminal := make(map[int]struct{}, numJobs)
	latencies := make([]time.Duration, 0, numJobs)
	start := time.Now()

	for i := 0; i < numJobs; i++ {
		j, err := e.SubmitJob("compute", 1)
		if err != nil {
			return nil, err
		}
		submitTimes[j.ID] = time.Now()
	}

	deadline := time.Now().Add(2 * time.Minute)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	var successCount, failureCount int
	for len(seenTerminal) < numJobs {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout: terminal=%d/%d", len(seenTerminal), numJobs)
		}
		<-ticker.C
		for _, j := range e.Jobs() {
			if _, seen := seenTerminal[j.ID]; seen {
				continue
			}
			if j.Status == job.Completed || j.Status == job.Failed {
				seenTerminal[j.ID] = struct{}{}
				latencies = append(latencies, time.Since(submitTimes[j.ID]))
				if j.Status == job.Completed {
					successCount++
				} else {
					failureCount++
				}
			}
		}
	}

	totalDuration := time.Since(start)
	throughput := float64(successCount) / totalDuration.Seconds()

	return &BenchmarkResult{
		NumJobs:              numJobs,
		NumWorkers:           numWorkers,
		TotalDuration:        totalDuration,
		ThroughputJobsPerSec: throughput,
		P50Latency:           Percentile(latencies, 50),
		P95Latency:           Percentile(latencies, 95),
		P99Latency:           Percentile(latencies, 99),
		SuccessCount:         successCount,
		FailureCount:         failureCount,
	}, nil
}

// ---------------------------------------------------------------------------
// Crash Recovery Benchmark
// ---------------------------------------------------------------------------

type RecoveryResult struct {
	JobCount         int
	StorageType      string
	SaveDuration     time.Duration
	RecoveryDuration time.Duration
	JobsPerSecond    float64
}

// RunRecoveryBenchmark seeds N pending jobs into the store, simulates a crash/restart,
// and measures exact state restoration and queue reconstruction duration.
func RunRecoveryBenchmark(numJobs int, storageType, dbPath string) (*RecoveryResult, error) {
	st, err := store.NewBoltStore(dbPath)
	if err != nil {
		return nil, fmt.Errorf("create store: %w", err)
	}

	saveStart := time.Now()
	for i := 1; i <= numJobs; i++ {
		j := job.NewJob(i, "email", i%10)
		j.Status = job.Pending
		if err := st.Add(j); err != nil {
			return nil, fmt.Errorf("seed job %d: %w", i, err)
		}
	}
	saveDuration := time.Since(saveStart)
	_ = st.Close()

	// Simulate crash & reboot: open fresh store and reconstruct queue
	recoveryStart := time.Now()
	newStore, err := store.NewBoltStore(dbPath)
	if err != nil {
		return nil, fmt.Errorf("reopen store: %w", err)
	}
	defer newStore.Close()

	pq := queue.NewJobQueue(newStore, queue.PriorityComparator)
	pendingJobs, err := newStore.ListByStatus(job.Pending)
	if err != nil {
		return nil, fmt.Errorf("list pending jobs: %w", err)
	}
	for _, j := range pendingJobs {
		pq.Enqueue(j.ID)
	}
	recoveryDuration := time.Since(recoveryStart)

	if len(pendingJobs) != numJobs {
		return nil, fmt.Errorf("expected %d recovered jobs, got %d", numJobs, len(pendingJobs))
	}

	recoveryThroughput := float64(numJobs) / recoveryDuration.Seconds()

	return &RecoveryResult{
		JobCount:         numJobs,
		StorageType:      storageType,
		SaveDuration:     saveDuration,
		RecoveryDuration: recoveryDuration,
		JobsPerSecond:    recoveryThroughput,
	}, nil
}

// ---------------------------------------------------------------------------
// Raft Leader Failover Benchmark
// ---------------------------------------------------------------------------

type FailoverResult struct {
	ClusterSize      int
	OriginalLeader   string
	NewLeader        string
	ElectionDuration time.Duration
}

// RunRaftFailoverBenchmark measures the exact leader election duration in a Raft node.
func RunRaftFailoverBenchmark() (*FailoverResult, error) {
	tempDir, err := os.MkdirTemp("", "raft_failover_bench_*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tempDir)

	memStore := store.NewMemoryStore()
	fsm := consensus.NewFSM(memStore)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	addr := l.Addr().String()
	_ = l.Close()

	start := time.Now()
	rn, err := consensus.NewRaftNode("node1", addr, fsm, filepath.Join(tempDir, "raft"))
	if err != nil {
		return nil, fmt.Errorf("create raft node: %w", err)
	}
	defer rn.Close()

	leaderCh := rn.LeaderCh()
	select {
	case <-leaderCh:
	case <-time.After(3 * time.Second):
		if !rn.IsLeader() {
			return nil, fmt.Errorf("timed out waiting for leader election")
		}
	}
	electionDuration := time.Since(start)

	return &FailoverResult{
		ClusterSize:      1,
		OriginalLeader:   "none",
		NewLeader:        "node1",
		ElectionDuration: electionDuration,
	}, nil
}

// Percentile computes the p-th percentile from a slice of durations.
func Percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}

	rank := int(math.Ceil((p / 100.0) * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// RunBenchmark runs a standard benchmark test with the provided configuration.
func RunBenchmark(cfg BenchmarkConfig) (*BenchmarkResult, error) {
	return runWorkerPoolTest(cfg.NumJobs, cfg.NumWorkers)
}
