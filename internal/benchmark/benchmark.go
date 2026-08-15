package benchmark

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"quorum/internal/config"
	"quorum/internal/engine"
	"quorum/internal/job"
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

type benchmarkExecutor struct{}

func (e *benchmarkExecutor) Execute(ctx context.Context, _ job.Job) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(1 * time.Millisecond):
		return nil
	}
}

func RunBenchmark(cfg BenchmarkConfig) (*BenchmarkResult, error) {
	if cfg.NumJobs <= 0 {
		return nil, errors.New("NumJobs must be > 0")
	}
	if cfg.NumWorkers <= 0 {
		return nil, errors.New("NumWorkers must be > 0")
	}
	if cfg.QueueSize <= 0 {
		return nil, errors.New("QueueSize must be > 0")
	}
	if cfg.JobType == "" {
		cfg.JobType = "benchmark"
	}

	engineCfg := config.Default()
	engineCfg.RaftEnabled = false
	engineCfg.StorageType = "memory"
	engineCfg.WorkerCount = 0
	engineCfg.ResultBuffer = cfg.QueueSize
	// Benchmark workers are local in-process workers without heartbeat RPCs.
	// Keep timeout high so liveness monitor does not evict active benchmark workers.
	engineCfg.HeartbeatTimeout = time.Hour

	e, err := engine.New(engineCfg)
	if err != nil {
		return nil, fmt.Errorf("create engine: %w", err)
	}
	defer e.Stop()

	for i := 1; i <= cfg.NumWorkers; i++ {
		w := worker.NewWorker(i, e.WorkerManager.Available, e.Scheduler.Results, e.JobStore, &benchmarkExecutor{})
		e.WorkerManager.Register(w, fmt.Sprintf("localhost:bench-%d", i), cfg.JobType)
	}

	e.Start()

	submitTimes := make(map[int]time.Time, cfg.NumJobs)
	seenTerminal := make(map[int]struct{}, cfg.NumJobs)
	latencies := make([]time.Duration, 0, cfg.NumJobs)
	benchmarkStart := time.Now()

	for i := 0; i < cfg.NumJobs; i++ {
		j, submitErr := e.SubmitJob(cfg.JobType, 1)
		if submitErr != nil {
			return nil, fmt.Errorf("submit job %d: %w", i+1, submitErr)
		}
		submitTimes[j.ID] = time.Now()
	}

	var successCount, failureCount int
	deadline := time.Now().Add(2 * time.Minute)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for len(seenTerminal) < cfg.NumJobs {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("benchmark timeout: terminal=%d/%d", len(seenTerminal), cfg.NumJobs)
		}

		<-ticker.C
		for _, j := range e.Jobs() {
			if _, seen := seenTerminal[j.ID]; seen {
				continue
			}
			if j.Status != job.Completed && j.Status != job.Failed && j.Status != job.Cancelled {
				continue
			}
			submittedAt, ok := submitTimes[j.ID]
			if !ok {
				continue
			}
			seenTerminal[j.ID] = struct{}{}
			latencies = append(latencies, time.Since(submittedAt))
			if j.Status == job.Completed {
				successCount++
			} else {
				failureCount++
			}
		}
	}

	totalDuration := time.Since(benchmarkStart)
	throughput := 0.0
	if totalDuration > 0 {
		throughput = float64(successCount) / totalDuration.Seconds()
	}

	return &BenchmarkResult{
		NumJobs:              cfg.NumJobs,
		NumWorkers:           cfg.NumWorkers,
		TotalDuration:        totalDuration,
		ThroughputJobsPerSec: throughput,
		P50Latency:           Percentile(latencies, 50),
		P95Latency:           Percentile(latencies, 95),
		P99Latency:           Percentile(latencies, 99),
		SuccessCount:         successCount,
		FailureCount:         failureCount,
	}, nil
}

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
