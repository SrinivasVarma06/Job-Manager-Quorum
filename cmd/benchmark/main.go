package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"quorum/internal/benchmark"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	})))

	fmt.Println("================================================================================")
	fmt.Println("             QUORUM DISTRIBUTED ENGINE — SYSTEM BENCHMARK SUITE                 ")
	fmt.Println("================================================================================")

	// ---------------------------------------------------------------------------
	// Experiment 1: 10,000-Job Crash Recovery Benchmark
	// ---------------------------------------------------------------------------
	fmt.Println("\n[Experiment 1] 10,000-Job Persistent Crash Recovery & State Reconstruction")
	tempDir, err := os.MkdirTemp("", "quorum_bench_recovery_*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "recovery_bench.db")
	recRes, err := benchmark.RunRecoveryBenchmark(10000, "bolt", dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Recovery benchmark failed: %v\n", err)
	} else {
		fmt.Printf("  • Total Jobs Persisted:   %d jobs\n", recRes.JobCount)
		fmt.Printf("  • Initial Storage Seed:   %s\n", recRes.SaveDuration.Round(time.Millisecond))
		fmt.Printf("  • State Recovery & Queue: %s\n", recRes.RecoveryDuration.Round(time.Microsecond))
		fmt.Printf("  • Recovery Throughput:    %.2f jobs/sec\n", recRes.JobsPerSecond)
	}

	// ---------------------------------------------------------------------------
	// Experiment 2: Worker Pool Scalability Matrix (1 -> 2 -> 4 -> 8 -> 16 workers)
	// ---------------------------------------------------------------------------
	fmt.Println("\n[Experiment 2] Worker Pool Scalability with Realistic CPU Workload (2KB / 15 SHA-256 rounds)")
	workerMatrix := []int{1, 2, 4, 8, 16}
	jobsPerRun := 500
	scaleResults, err := benchmark.RunScalabilityBenchmark(jobsPerRun, workerMatrix)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Scalability benchmark failed: %v\n", err)
	} else {
		fmt.Printf("  %-10s | %-16s | %-12s | %-12s | %-10s\n", "Workers", "Throughput", "p50 Latency", "p95 Latency", "Speedup")
		fmt.Println("  -----------+------------------+--------------+--------------+-----------")
		for _, sr := range scaleResults {
			fmt.Printf("  %-10d | %-12.2f ops/s | %-12s | %-12s | %-6.2fx\n",
				sr.WorkerCount,
				sr.Throughput,
				sr.P50Latency.Round(time.Millisecond),
				sr.P95Latency.Round(time.Millisecond),
				sr.Speedup,
			)
		}
	}

	// ---------------------------------------------------------------------------
	// Experiment 3: Raft Leader Failover & Sub-Second Election Latency
	// ---------------------------------------------------------------------------
	fmt.Println("\n[Experiment 3] Raft 3-Node Cluster Consensus Failover")
	failoverRes, err := benchmark.RunRaftFailoverBenchmark()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failover benchmark failed: %v\n", err)
	} else {
		fmt.Printf("  • Cluster Size:       %d nodes\n", failoverRes.ClusterSize)
		fmt.Printf("  • Terminated Leader:  %s\n", failoverRes.OriginalLeader)
		fmt.Printf("  • New Leader Elected: %s\n", failoverRes.NewLeader)
		fmt.Printf("  • Failover Duration:  %s\n", failoverRes.ElectionDuration.Round(time.Millisecond))
	}

	fmt.Println("\n================================================================================")
	fmt.Println("                         BENCHMARK SUITE COMPLETE                               ")
	fmt.Println("================================================================================")
}
