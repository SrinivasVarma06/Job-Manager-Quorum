package benchmark

import (
	"testing"
	"time"
)

func TestSmallBenchmark(t *testing.T) {
	res, err := RunBenchmark(BenchmarkConfig{
		NumJobs:    10,
		NumWorkers: 2,
		QueueSize:  32,
		JobType:    "benchmark",
	})
	if err != nil {
		t.Fatalf("RunBenchmark error: %v", err)
	}
	if res.SuccessCount != 10 {
		t.Fatalf("expected success 10 got %d", res.SuccessCount)
	}
	if res.FailureCount != 0 {
		t.Fatalf("expected failure 0 got %d", res.FailureCount)
	}
	if res.ThroughputJobsPerSec <= 0 {
		t.Fatalf("expected throughput > 0 got %f", res.ThroughputJobsPerSec)
	}
}

func TestPercentileCalculation(t *testing.T) {
	latencies := []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
		3 * time.Millisecond,
		4 * time.Millisecond,
		5 * time.Millisecond,
	}

	if got := Percentile(latencies, 50); got != 3*time.Millisecond {
		t.Fatalf("p50 expected 3ms got %s", got)
	}
	if got := Percentile(latencies, 95); got != 5*time.Millisecond {
		t.Fatalf("p95 expected 5ms got %s", got)
	}
	if got := Percentile(latencies, 99); got != 5*time.Millisecond {
		t.Fatalf("p99 expected 5ms got %s", got)
	}
}
