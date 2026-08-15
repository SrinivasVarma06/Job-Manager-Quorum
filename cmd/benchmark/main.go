package main

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"quorum/internal/benchmark"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})))

	cfg := benchmark.BenchmarkConfig{
		NumJobs:    1000,
		NumWorkers: 10,
		QueueSize:  2000,
		JobType:    "benchmark",
	}

	result, err := benchmark.RunBenchmark(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", err)
		os.Exit(1)
	}

	printResult(result)

	if err := os.MkdirAll("benchmarks", 0755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create benchmarks directory: %v\n", err)
		os.Exit(1)
	}

	resultsCSV := filepath.Join("benchmarks", "results.csv")
	if err := appendResultCSV(resultsCSV, result); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write results.csv: %v\n", err)
		os.Exit(1)
	}

	resultsMD := filepath.Join("benchmarks", "results.md")
	if err := generateMarkdownReport(resultsCSV, resultsMD); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write results.md: %v\n", err)
		os.Exit(1)
	}

	scalingCSV := filepath.Join("benchmarks", "scaling.csv")
	if err := runScalingMatrix(cfg, scalingCSV); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write scaling.csv: %v\n", err)
		os.Exit(1)
	}
}

func printResult(r *benchmark.BenchmarkResult) {
	fmt.Printf("Workers: %d\n", r.NumWorkers)
	fmt.Printf("Jobs: %d\n", r.NumJobs)
	fmt.Printf("Duration: %s\n", r.TotalDuration.Round(time.Millisecond))
	fmt.Printf("Throughput: %.2f jobs/sec\n", r.ThroughputJobsPerSec)
	fmt.Printf("P50: %s\n", r.P50Latency.Round(time.Millisecond))
	fmt.Printf("P95: %s\n", r.P95Latency.Round(time.Millisecond))
	fmt.Printf("P99: %s\n", r.P99Latency.Round(time.Millisecond))
	fmt.Printf("Success: %d\n", r.SuccessCount)
	fmt.Printf("Failed: %d\n", r.FailureCount)
}

func appendResultCSV(path string, r *benchmark.BenchmarkResult) error {
	writeHeader := false
	if fi, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		writeHeader = true
	} else if fi.Size() == 0 {
		writeHeader = true
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if writeHeader {
		if err := w.Write([]string{
			"workers",
			"jobs",
			"duration",
			"throughput",
			"p50",
			"p95",
			"p99",
			"success",
			"failure",
			"timestamp",
		}); err != nil {
			return err
		}
	}

	return w.Write([]string{
		strconv.Itoa(r.NumWorkers),
		strconv.Itoa(r.NumJobs),
		r.TotalDuration.String(),
		fmt.Sprintf("%.4f", r.ThroughputJobsPerSec),
		r.P50Latency.String(),
		r.P95Latency.String(),
		r.P99Latency.String(),
		strconv.Itoa(r.SuccessCount),
		strconv.Itoa(r.FailureCount),
		time.Now().Format(time.RFC3339),
	})
}

func generateMarkdownReport(csvPath, mdPath string) error {
	f, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	defer f.Close()

	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return err
	}

	var b strings.Builder
	b.WriteString("# Benchmark Results\n\n")
	b.WriteString("| Workers | Jobs | Throughput | P50 | P95 | P99 |\n")
	b.WriteString("|---:|---:|---:|---:|---:|---:|\n")

	for i, row := range records {
		if i == 0 {
			continue
		}
		if len(row) < 7 {
			continue
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %s/sec | %s | %s | %s |\n",
			row[0], row[1], row[3], row[4], row[5], row[6]))
	}

	return os.WriteFile(mdPath, []byte(b.String()), 0644)
}

func runScalingMatrix(base benchmark.BenchmarkConfig, outputCSV string) error {
	matrixWorkers := []int{1, 5, 10, 25, 50, 100}
	f, err := os.Create(outputCSV)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{
		"workers",
		"jobs",
		"duration",
		"throughput",
		"p50",
		"p95",
		"p99",
		"success",
		"failure",
		"timestamp",
	}); err != nil {
		return err
	}

	for _, workers := range matrixWorkers {
		cfg := base
		cfg.NumWorkers = workers
		switch {
		case workers == 1 && cfg.NumJobs > 20:
			cfg.NumJobs = 20
		case workers <= 5 && cfg.NumJobs > 100:
			cfg.NumJobs = 100
		case cfg.NumJobs > 200:
			cfg.NumJobs = 200
		}
		res, runErr := benchmark.RunBenchmark(cfg)
		if runErr != nil {
			return fmt.Errorf("matrix workers=%d: %w", workers, runErr)
		}
		if err := w.Write([]string{
			strconv.Itoa(res.NumWorkers),
			strconv.Itoa(res.NumJobs),
			res.TotalDuration.String(),
			fmt.Sprintf("%.4f", res.ThroughputJobsPerSec),
			res.P50Latency.String(),
			res.P95Latency.String(),
			res.P99Latency.String(),
			strconv.Itoa(res.SuccessCount),
			strconv.Itoa(res.FailureCount),
			time.Now().Format(time.RFC3339),
		}); err != nil {
			return err
		}
	}

	return nil
}
