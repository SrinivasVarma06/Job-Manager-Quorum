package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"

	"quorum/internal/config"
	"quorum/internal/executor"
	"quorum/internal/job"
	"quorum/internal/metrics"
	rpcclient "quorum/internal/rpc/client"
	rpcserver "quorum/internal/rpc/server"
	"quorum/internal/runner"
	"quorum/internal/tracing"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg := config.Default()

	// Worker ID can be overridden via QUORUM_WORKER_ID environment variable.
	// This is required when running multiple worker nodes on the same config
	// so each worker has a unique identity on the control node.
	// Example: QUORUM_WORKER_ID=2 go run ./cmd/worker
	workerID := cfg.WorkerID
	if v := os.Getenv("QUORUM_WORKER_ID"); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			workerID = id
		} else {
			log.Fatalf("invalid QUORUM_WORKER_ID %q: %v", v, err)
		}
	}

	if shutdown, err := tracing.Init(fmt.Sprintf("quorum-worker-%d", workerID)); err == nil {
		defer shutdown()
	} else {
		slog.Warn("Tracing initialization failed, continuing with fallback tracer", "error", err)
	}

	// Start HTTP server for Prometheus metrics scraping.
	metricsPort := 9090
	if v := os.Getenv("QUORUM_METRICS_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			metricsPort = p
		}
	}
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(fmt.Sprintf(`{"service":"quorum-worker","worker_id":%d,"status":"running"}`, workerID)))
		})
		slog.Info("Worker metrics HTTP server listening", "port", metricsPort)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", metricsPort), mux); err != nil && err != http.ErrServerClosed {
			slog.Warn("Worker metrics HTTP server error", "error", err)
		}
	}()

	// Worker port can also be overridden via QUORUM_WORKER_PORT.
	workerPort := cfg.WorkerGRPCPort
	if v := os.Getenv("QUORUM_WORKER_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			workerPort = p
		} else {
			log.Fatalf("invalid QUORUM_WORKER_PORT %q: %v", v, err)
		}
	}

	// Worker topics can be configured via QUORUM_WORKER_TOPICS (comma-separated).
	// Default is "*" (wildcard, handles any topic).
	topics := []string{"*"}
	if v := os.Getenv("QUORUM_WORKER_TOPICS"); v != "" {
		rawTopics := strings.Split(v, ",")
		var parsed []string
		for _, t := range rawTopics {
			if trimmed := strings.TrimSpace(t); trimmed != "" {
				parsed = append(parsed, trimmed)
			}
		}
		if len(parsed) > 0 {
			topics = parsed
		}
	}

	// Local results channel owned by this worker process.
	results := make(chan job.Result, cfg.ResultBuffer)

	// Execution pipeline: MockExecutor → RateLimiter → CircuitBreaker.
	base := &executor.MockExecutor{}
	limiter := executor.NewTokenBucketLimiter(cfg.RateLimit, cfg.RateBurst)
	rateLimited := executor.NewRateLimitedExecutor(base, limiter)
	exec := executor.NewCircuitBreakerExecutor(
		rateLimited,
		cfg.BreakerFailureThreshold,
		cfg.BreakerResetTimeout,
	)

	r := runner.New(nil, results, exec)

	// gRPC server on the worker node: receives SubmitJob calls from the control node.
	grpcWorker := rpcserver.NewExecutionServer(r)

	go func() {
		if err := rpcserver.StartGRPCServer(workerPort, grpcWorker); err != nil {
			log.Fatal(err)
		}
	}()

	workerAddr := fmt.Sprintf("localhost:%d", workerPort)
	if v := os.Getenv("QUORUM_WORKER_ADDR"); v != "" {
		workerAddr = v
	}

	controllerAddr := fmt.Sprintf("localhost:%d", cfg.ControllerGRPCPort)
	if v := os.Getenv("QUORUM_CONTROLLER_ADDR"); v != "" {
		controllerAddr = v
	}

	// gRPC client: connects to the control node for registration and heartbeats.
	controller, err := rpcclient.New(workerID, workerAddr, controllerAddr, topics...)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// Register with the control node and start sending heartbeats.
	go controller.Start(ctx)

	// Forward local execution results to the control node via ReportResult RPC.
	go func() {
		for result := range results {
			errMsg := ""
			if result.Error != nil {
				errMsg = result.Error.Error()
			}
			if err := controller.ReportResult(ctx, result.JobID, result.Attempt, result.Success, errMsg); err != nil {
				slog.Error("ReportResult failed", "job_id", result.JobID, "error", err)
			}
		}
	}()

	slog.Info("Worker node started", "worker_id", workerID, "worker_addr", workerAddr, "controller_addr", controllerAddr)

	select {}
}
