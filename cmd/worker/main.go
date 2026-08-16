package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"quorum/internal/config"
	"quorum/internal/executor"
	"quorum/internal/job"
	rpcclient "quorum/internal/rpc/client"
	rpcserver "quorum/internal/rpc/server"
	"quorum/internal/runner"
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
	controllerAddr := fmt.Sprintf("localhost:%d", cfg.ControllerGRPCPort)

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
