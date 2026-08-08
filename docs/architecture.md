# Quorum Architecture (Current Codebase)

This document is the source of truth for Quorum's current implementation.

## 1) High-level flow

**Local path (unchanged):**
Client (HTTP) → Handlers → Engine → JobStore + WAL + Queues → Scheduler → WorkerManager.Available → WorkerClient → Executor → Result → Scheduler result/retry/fail handling

**Distributed path (PR #15):**
Client (HTTP) → Engine → PriorityQueue → Scheduler.dispatchLoop → RemoteWorker (proxy) → gRPC SubmitJob → Worker Node: ExecutionServer → Runner → Executor → Results chan → gRPC ReportResult → Control Node: WorkerServer.ReportResult → Scheduler.Results → resultLoop

Cron path: CronScheduler → Engine.SubmitJob → WAL + Store + PriorityQueue
Scheduled job path: POST /jobs with run_at → Engine.SubmitJobAt → WAL + Store + DelayQueue

## 2) Project structure

```text
cmd/
  server/
    server.go    control node entrypoint
  worker/
    main.go      worker node entrypoint

internal/
  broker/         topic subscription management & capability-aware worker routing
  config/         runtime config defaults
  consensus/      Raft consensus, FSM log replication, & leader election (hashicorp/raft)
  cron/           cron scheduler integrated with engine submit path
  dlq/            dead-letter queue
  engine/         application composition + lifecycle
  executor/       execution abstraction + rate limit + circuit breaker
  handlers/       HTTP handlers
  httpapi/        JSON error helper
  job/            job model + result model
  middleware/     request id + logging middleware
  queue/          heap-based queue abstraction (priority + delay)
  retry/          retry policy + backoff
  rpc/
    client/       gRPC client (worker→controller registration/heartbeat/ReportResult, controller→worker Submit)
    proto/        protobuf definitions and generated code
    proxy/        RemoteWorker — implements worker.WorkerClient over gRPC
    server/
      grpc.go           StartGRPCServer (accepts WorkerServiceServer interface)
      server.go         WorkerServer — control node RPC handler
      execution_server.go  ExecutionServer — worker node RPC handler
  runner/         job execution (used by both local Worker and ExecutionServer)
  scheduler/      dispatch/result/delay/recovery loops
  storage/        WAL + snapshot storage
  store/          Store interface + MemoryStore (in-memory) + BoltStore (bbolt ACID disk DB)
  workermanager/  worker registry + availability channel + MakeAvailable
  worker/         local worker lifecycle (WorkerClient interface + Worker impl)
```

## 3) Package responsibilities, API, dependencies

### `internal/broker`
- **Purpose:** worker topic capability registration & capability-aware job selection.
- **Public API:** `New`, `RegisterWorker`, `UnregisterWorker`, `CanHandle`, `SelectWorker`.
- **Depends on:** `job`, `worker`, `sync`, `log/slog`.

### `internal/consensus`
- **Purpose:** Raft consensus cluster management, FSM log replication, & leader election.
- **Public API:** `NewFSM`, `NewRaftNode`, `IsLeader`, `LeaderAddr`, `ProposeAddJob`, `ProposeUpdateJob`, `ProposeDeleteJob`.
- **Depends on:** `github.com/hashicorp/raft`, `job`, `store`, `log/slog`.

### `cmd/server`
- **Purpose:** control node entrypoint — boot engine, HTTP server, gRPC WorkerServer.
- **Key API:** `main()`.
- **Depends on:** `internal/engine`, `internal/handlers`, `internal/middleware`, `internal/rpc/server`, `net/http`.

### `cmd/worker`
- **Purpose:** worker node entrypoint — boot runner, gRPC ExecutionServer, gRPC client to controller.
- **Key API:** `main()`.
- **Depends on:** `config`, `executor`, `job`, `rpc/client`, `rpc/server`, `runner`, `store`.

### `internal/engine`
- **Purpose:** composition root and lifecycle owner for the control node.
- **Owns:** priority queue, delay queue, WAL, snapshot store, scheduler, cron scheduler, worker manager, job store, DLQ, config.
- **Public API:** `New`, `Start`, `Stop`, `Restore`, `SubmitJob`, `SubmitJobAt`, `Jobs`, `Job`, `DeleteJob`, `CancelJob`, `DeadJobs`, `AddCronJob`, `RemoveCronJob`, `ListCronJobs`.
- **Depends on:** `config`, `dlq`, `executor`, `job`, `queue`, `scheduler`, `storage`, `store`, `worker`, `workermanager`.

### `internal/rpc/server`
- **Purpose:** gRPC service implementations.
- **`WorkerServer`:** control node handler — `RegisterWorker` (dials worker back, creates `RemoteWorker`, registers + makes available), `Heartbeat`, `ReportResult` (writes result to `Scheduler.Results`, re-queues worker).
- **`ExecutionServer`:** worker node handler — `SubmitJob` only (delegates to `runner.Execute` in a goroutine).
- **`StartGRPCServer`:** accepts `workerpb.WorkerServiceServer` interface; used by both servers.

### `internal/rpc/client`
- **Purpose:** gRPC client for worker↔controller communication.
- **`New(id, workerAddr, controllerAddr)`:** worker node dialing the controller.
- **`NewOutbound(id, workerAddr)`:** control node dialing a worker.
- **Methods:** `Start` (register + heartbeat loop), `Submit` (used via proxy), `ReportResult`.

### `internal/rpc/proxy`
- **Purpose:** adapts `rpc/client.Client` to the `worker.WorkerClient` interface.
- **`RemoteWorker`:** `ID`, `Start` (no-op), `Submit` (calls `client.Submit`).

### `internal/scheduler`
- **Purpose:** moves jobs from queues to workers and processes results.
- **Loops:** `dispatchLoop`, `resultLoop`, `delayLoop`.
- **Public API:** `NewScheduler`, `Start`, `Dispatch`, `CreateSnapshot`, `ProcessDelayedJobs`.
- **Depends on:** `config`, `dlq`, `job`, `queue`, `retry`, `storage`, `store`, `worker`, `time`.

### `internal/worker`
- **Purpose:** worker abstraction + lifecycle + execution.
- **Public API:** `Client` (interface: `ID`, `Start`, `Execute`, `Submit`), `Worker` (concrete local implementation), `NewWorker`.
- **Depends on:** `job`, `store`, `executor`, `context`.

### `internal/workermanager`
- **Purpose:** registry of `worker.Client` instances and shared availability channel (`Available chan worker.Client`).
- **Public API:** `NewManager`, `Register`, `Remove`, `Get`, `Count`, `List`.
- **Depends on:** `worker`, `sync`.

### `internal/store`
- **Purpose:** persistent and in-memory job storage abstraction.
- **`Store` Interface:** `Add`, `Get`, `List`, `Update`, `Delete`, `Cancel`, `RunningJobs`.
- **`MemoryStore`:** thread-safe in-memory map store (zero disk IO).
- **`BoltStore`:** embedded bbolt ACID disk database (`quorum.db`). Saves jobs as canonical JSON key-values in the `"jobs"` bucket.


### `internal/queue`
- **Purpose:** heap-backed queue abstraction by job ID.
- **Public API:** `NewJobQueue`, `Enqueue`, `Dequeue`, `Wait`, `Peek`.
- **Comparators:** `PriorityComparator`, `DelayComparator`.
- **Depends on:** `container/heap`, `store`, `sync`.

### `internal/storage`
- **Purpose:** persistence layer for crash recovery.
- **WAL API:** `NewWal`, `Append`, `AppendRetry`, `AppendFailure`, `AppendCompletion`, `AppendCancel`, `Replay`, `Reset`, `Close`.
- **Snapshot API:** `NewSnapshot`, `Save`, `Load`.
- **Depends on:** `job`, `os`, `encoding/json`, `bufio`.

### `internal/job`
- **Purpose:** domain model.
- **Public API:** `Job`, statuses, `NewJob`, `Result`.

### `internal/retry`
- **Purpose:** retry decisions and exponential backoff.
- **Public API:** `ShouldRetry`, `Backoff`, `NextRetryTime`.

### `internal/dlq`
- **Purpose:** dead-letter storage for permanently failed jobs.
- **Public API:** `New`, `Add`, `Get`, `List`, `Delete`.

### `internal/executor`
- **Purpose:** execution pipeline and resilience wrappers.
- **Public API:** `Executor`, `MockExecutor`, `Limiter`, `TokenBucketLimiter`, `RateLimitedExecutor`, `CircuitBreakerExecutor`.
- **Notes:** engine composes `Mock -> RateLimited -> CircuitBreaker`.

### `internal/handlers`
- **Purpose:** HTTP REST handlers for jobs, cron, and cluster management.
- **Endpoints:** `POST /jobs`, `GET /jobs`, `GET /jobs/{id}`, `DELETE /jobs/{id}`, `POST /cron`, `GET /cron`, `DELETE /cron/{id}`, `GET /cluster/status`, `GET /cluster/nodes`, `DELETE /cluster/nodes/{id}`.
- **Depends on:** `engine`, `httpapi`, `job`, `workermanager`.

### `internal/middleware`
- **Purpose:** request logging and request ID response header.
- **Public API:** `Logging`, `RequestID`.

### `internal/httpapi`
- **Purpose:** helper for JSON error responses.
- **Public API:** `WriteError`.

### `internal/config`
- **Purpose:** central defaults for workers/retries/backoff/rate-limits/breaker.
- **Public API:** `Config`, `Default`.

### `internal/cron`
- **Purpose:** cron-style recurring job producer.
- **Public API:** `New`, `Add`, `Remove`, `Start`.
- **Current state:** integrated into engine lifecycle; cron ticks call `Engine.SubmitJob` through injected submit function.

## 4) Concurrency design (goroutines/channels/mutexes)

### Goroutines
- Engine starts one goroutine per worker (`worker.Start`).
- Engine starts scheduler (`scheduler.Start`), which itself starts:
  - `dispatchLoop`
  - `resultLoop`
  - `delayLoop`
- Engine starts cron scheduler (`cron.Start`).
- `TokenBucketLimiter` starts an internal refill goroutine.

### Channels
- `workermanager.Manager.Available chan *worker.Worker`: worker availability signaling.
- `worker.Worker.JobChannel chan job.Job`: scheduler -> worker dispatch.
- `scheduler.Scheduler.Results chan job.Result`: worker -> scheduler results.
- `queue.JobQueue.notify chan struct{}`: queue wake-up signaling.

### Mutex ownership
- `JobStore.mu`: protects all job map access.
- `JobQueue.mu`: protects heap operations.
- `WorkerManager.mu`: protects worker registry map.
- `DeadLetterQueue.mu`: protects DLQ map.
- `CircuitBreakerExecutor.mu`: protects breaker state transitions.
- `Engine.wg`: coordinates graceful goroutine shutdown.

## 5) Persistence/recovery model

- On submit: WAL `submit` record + store add + priority queue enqueue.
- On scheduled submit: WAL `submit` record + store add (`SCHEDULED`) + delay queue enqueue.
- On cron tick due: cron scheduler calls engine submit path, so cron-created jobs follow the exact same WAL/store/queue path as API-created jobs.
- On cancel: store marks job cancelled + WAL `cancel` record.
- On retry: scheduler writes WAL `retry`, updates job, enqueue into delay queue.
- On completion: scheduler writes WAL `complete`.
- On permanent failure: scheduler writes WAL `failed`, updates store, adds to DLQ.
- On graceful stop: scheduler snapshot is written and WAL is truncated (compaction).
- On boot: `Engine.Restore()` loads snapshot first, overlays WAL replay state, normalizes recoverable statuses, and requeues only runnable jobs.

## 6) Current tradeoffs / known gaps

- In-memory `JobStore` only; no distributed state.
- Worker ID is hardcoded (`cfg.WorkerID = 1`); multi-worker needs env var or flag.
- `WorkerManager.Monitor` (heartbeat timeout) is implemented but not yet started by `Engine.Start`; worker failover completes in PR #16.
- No auth/TLS on gRPC (Phase 11).
- No observability stack (metrics/tracing) yet (Phase 10).
- `fmt.Printf` used for logging; will be replaced with structured logger (Phase 10).
- Single proto service (`WorkerService`) shared by control and worker nodes; will be split into `ControllerService` / `ExecutionService` when the protocol grows (Phase 8+).
- Cron parser supports `* * * * *` and `*/N * * * *` formats only.

## 7) Next recommended implementation order

1. Add deterministic tests for scheduler loops, retry policy, WAL replay, and snapshot merge behavior.
2. Add deterministic tests for cron submission flow and scheduled-job restore behavior.
3. Start distributed worker protocol (registration/heartbeat/lease/reassignment).
