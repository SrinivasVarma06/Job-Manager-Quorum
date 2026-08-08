# Chapter 7 — Distributed Workers & Resilience

## What Was Built

PR #15 & #16 complete **Phase 5: Distributed Workers & Resilience**.

Quorum operates as a true distributed job orchestration system:
- **Control Node (`cmd/server`)**: Hosts the HTTP API (`:8080`), gRPC service (`:50051`), Scheduler, and `WorkerManager`.
- **Worker Nodes (`cmd/worker`)**: Distributed execution nodes running as independent processes (e.g., Worker 101 on `:50052`, Worker 102 on `:50053`, Worker 103 on `:50054`).
- **Resilience & Failover**: Heartbeat monitoring detects dead worker nodes within 5 seconds and automatically re-queues in-flight jobs to healthy workers.
- **Structured Logging (`slog`)**: High-performance, clean, structured logs for real-time observability across multi-process nodes.

---

## Architecture & Communication Protocols

```
               +----------------------------------+
               |     HTTP Client / REST API       |
               +----------------------------------+
                                |
                                v
               +----------------------------------+
               |          Control Node            |
               |         (cmd/server)             |
               |  - Engine composition root       |
               |  - Scheduler (dispatch & recovery)|
               |  - WorkerManager                 |
               +----------------------------------+
                      /         |          \
         Register /  /  gRPC    |  gRPC     \  gRPC
        Heartbeats  / SubmitJob | SubmitJob  \ SubmitJob
                   v            v             v
             +----------+  +----------+  +----------+
             | Worker   |  | Worker   |  | Worker   |
             | Node 101 |  | Node 102 |  | Node 103 |
             | (:50052) |  | (:50053) |  | (:50054) |
             +----------+  +----------+  +----------+
```

### 1. Worker Registration & Availability
1. When a worker process starts up, it binds its local gRPC server (e.g. `:50052`) and calls `RegisterWorker` on the control node (`:50051`), advertising its ID and address.
2. The control node's `WorkerServer` dials an outbound gRPC connection back to the worker, creates a `RemoteWorker` proxy, registers it with `WorkerManager`, and pushes it into the `Available` channel.
3. The scheduler pops workers from `Available` in load-balanced order.

### 2. Job Dispatch & Remote Execution
1. When a job is pending in `PriorityQueue`, `dispatchLoop` pops the job ID and retrieves the next ready `WorkerClient` from `Available`.
2. It calls `remoteWorker.Submit(ctx, job)`, which invokes the gRPC `SubmitJob` RPC on the worker node.
3. The worker's `ExecutionServer` kicks off `runner.Execute` asynchronously in a goroutine and returns `accepted = true` immediately.

### 3. Result Reporting
1. When execution finishes, the worker process reads `job.Result` from its local channel and sends a `ReportResult` gRPC RPC back to the control node.
2. The control node writes the result to `Scheduler.Results` (triggering retry, completion, or DLQ handling) and re-queues the remote worker back into `Available`.

### 4. Worker Failover & Job Recovery
1. Each worker sends a `Heartbeat` RPC every 1 second.
2. `WorkerManager.Monitor` runs a background check every second. If `time.Since(LastHeartbeat) > 5s`, the worker is marked dead and its ID is sent to `DeadWorkers`.
3. `Scheduler.recoveryLoop` catches dead worker IDs, queries `Store.RunningJobs(deadWorkerID)`, resets job status to `Pending` (`WorkerID = 0`), and enqueues them back into `PriorityQueue`.
4. The scheduler immediately dispatches the recovered jobs to another active worker.

---

## Structured Logging with `log/slog`

All system components use Go's standard `log/slog` structured logging package:
- **Clean output**: Standard text format for local development, structured key-value pairs (`job_id`, `worker_id`, `error`).
- **Log Levels**:
  - `INFO`: Job submission, worker registration, job completion.
  - `WARN`: Worker heartbeat timeouts, job execution failures, failover triggering.
  - `DEBUG`: Per-second heartbeats and result acknowledgments (kept clean by default).

---

## Verification & Demonstrable Results

Unit tests pass across all packages:
```powershell
go test ./...
# ok quorum/internal/scheduler 0.920s
# ok quorum/internal/worker
# ok quorum/internal/workermanager
```

End-to-end failover test (`TestSchedulerWorkerFailoverAndReDispatch`):
1. Simulates Worker 101 executing Job 42.
2. Signals Worker 101 death to `DeadWorkers`.
3. Verifies `recoveryLoop` resets Job 42 status to `Pending` and `dispatchLoop` re-dispatches it to healthy Worker 102.

---

## Interview Talking Points

- **Dependency Inversion at the Process Boundary**: The `Scheduler` depends on the `worker.WorkerClient` interface. Whether execution happens in an in-process goroutine or over gRPC on a remote machine is an implementation detail hidden behind the proxy pattern.
- **At-Least-Once Delivery & Failover**: Heartbeat loss triggers automatic state recovery. Jobs in `Running` state on dead nodes are safely returned to `Pending` and re-queued.
- **Non-Blocking Dispatch**: `SubmitJob` RPC returns immediately upon receipt; execution happens asynchronously. Result delivery via `ReportResult` completes the execution loop cleanly without blocking gRPC connections.
