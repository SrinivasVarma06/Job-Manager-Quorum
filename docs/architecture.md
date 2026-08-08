# Quorum System Architecture Guide

Quorum is an enterprise-grade, fault-tolerant distributed job orchestration platform designed around the **Temporal / Kubernetes model: Separation of Persisted Control State from Ephemeral Execution State**.

---

## 1. High-Level System Topology

```
+-----------------------------------------------------------------------------------+
|                                  CLIENT (HTTP API)                                |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
|                                 RAFT CONTROL NODE                                 |
|                                                                                   |
|   +-----------------------+     Rebuilds      +--------------------------------+  |
|   |      SCHEDULER        |<------------------|     BOLTDB PERSISTED STORE     |  |
|   |   (Priority / Delay)  |                   | Buckets: status_pending,       |  |
|   +-----------------------+                   | status_scheduled, cron, dlq    |  |
|               |                               +--------------------------------+  |
|               v Acquire Ephemeral Lease                       ^                   |
|   +-----------------------+                                   |                   |
|   |     LEASE MANAGER     |                                   | Replicated via    |
|   | (In-Memory Ownership) |                                   | Raft FSM Apply    |
|   +-----------------------+                                   |                   |
|               |                               +--------------------------------+  |
|               | Dispatches over gRPC          |      RAFT REPLICATED LOG       |  |
|               v                               | (HashiCorp Raft Consensus)     |  |
|   +-----------------------+                   +--------------------------------+  |
|   |     CAPABILITY BROKER |                                   ^                   |
|   |  (Topic-Based Match)  |-----------------------------------+                   |
|   +-----------------------+                                                       |
+-----------------------------------------------------------------------------------+
                                         |
                                         v gRPC / TCP
+-----------------------------------------------------------------------------------+
|                                  REMOTE WORKERS                                   |
|   Worker 1 (topics: email) | Worker 2 (topics: video_processing) | Worker N     |
+-----------------------------------------------------------------------------------+
```

---

## 2. Key Architectural Principles

### A. Control State vs. Execution Leases
- **Persisted Desired Control State (BoltDB via Raft)**:
  `Pending`, `Scheduled`, `Completed`, `Failed`, `Cancelled`. Stored permanently in BoltDB B-tree buckets and replicated across followers via Raft log.
- **Ephemeral Execution Lease (`LeaseManager`)**:
  `Lease { JobID, WorkerID, Term, Attempt, StartedAt }`. Maintained strictly in Leader RAM. Evaporates on leader crash or worker timeout without corrupting persistent storage.

### B. Leader Startup Sequencing & Grace Period
When a node claims Raft Leadership, it executes:
1. **Claim Leadership** $\rightarrow$ Pause dispatch loop.
2. **Rebuild Queues** $\rightarrow$ Query BoltDB status buckets (`status_pending`, `status_scheduled`) in $O(k)$ time to populate priority and delay queues.
3. **Worker Re-registration Grace Period** $\rightarrow$ Wait 1s for active workers to re-send gRPC heartbeats.
4. **Enable Dispatch** $\rightarrow$ Resume scheduler dispatch and cron execution.

### C. Transient Fencing Tokens (`Term` + `Attempt`)
Every dispatch payload carried to remote workers includes `Term` (Raft term) and `Attempt` (dispatch counter). Stale completions from previous terms or dead workers are rejected by `LeaseManager`.

### D. Per-Job Execution Context Cancellation
`Runner` maintains `activeTasks map[int]context.CancelFunc`. Calling `Runner.Cancel(jobID)` immediately cancels the context of the running `Executor.Execute(ctx, j)`.

---

## 3. Storage Layout (BoltDB B-Tree Buckets)

- `jobs`: Primary job storage (`JobID` $\rightarrow$ JSON job)
- `cron_jobs`: Cron definitions (`CronID` $\rightarrow$ JSON cron)
- `dlq`: Dead Letter Queue (`JobID` $\rightarrow$ JSON job)
- `status_pending`: Pending index bucket (`JobID` $\rightarrow$ empty)
- `status_scheduled`: Scheduled index bucket (`JobID` $\rightarrow$ empty)
- `status_completed`: Completed index bucket (`JobID` $\rightarrow$ empty)
- `status_failed`: Failed index bucket (`JobID` $\rightarrow$ empty)
- `status_cancelled`: Cancelled index bucket (`JobID` $\rightarrow$ empty)

---

## 4. UI Dashboard Control Plane (`http://localhost:8080/ui`)

Embedded single-binary web control plane featuring:
- **System Overview**: Cluster health, Raft term, throughput, active leases.
- **Cluster Topology**: Interactive visual node graph.
- **Job Lifecycle Explorer**: Distributed systems millisecond audit log timeline.
- **Lease Manager Visualization**: Desired state vs. execution lease inspector.
- **Failover Simulator**: Interactive "Kill Leader" button with timing meters.
- **Queue Reconstruction Viewer**: $O(k)$ status bucket queue rebuild visualizer.
- **Live Event Stream**: Real-time log broadcaster (`tail -f` in UI).
- **Prometheus Telemetry**: `/metrics` exposition.
