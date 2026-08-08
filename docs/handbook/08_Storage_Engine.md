# Chapter 8 — Storage Engine & Total System Durability (bbolt)

## What Was Built

PR #17 delivers **Phase 6: Storage Engine & Total System Durability**.

Quorum now features a persistent storage layer backed by **`bbolt` (BoltDB)**:
- **`store.Store` interface**: Decouples application logic from storage backends.
- **`MemoryStore`**: In-memory, zero-overhead storage for tests and local development.
- **`BoltStore`**: Embedded ACID-compliant B-tree storage engine that writes directly to disk (`quorum.db`).
- **Total Durability & Failure Recovery**: Jobs, statuses, priority levels, next retry timestamps, and execution metadata persist across control node process restarts without losing state.

---

## Complete End-to-End Resilience Architecture

```text
Job Submitted
      ↓
Stored on Disk (bbolt quorum.db) + WAL Log
      ↓
Dispatched over gRPC to Remote Worker
      ↓
Worker Dies? ──► Heartbeat timeout -> recoveryLoop -> Status Pending -> Re-queue -> Next worker executes
      ↓
Server Dies? ──► Restore() scans quorum.db -> Status Pending -> Re-queue -> Next worker executes
      ↓
Eventually Completes Cleanly
```

---

## Key Recovery Behaviors Verified by Staff-Level Tests

### 1. In-Flight Execution Recovery During Server Restarts
- **Scenario**: A job was dispatched to Worker 101 and marked `Status = Running`. The control node process is abruptly terminated or restarted before Worker 101 reports completion.
- **Recovery Action**: When `Engine.Restore()` runs on boot, `normalizeRecoveredJob` detects the orphaned `Running` status, converts it to `Pending` (`WorkerID = 0`), updates `BoltStore`, and re-enqueues the job into `PriorityQueue`.
- **Test**: `TestEngineRecoveryWhenWorkerDiesAndServerRestarts` in `internal/engine/engine_test.go`.

### 2. High-Volume Bulk Persistence Recovery (1,000 Jobs)
- **Scenario**: 1,000 jobs are submitted to the control node. The control node is stopped and restarted.
- **Recovery Action**: `Engine.Restore()` scans `quorum.db`, recovers 100% of all 1,000 jobs, sets `nextJobID = 1001`, and re-enqueues all runnable jobs into priority/delay queues.
- **Test**: `TestEngineBulkDurabilityRecovery1000Jobs` in `internal/engine/engine_test.go`.

### 3. Graceful Recovery from Corrupted WAL Files
- **Scenario**: A power outage occurs mid-write, leaving incomplete/corrupted JSON trailing bytes at the end of `jobs.log`.
- **Recovery Action**: `WAL.Replay()` detects incomplete JSON records, safely stops scanning at the corrupted byte boundary, and successfully recovers all valid preceding transactions without failing engine startup.
- **Test**: `TestWALCorruptedFileRecovery` in `internal/storage/wal_test.go`.

---

## Physical Storage Layout in bbolt

- **Database File**: `quorum.db` (permissions `0600`).
- **Bucket**: `"jobs"`
- **Key**: ASCII byte string of `job.ID` (e.g., `"42"` → `[]byte("42")`).
- **Value**: Canonical JSON-encoded `job.Job` struct.

```json
{
  "id": 42,
  "type": "email",
  "priority": 5,
  "status": "pending",
  "retry_count": 0,
  "max_retries": 3,
  "worker_id": 0,
  "created_at": "2026-08-06T01:10:00Z"
}
```

---

## Interview Questions & Answers

**Q: What is the relationship between the WAL (`jobs.log`) and the Storage Engine (`quorum.db`)?**
> The Storage Engine (`bbolt`) is our primary persistent store holding the current canonical state of all jobs. The WAL (`jobs.log`) is an append-only log capturing transient submission and retry events. During `Engine.Restore()`, snapshot and WAL events are replayed into `JobStore`, and then `JobStore` state is normalized to ensure any in-flight jobs are recovered.

**Q: What happens if a control node crashes while a worker is processing a job?**
> On control node restart, `Engine.Restore()` scans the stored jobs in `quorum.db`. Any job left in the `Running` state is normalized to `Pending` (`WorkerID = 0`) and re-enqueued into the scheduler's `PriorityQueue`. When a worker becomes available, the job is re-dispatched, guaranteeing at-least-once execution.

**Q: How does Quorum handle corrupted WAL files caused by sudden power loss?**
> `WAL.Replay()` uses a line-by-line scanner. If it encounters a corrupted JSON record at the tail of the log (incomplete write from a sudden crash), it stops reading at that boundary and returns all preceding valid transactions. This prevents a single truncated log entry from preventing system recovery.
