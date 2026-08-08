# ADR 0001: Separation of Persisted Desired Control State from Ephemeral Execution Leases

## Context
In naive distributed job schedulers, job execution status (`RUNNING`) is written directly to persistent storage when a job is dispatched to a worker. If the control node or worker crashes before completion, the database is left with stale `RUNNING` records, causing split-brain execution, duplicate processing, or stranded jobs upon failover.

## Decision
We adopt the **Temporal / Kubernetes Architecture Model**:
1. **Persisted Desired Control State**:
   - `Pending`, `Scheduled`, `Completed`, `Failed`, `Cancelled`.
   - Stored permanently in BoltDB B-tree buckets and replicated across followers via Raft log.
2. **Ephemeral Execution Leases**:
   - `Lease { JobID, WorkerID, Term, Attempt, StartedAt }`.
   - Maintained strictly in the active Raft Leader's memory (`LeaseManager`).

## Consequences
- **Positive**: On Leader crash or Worker heartbeat timeout, execution leases evaporate cleanly without leaving corrupted states in persistent storage.
- **Positive**: Upon failover, the new Leader reads clean `Pending` and `Scheduled` state from BoltDB status buckets and re-evaluates execution cleanly.
- **Positive**: Eliminates split-brain duplicate execution when dead workers report results late.
