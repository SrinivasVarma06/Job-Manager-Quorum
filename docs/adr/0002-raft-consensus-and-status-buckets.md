# ADR 0002: Replicated Raft Consensus and O(k) Status Bucket Queue Reconstruction

## Context
Full database scans ($O(N)$) or full Write-Ahead Log (WAL) replays during cluster failover scale poorly when managing millions of jobs. Furthermore, custom WAL implementations alongside Raft consensus introduce redundant persistence layers.

## Decision
1. **Consolidated Durability**:
   - Replicated **Raft Log + BoltDB FSM** is the single source of truth for persistence, log compaction, and state replication. Custom WAL and standalone Snapshot modules are removed.
2. **Dedicated Status Index Buckets**:
   - `BoltStore` maintains dedicated B-tree buckets (`status_pending`, `status_scheduled`, `status_completed`, `status_failed`, `status_cancelled`).
3. **$O(k)$ Queue Reconstruction**:
   - Upon leadership claim, the new Leader queries only `status_pending` and `status_scheduled` buckets to reconstruct `PriorityQueue` and `DelayQueue` in $O(k)$ time ($k$ = active pending/scheduled jobs).

## Consequences
- **Positive**: Sub-second startup and failover queue reconstruction regardless of total historical job count in storage ($N$).
- **Positive**: Single authoritative durability engine (HashiCorp Raft + BoltDB).
