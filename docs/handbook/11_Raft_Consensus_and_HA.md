# Chapter 11 — Raft Consensus & Control Plane High Availability

## What Was Built

PR #20 delivers **Phase 9: High Availability & Raft Consensus**.

Before Phase 9, a single control node represented a Single Point of Failure (SPOF). Phase 9 introduces **Raft Consensus** (`internal/consensus`) via `hashicorp/raft`:
- **Leader Election**: Multiple control nodes run as a Raft cluster. A single active Raft Leader is elected by majority quorum.
- **Raft FSM (Finite State Machine)**: Job submissions, state updates, and cancellations are replicated across Raft cluster logs before being committed to `store.Store`.
- **Leader & Follower Roles**:
  - The **Raft Leader** accepts state changes, writes Raft log entries, and dispatches jobs.
  - **Raft Followers** replicate Raft logs in standby mode, maintaining an identical mirror of the job store.
- **Automatic Failover**: If the active Raft Leader crashes, followers detect heartbeat loss and elect a new Leader within 1-2 seconds, resuming job dispatch without data loss.

---

## Raft Consensus Architecture

```text
               +----------------------------------+
               |     HTTP / Client Requests       |
               +----------------------------------+
                                |
                   Submit Job / State Change
                                |
                                v
               +----------------------------------+
               |        Active Raft Leader        |
               |         (Control Node 1)         |
               |  - Accepts mutations             |
               |  - Appends to Raft Log           |
               +----------------------------------+
                       /                  \
          Replicate   /                    \ Replicate
          Log Entries/                      \ Log Entries
                    v                        v
          +-------------------+    +-------------------+
          |  Raft Follower 2  |    |  Raft Follower 3  |
          | (Standby Node 2)  |    | (Standby Node 3)  |
          +-------------------+    +-------------------+
```

### Raft FSM Command Lifecycle (`internal/consensus/fsm.go`)
```go
type CommandType string

const (
    CmdAddJob    CommandType = "add_job"
    CmdUpdateJob CommandType = "update_job"
    CmdDeleteJob CommandType = "delete_job"
)
```

1. **Proposal**: When `Engine.SubmitJob` is called on the leader, the engine proposes `Command{Type: CmdAddJob, Job: j}` to `rn.Propose()`.
2. **Quorum Replication**: Raft replicates the log entry to a majority of follower nodes over TCP (`raft.NewTCPTransport`).
3. **Commit & Apply**: Once a quorum confirms receipt, `fsm.Apply()` executes on all nodes, applying the mutation to `store.Store`.

---

## Active/Passive Control Plane Failover

- **Leader**: Runs `Scheduler.Start(ctx)` and actively dispatches jobs to workers.
- **Follower**: Runs in passive standby mode, continually applying committed Raft logs to keep its `Store` 100% in sync with the leader.
- **Failover Sequence**:
  1. Active Leader crashes / network partition occurs.
  2. Followers detect missed Raft heartbeats (>1s election timeout).
  3. Followers transition to `Candidate` state and request votes.
  4. Node 2 receives majority votes and becomes the new `Leader`.
  5. Node 2 starts its scheduler dispatch loop and resumes job dispatching instantly.

---

## Verification & Demonstrable Results

Unit tests pass across all packages (`go test ./...`):
- `ok quorum/internal/consensus` (`TestRaftNodeSingleNodeCluster`)
- `ok quorum/internal/engine` (`TestEngineRestoreWithBoltStore`, `TestEngineBulkDurabilityRecovery1000Jobs`)

---

## Interview Questions & Answers

**Q: Why use Raft consensus instead of simple database replication?**
> Raft provides strong consistency (Linearizability) and automated leader election without relying on an external coordinator (like ZooKeeper or Consul). The control nodes themselves form a self-healing quorum that can survive minority node failures ($F$ failures tolerated in $2F+1$ cluster nodes).

**Q: How does Raft log compaction work in Quorum?**
> As Raft logs grow over time, `fsm.Snapshot()` serializes the entire current `store.Store` state to a snapshot file (`fsmSnapshot.Persist`). Raft then truncates old log entries prior to the snapshot index, preventing disk exhaustion.

**Q: What happens if a client sends a write request to a Raft Follower instead of the Leader?**
> `RaftNode.Propose()` checks `IsLeader()`. If false, it returns an error containing `LeaderAddr()`. The HTTP handler can return `307 Temporary Redirect` pointing to `LeaderAddr()`, guiding the client to the active leader seamlessly.
