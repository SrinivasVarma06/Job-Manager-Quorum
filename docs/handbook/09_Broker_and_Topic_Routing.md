# Chapter 9 — Broker & Capability-Based Topic Routing

## What Was Built

PR #18 delivers **Phase 7: Broker & Capability-Based Topic Routing**.

Before Phase 7, Quorum treated all worker nodes as homogenous executors. In production distributed systems, workers are specialized:
- **`email` workers**: Optimized for network IO & SMTP integration.
- **`video_processing` workers**: Deployed on GPU/high-CPU instances.
- **`general` workers**: Handle miscellaneous lightweight tasks.

Phase 7 introduces the **Broker** layer (`internal/broker`):
- **Topic Subscriptions**: Workers advertise supported job types during registration (`topics` parameter or `QUORUM_WORKER_TOPICS` environment variable).
- **Wildcard Subscriptions (`"*"`)**: Workers can subscribe to wildcard `"*"`, allowing them to execute any job type.
- **Capability-Aware Dispatching**: The scheduler inspects job type (e.g. `"video_processing"`) and uses the `Broker` to select an available worker capable of executing that specific job type.

---

## Architecture & Topic Routing Sequence

```text
               +----------------------------------+
               |     HTTP API Submit Job          |
               |     Type: "video_processing"     |
               +----------------------------------+
                                |
                                v
               +----------------------------------+
               |          Scheduler               |
               |          dispatchLoop            |
               +----------------------------------+
                                |
                                v
               +----------------------------------+
               |            Broker                |
               |  SelectWorker("video_processing")|
               +----------------------------------+
                      /                   \
        Unmatching   /                     \ Capable Worker
        Re-enqueued /                       \ Selected
                   v                         v
           +---------------+         +---------------+
           | Worker 101    |         | Worker 102    |
           | Topics: email |         | Topics: video |
           +---------------+         +---------------+
```

### 1. Registration Protocol
When a worker node starts up, it supplies a list of topics:
```protobuf
message RegisterWorkerRequest {
  int32 worker_id = 1;
  string address = 2;
  repeated string topics = 3;
}
```
Example environment configuration:
- Worker 101: `QUORUM_WORKER_TOPICS="email,sms"`
- Worker 102: `QUORUM_WORKER_TOPICS="video_processing"`
- Worker 103: `QUORUM_WORKER_TOPICS="*"` (wildcard)

### 2. Capability Matching Logic (`internal/broker/broker.go`)
```go
func (b *Broker) CanHandle(workerID int, jobType string) bool {
    b.mu.RLock()
    defer b.mu.RUnlock()

    if b.workerIsAll[workerID] {
        return true // Wildcard matching
    }
    _, supported := b.workerTopics[workerID][jobType]
    return supported
}
```

### 3. Topic-Aware Scheduler Selection
When the scheduler dequeues a job of `Type = "video_processing"`:
- Calls `Broker.SelectWorker("video_processing", s.Available)`.
- The broker inspects ready workers in `Available`. Unmatching workers (e.g. Worker 101) are safely re-enqueued.
- Worker 102 (or wildcard Worker 103) is selected and receives the `SubmitJob` RPC.

---

## Verification & Demonstrable Results

Unit tests pass across all packages (`go test ./...`):
- `ok quorum/internal/broker` (`TestBrokerTopicMatching`, `TestBrokerSelectWorkerTopicRouting`)
- `ok quorum/internal/scheduler` (`TestSchedulerTopicAwareRouting`)

### End-to-End Verification Setup

#### Terminal 1: Control Node
```powershell
go run ./cmd/server
```

#### Terminal 2: Email Worker (Worker 101)
```powershell
$env:QUORUM_WORKER_ID="101"; $env:QUORUM_WORKER_PORT="50052"; $env:QUORUM_WORKER_TOPICS="email"; go run ./cmd/worker
```

#### Terminal 3: Video Worker (Worker 102)
```powershell
$env:QUORUM_WORKER_ID="102"; $env:QUORUM_WORKER_PORT="50053"; $env:QUORUM_WORKER_TOPICS="video_processing"; go run ./cmd/worker
```

#### Terminal 4: Submit Video Processing Job
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/jobs" -Method POST -ContentType "application/json" -Body '{"type":"video_processing","priority":10}'
```
**Observation**:
- Server logs: `INFO Worker registered worker_id=101 topics=[email]`
- Server logs: `INFO Worker registered worker_id=102 topics=[video_processing]`
- Job is routed **exclusively to Worker 102**, while Worker 101 ignores it!

---

## Interview Questions & Answers

**Q: Why separate the Broker from the PriorityQueue?**
> Decoupling priority scheduling (ordering *when* jobs should run) from capability routing (matching *where* jobs can run) keeps algorithms simple. The `PriorityQueue` operates as a pure max-heap based on job priority/time, while the `Broker` provides capability-based filtering during worker selection.

**Q: How does wildcard (`"*"`) matching work?**
> A worker registering with topic `"*"` or `"all"` is flagged as `workerIsAll = true`. The Broker matches this worker to any job type instantly. This allows generalist workers to scale side-by-side with topic-specific specialist workers.

**Q: What happens if a job is submitted for a topic with no registered workers?**
> If no capable worker is available, `Broker.SelectWorker` returns `false`. The scheduler leaves the job in `Pending` state and re-enqueues it to `PriorityQueue`. As soon as a capable worker registers or completes a job, the scheduler dispatches it automatically.
