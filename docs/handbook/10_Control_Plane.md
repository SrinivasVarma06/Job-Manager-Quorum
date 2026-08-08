# Chapter 10 — Control Plane & Cluster Management

## What Was Built

PR #19 delivers **Phase 8: Control Plane & Cluster Management**.

Quorum now features a dedicated Control Plane inspection and management layer:
- **`GET /cluster/nodes`**: Real-time inspection of all registered worker nodes, their gRPC addresses, liveness status (`Alive`), last heartbeat timestamp, and subscribed topics.
- **`GET /cluster/status`**: Aggregated cluster-wide metrics (active nodes, dead nodes, total jobs, pending jobs, running jobs, completed jobs, failed jobs, cancelled jobs).
- **`DELETE /cluster/nodes/{id}`**: Graceful eviction/deregistration of worker nodes from the cluster.

---

## Control Plane Architecture

```text
               +----------------------------------+
               |     Admin / Monitoring Client    |
               +----------------------------------+
                                |
                   HTTP GET /cluster/status
                   HTTP GET /cluster/nodes
                   HTTP DELETE /cluster/nodes/{id}
                                |
                                v
               +----------------------------------+
               |          Control Node            |
               |         (cmd/server)             |
               |         ClusterHandler           |
               +----------------------------------+
                                |
                                v
               +----------------------------------+
               |     WorkerManager & JobStore     |
               |  - Nodes() snapshot              |
               |  - Remove(nodeID) eviction       |
               |  - Aggregated ClusterStatus      |
               +----------------------------------+
```

---

## REST Endpoints Specification

### 1. `GET /cluster/status`
Returns high-level status metrics for cluster operators and dashboard UIs.

**Response `200 OK`**:
```json
{
  "total_nodes": 3,
  "active_nodes": 3,
  "dead_nodes": 0,
  "total_jobs": 150,
  "pending_jobs": 2,
  "running_jobs": 5,
  "completed_jobs": 140,
  "failed_jobs": 3,
  "cancelled_jobs": 0
}
```

### 2. `GET /cluster/nodes`
Returns an array of registered worker node snapshots.

**Response `200 OK`**:
```json
[
  {
    "id": 101,
    "address": "localhost:50052",
    "alive": true,
    "last_heartbeat": "2026-08-07T19:50:00Z",
    "topics": ["email", "sms"]
  },
  {
    "id": 102,
    "address": "localhost:50053",
    "alive": true,
    "last_heartbeat": "2026-08-07T19:50:01Z",
    "topics": ["video_processing"]
  }
]
```

### 3. `DELETE /cluster/nodes/{id}`
Evicts a worker node from the cluster state and topic broker immediately.

**Response `200 OK`**:
```json
{
  "evicted": true,
  "node_id": 101,
  "message": "node evicted from cluster"
}
```

---

## Verification & Demonstrable Results

Unit tests pass across all packages (`go test ./...`):
- `ok quorum/internal/handlers` (`TestClusterHandlerNodesAndStatus`)
- `ok quorum/internal/workermanager` (`TestWorkerManagerNodes`)

### Demonstration Guide

#### 1. Start Control Node
```powershell
go run ./cmd/server
```

#### 2. Start Worker Nodes
```powershell
$env:QUORUM_WORKER_ID="101"; $env:QUORUM_WORKER_PORT="50052"; $env:QUORUM_WORKER_TOPICS="email"; go run ./cmd/worker
$env:QUORUM_WORKER_ID="102"; $env:QUORUM_WORKER_PORT="50053"; $env:QUORUM_WORKER_TOPICS="video_processing"; go run ./cmd/worker
```

#### 3. Query Cluster Status
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/cluster/status" -Method GET
```

#### 4. Query Cluster Nodes
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/cluster/nodes" -Method GET
```

#### 5. Evict a Node
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/cluster/nodes/101" -Method DELETE
```

---

## Interview Questions & Answers

**Q: How does the Control Plane monitor node liveness without polling every worker endpoint over HTTP?**
> Workers push `Heartbeat` RPCs over gRPC to the control node every 1 second. `WorkerManager` tracks the `LastHeartbeat` timestamp locally in memory. The `GET /cluster/nodes` endpoint reads this memory map in $O(N)$ time with zero network overhead.

**Q: What happens when a node is evicted via `DELETE /cluster/nodes/{id}`?**
> The `WorkerManager` removes the worker from the active node registry and unsubscribes its topics from the `Broker`. Any in-flight jobs assigned to that worker will time out and be safely recovered by `recoveryLoop` and re-dispatched to remaining healthy workers.
