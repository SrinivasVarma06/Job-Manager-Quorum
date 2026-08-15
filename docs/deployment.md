# Deployment

Covers single-node, Docker Compose, multi-node cluster, and recovery. Read the
[cluster caveat](#cluster-deployment) before treating a Compose stack as a real
quorum.

## Topology and ports

| Component | Port | Protocol | Purpose |
|---|---|---|---|
| Control node HTTP | 8080 | HTTP | API, `/metrics`, `/events`, `/ui` |
| Control node gRPC | 50051 | gRPC | `RegisterWorker`, `Heartbeat`, `ReportResult` |
| Raft transport | 18088 | TCP | Raft RPC between control nodes |
| Worker gRPC | 50052+ | gRPC | `SubmitJob` from the control node |
| OTLP collector | 4317 | gRPC | trace export target (external) |

## Configuration

Control-node configuration comes from `config.Default()` in
`internal/config/config.go` and is **compile-time only** today — the control node does
not read environment variables. To change ports, storage paths or Raft identity you
edit that file (or add env parsing; see [Roadmap](#roadmap-for-this-document)).

| Setting | Default | Meaning |
|---|---|---|
| `WorkerCount` | `0` | in-process workers; `0` = distributed-only |
| `HeartbeatTimeout` | `5s` | worker declared dead after this silence |
| `MaxRetries` | `3` | retries before the DLQ (4 attempts total) |
| `MaxBackoff` | `1m` | ceiling on exponential backoff |
| `WorkerExecutionTimeout` | `30s` | per-execution timeout |
| `DelayQueuePollInterval` | `500ms` | delay-queue tick |
| `RateLimit` / `RateBurst` | `5` / `10` | token bucket per worker |
| `BreakerFailureThreshold` | `5` | consecutive failures before the breaker opens |
| `BreakerResetTimeout` | `30s` | breaker half-open delay |
| `ControllerGRPCPort` | `50051` | control-node gRPC port |
| `WorkerGRPCPort` | `50052` | default worker gRPC port |
| `StorageType` / `StoragePath` | `bolt` / `quorum.db` | job store |
| `RaftEnabled` | `true` | leader-only scheduling |
| `RaftNodeID` / `RaftAddr` / `RaftDataDir` | `node1` / `127.0.0.1:18088` / `data/raft` | Raft identity |

The **worker** binary does read the environment:

| Variable | Default | Meaning |
|---|---|---|
| `QUORUM_WORKER_ID` | `1` | unique worker id; must differ per process |
| `QUORUM_WORKER_PORT` | `50052` | worker's own gRPC listen port |
| `QUORUM_WORKER_TOPICS` | `*` | comma-separated job types, `*` = any |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | trace collector |

The worker always dials the controller at `localhost:ControllerGRPCPort`, so today a
worker must run on the same host (or same network namespace) as its control node.

---

## Single-node deployment

The simplest production-ish shape: one control node, N workers on the same host.

```bash
go build -o bin/quorum-server ./cmd/server
go build -o bin/quorum-worker ./cmd/worker

./bin/quorum-server &
QUORUM_WORKER_ID=1 QUORUM_WORKER_PORT=50052 ./bin/quorum-worker &
QUORUM_WORKER_ID=2 QUORUM_WORKER_PORT=50053 ./bin/quorum-worker &
```

The control node bootstraps a single-node Raft cluster, immediately claims leadership
and starts the scheduler. State lives in `quorum.db` and `data/raft/` relative to the
working directory — run the binary from a stable directory and back both up together.

systemd unit sketch for the control node:

```ini
[Unit]
Description=Quorum control node
After=network.target

[Service]
WorkingDirectory=/var/lib/quorum
ExecStart=/usr/local/bin/quorum-server
Restart=always
RestartSec=2
Environment=OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317

[Install]
WantedBy=multi-user.target
```

Health checks: `GET /` for liveness, `GET /cluster/raft` for "is this node the one
scheduling", `GET /cluster/status` for worker and job counts.

---

## Docker Compose deployment

```bash
docker compose up --build
```

`docker-compose.yml` starts five containers:

| Service | Container | Host ports (HTTP / gRPC / Raft) |
|---|---|---|
| `node-1` | `quorum-node-1` | 8080 / 50051 / 18088 |
| `node-2` | `quorum-node-2` | 8081 / 50052 / 18089 |
| `node-3` | `quorum-node-3` | 8082 / 50053 / 18090 |
| `worker-1` | `quorum-worker-1` | — |
| `worker-2` | `quorum-worker-2` | — |

Each control node gets a named volume (`node1_data`, `node2_data`, `node3_data`)
mounted at `/app/data`.

Verify:

```bash
curl -s localhost:8080/cluster/status | jq
curl -s localhost:8080/cluster/raft   | jq
docker compose logs -f node-1
```

Teardown, including state:

```bash
docker compose down -v
```

The image is a two-stage build (`golang:1.26-alpine` → `alpine`) producing static
`quorum-server` and `quorum-worker` binaries; it exposes 8080, 50051 and 18088 and
defaults to the server.

### Compose caveats

Two mismatches between the Compose file and the current code — both are documented in
the README's Known Limitations and neither is fixed by this document:

1. **Control-node env vars are ignored.** `RAFT_NODE_ID`, `RAFT_ADDR`,
   `RAFT_DATA_DIR`, `CONTROLLER_GRPC_PORT` and `STORAGE_PATH` are set per service but
   `config.Default()` never reads them, so all three nodes use `node1`,
   `127.0.0.1:18088` and `quorum.db`. Each container is isolated, so they start, but
   they are three independent single-node clusters rather than one quorum.
2. **Worker flags are ignored.** `worker-1` and `worker-2` are launched with
   `-id`, `-controller` and `-topics`; `cmd/worker` does not call `flag.Parse` and
   reads `QUORUM_WORKER_*` instead. Both workers therefore come up as id `1` with
   topics `*` and dial `localhost:50051` inside their own container, where no control
   node is listening.

Until control-node configuration is environment-driven, the reliable container shape
is one control node plus workers in the *same* container network namespace, e.g.:

```yaml
services:
  node-1:
    build: .
    command: /app/quorum-server
    ports: ["8080:8080"]
    volumes: ["node1_data:/app/data"]

  worker-1:
    build: .
    command: /app/quorum-worker
    network_mode: "service:node-1"     # share localhost with the control node
    environment:
      - QUORUM_WORKER_ID=1
      - QUORUM_WORKER_PORT=50052
      - QUORUM_WORKER_TOPICS=email,video_processing
    depends_on: [node-1]
```

---

## Cluster deployment

**Target design.** Three or five control nodes form a Raft group. Every node applies
the replicated log to its own BoltDB, but only the leader runs the scheduler, cron and
dispatch loops. On leadership change the new leader runs the startup sequence:
rebuild queues from the store → 1s worker re-registration grace period → enable
dispatch. Followers cancel their scheduler context and clear all leases, so exactly
one node dispatches at a time.

Writes go through Raft (`ProposeAddJob`, `ProposeUpdateJob`, `ProposeCancelJob`,
`ProposeAddCron`, `ProposeDeleteCron`); reads are served from the local BoltDB. A
five-node group tolerates two failures; a three-node group tolerates one.

**Current state.** `NewRaftNode` bootstraps a single-node configuration whenever it
finds no existing state, and nothing calls `AddVoter` — there is no join endpoint or
peer list. `AddVoter` and `RemoveServer` exist in `internal/consensus/raft.go`, so
wiring a real cluster needs:

1. A peer/bootstrap configuration (env or config file: node id, Raft addr, peer list).
2. Bootstrap only on the designated first node; other nodes start empty and are added
   with `AddVoter` by the leader (e.g. a `POST /cluster/join` endpoint).
3. A persistent log store — `raft.NewInmemStore()` is currently used for both the log
   and stable store, so replicated log state does not survive a restart. Swap in
   `raft-boltdb` before running multi-node for real.
4. API forwarding or client redirect from followers to the leader for writes.

Do not run a multi-node Quorum cluster in production before those four items land;
run the single-node shape and treat the control node as a restartable, backed-up
component.

### Scaling workers

Workers are stateless and safe to scale horizontally. Each needs a unique
`QUORUM_WORKER_ID` and port. Route by job type with `QUORUM_WORKER_TOPICS`: the
broker (`internal/broker`) only offers a job to workers subscribed to its type, with
`*` as the catch-all. Scale on `quorum_queue_depth` from `/metrics`.

---

## Recovery process

### Worker crash

Automatic. `WorkerManager.Monitor` marks a node dead after `HeartbeatTimeout` (5s) and
pushes its id onto `DeadWorkers`; the scheduler's recovery loop calls
`Leases.ReleaseByWorker`, and every released job still in `PENDING` is re-enqueued on
the priority queue with the attempt counter incremented. A late `ReportResult` from
the presumed-dead worker carries the old attempt number and is discarded.

Manual eviction, if a worker is wedged but still heartbeating:

```bash
curl -X DELETE localhost:8080/cluster/nodes/2
```

### Control-node restart

1. Stop the process (`SIGINT`); `Engine.Stop()` cancels every loop and closes BoltDB.
2. Restart. `Engine.Restore()` reloads jobs and restores the monotonic ID counter.
3. On leadership claim, `RebuildQueuesFromStore()` rebuilds the priority and delay
   queues from the status buckets.

In-flight executions are lost by design — leases are ephemeral leader memory, so a job
that was running is simply re-dispatched. That is the at-least-once contract.

### Leader failover

With a real multi-node cluster: kill the leader, the survivors elect a new one within
about 2× the 1s election timeout, and the new leader runs the startup sequence.
`scripts/chaos.sh` exercises this against Compose:

```bash
SERVER_URL=http://localhost:8080 ./scripts/chaos.sh
```

It checks cluster health, submits 100 jobs under load, `docker kill`s
`quorum-node-1`, waits 2s and queries `localhost:8081/cluster/status`. Note the script
prints a "CHAOS TEST PASSED" banner unconditionally — it does not assert job counts,
and with the Compose caveats above the nodes are not actually one quorum, so treat it
as a demo rather than a test.

`POST /cluster/failover-simulate` only broadcasts a scripted SSE sequence for the
dashboard; it kills nothing.

### Backup and restore

State to back up on each control node:

| Path | Contents |
|---|---|
| `quorum.db` | BoltDB job store, cron definitions, DLQ |
| `data/raft/` | Raft file snapshot store |

BoltDB is a single file; snapshot it with the process stopped, or copy it from a
filesystem snapshot. To restore, stop the node, put both paths back, and start it —
`Restore()` plus `RebuildQueuesFromStore()` bring the queues back. `internal/storage`
also provides standalone WAL append/replay and snapshot/restore helpers used by the
storage tests.

RPO/RTO are not yet defined; with the in-memory Raft log the honest statement is
"durability is whatever BoltDB has fsynced".

---

## Observability wiring

Nothing scrapes or collects by default — `docker-compose.yml` has no Prometheus,
Grafana or trace collector. Minimal additions:

```yaml
  prometheus:
    image: prom/prometheus
    ports: ["9090:9090"]
    volumes: ["./prometheus.yml:/etc/prometheus/prometheus.yml"]

  jaeger:
    image: jaegertracing/all-in-one
    ports: ["16686:16686", "4317:4317"]
```

```yaml
# prometheus.yml
scrape_configs:
  - job_name: quorum
    static_configs:
      - targets: ['node-1:8080', 'node-2:8080', 'node-3:8080']
```

Then set `OTEL_EXPORTER_OTLP_ENDPOINT=jaeger:4317` on the control node and worker
services.

---

## Roadmap for this document

Items that will change these instructions once implemented: environment-driven control
node config, a persistent Raft log store, a cluster join endpoint, Kubernetes
manifests and a Helm chart, Terraform for the underlying infrastructure, and HPA on
`quorum_queue_depth`. See the README roadmap and `docs/roadmap.md`.
