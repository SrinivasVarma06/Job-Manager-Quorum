# Quickstart

Five minutes from clone to a job executing on a worker.

## Prerequisites

- Go 1.26 or newer (`go.mod` declares `go 1.26.5`)
- `curl` and optionally `jq`
- Docker + Docker Compose, only for the container path

## 1. Clone and verify

```bash
git clone https://github.com/SrinivasVarma06/Job-Manager-Quorum.git
cd Job-Manager-Quorum
go mod download
go test ./...
```

All packages should report `ok`. If you see
`go.mod requires go >= 1.26.5`, upgrade your toolchain.

## 2. Start the control node

```bash
go run ./cmd/server
```

You should see:

```
Quorum control node listening on :8080 (UI available at http://localhost:8080/ui)
INFO Bootstrapped Raft cluster node_id=node1 addr=127.0.0.1:18088
INFO Claimed Raft Leadership: executing Leader Startup Sequence...
```

Ports: `8080` HTTP, `50051` gRPC (worker protocol), `18088` Raft.
State is written to `quorum.db` (BoltDB) and `data/raft/` in the working directory.

## 3. Start a worker

In a second terminal:

```bash
QUORUM_WORKER_ID=1 \
QUORUM_WORKER_PORT=50052 \
QUORUM_WORKER_TOPICS=email,video_processing \
go run ./cmd/worker
```

```
INFO Worker node started worker_id=1 worker_addr=localhost:50052 controller_addr=localhost:50051
```

The worker registers over gRPC and starts heartbeating. Confirm the control node sees
it:

```bash
curl -s localhost:8080/cluster/nodes | jq
```

For a second worker use a different id and port:

```bash
QUORUM_WORKER_ID=2 QUORUM_WORKER_PORT=50053 QUORUM_WORKER_TOPICS='*' go run ./cmd/worker
```

`QUORUM_WORKER_TOPICS='*'` is the wildcard — that worker accepts any job type.

## 4. Submit a job

```bash
curl -s -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"email","priority":5}' | jq
```

```json
{ "id": 1, "status": "submitted" }
```

The worker log shows the execution:

```
INFO Executing job worker_id=1 job_id=1 type=email
INFO Job completed worker_id=1 job_id=1
```

Check the final state:

```bash
curl -s localhost:8080/jobs/1 | jq
```

```json
{
  "id": 1,
  "type": "email",
  "priority": 5,
  "status": "COMPLETED",
  "retry_count": 0,
  "max_retries": 3,
  "next_run_at": "0001-01-01T00:00:00Z",
  "last_error": ""
}
```

## 5. Try the rest of the surface

Priority ordering — submit several jobs while no worker is running, then start a
worker and watch the highest priority go first:

```bash
for p in 1 9 5; do
  curl -s -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
    -d "{\"type\":\"email\",\"priority\":$p}" > /dev/null
done
```

A scheduled job (delay queue):

```bash
RUN_AT=$(date -u -d '+2 minutes' +%Y-%m-%dT%H:%M:%SZ)   # macOS: date -u -v+2M +%Y-%m-%dT%H:%M:%SZ
curl -s -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
  -d "{\"type\":\"report\",\"priority\":1,\"run_at\":\"$RUN_AT\"}" | jq
```

A recurring job (cron):

```bash
curl -s -X POST localhost:8080/cron -H 'Content-Type: application/json' \
  -d '{"id":"heartbeat-report","schedule":"*/5 * * * *","type":"report","priority":2}' | jq
curl -s localhost:8080/cron | jq
```

Cancel a job:

```bash
curl -s -X DELETE localhost:8080/jobs/3 | jq
```

Retries and the DLQ — `executor.MockExecutor` fails some jobs deliberately, so keep
submitting and you will see `retry_count` climb with `status: "SCHEDULED"` and
`next_run_at` moving out 1s → 2s → 4s, then `FAILED` after the fourth attempt.

Watch the live event stream:

```bash
curl -N localhost:8080/events
```

Cluster and telemetry:

```bash
curl -s localhost:8080/cluster/status | jq
curl -s localhost:8080/cluster/raft | jq
curl -s localhost:8080/metrics | grep ^quorum_
```

Dashboard: open <http://localhost:8080/ui>.

## 6. Failure recovery in 30 seconds

1. Submit a batch of jobs.
2. Kill a worker with `Ctrl+C` mid-run.
3. Within `HeartbeatTimeout` (5s) the control node logs
   `Processing failover for dead worker`, releases that worker's leases and re-queues
   the affected jobs.
4. A surviving worker picks them up with `attempt` incremented; a late result from the
   dead worker would be discarded as stale.

Restart durability:

1. Submit jobs, `Ctrl+C` the control node.
2. `go run ./cmd/server` again — `Engine.Restore()` reloads jobs from `quorum.db` and
   `RebuildQueuesFromStore()` refills the queues on leadership claim.
3. `curl localhost:8080/jobs` still shows them.

## 7. Benchmark

```bash
go run ./cmd/benchmark
```

Results land in `benchmarks/`. See [`../benchmarks/results.md`](../benchmarks/results.md).

## Next steps

- API reference: [`api.md`](api.md)
- Deployment and cluster topology: [`deployment.md`](deployment.md)
- Design decisions: [`adr/`](adr/) and [`architecture.md`](architecture.md)

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `bind: address already in use` on 8080/50051/18088 | previous run still alive | kill it, or change the ports in `internal/config/config.go` |
| Worker starts but `/cluster/nodes` is empty | control node not up, or worker dialing the wrong port | the worker always dials `localhost:ControllerGRPCPort` (50051) |
| Jobs stay `PENDING` | no worker registered for that job type | start a worker with a matching `QUORUM_WORKER_TOPICS` or `'*'` |
| Two workers with the same id | `QUORUM_WORKER_ID` not set | give every worker a unique id *and* port |
| `failed to create otlp exporter` | tracing enabled without a collector | run a collector on `localhost:4317` or set `OTEL_EXPORTER_OTLP_ENDPOINT` |
| Stale jobs from an old run | `quorum.db` persists across restarts | delete `quorum.db` and `data/` for a clean slate |
