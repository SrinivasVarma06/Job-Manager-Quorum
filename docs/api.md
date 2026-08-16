# Quorum HTTP API

Base URL: `http://localhost:8080` (control node).

## Authentication & Authorization (JWT + RBAC)

Quorum supports HMAC-SHA256 (HS256) JWT authentication with Role-Based Access Control (RBAC).

To enable authentication on the control plane, set:
```bash
export QUORUM_AUTH_ENABLED=true
export QUORUM_JWT_SECRET="your-256-bit-secret"
```

### Authorization Header
Authenticated requests must pass a valid Bearer token:
```http
Authorization: Bearer <jwt-token>
```

### Roles & Permission Matrix

| Role | Permissions |
|---|---|
| **`admin`** | Full access to all endpoints (Job submission, cancellation, Cron management, node eviction, failover simulation, metrics, and cluster status). |
| **`submitter`** | Create jobs (`POST /jobs`), cancel jobs (`DELETE /jobs/{id}`), and view jobs (`GET /jobs`, `GET /jobs/{id}`). |
| **`viewer`** | Read-only access to jobs, cron schedules, cluster state, and metrics (`GET` endpoints). |

### Endpoint Access Requirements

| Method | Path | Required Role | Purpose |
|---|---|---|---|
| `POST` | `/jobs` | `submitter` or `admin` | Submit a job (optionally with idempotency key or schedule) |
| `GET` | `/jobs` | `viewer`, `submitter`, or `admin` | List all jobs |
| `GET` | `/jobs/{id}` | `viewer`, `submitter`, or `admin` | Fetch one job by ID |
| `DELETE` | `/jobs/{id}` | `submitter` or `admin` | Cancel an active or pending job |
| `GET` | `/jobs/leases` | `viewer`, `submitter`, or `admin` | Active execution leases |
| `POST` | `/cron` | `admin` | Create a recurring cron job |
| `GET` | `/cron` | `viewer`, `submitter`, or `admin` | List recurring cron jobs |
| `DELETE` | `/cron/{id}` | `admin` | Delete a recurring cron job |
| `GET` | `/cluster/status` | `viewer`, `submitter`, or `admin` | Aggregated cluster + job counts |
| `GET` | `/cluster/nodes` | `viewer`, `submitter`, or `admin` | Registered worker nodes |
| `DELETE` | `/cluster/nodes/{id}` | `admin` | Evict a worker node |
| `GET` | `/cluster/raft` | `viewer`, `submitter`, or `admin` | Raft leadership state |
| `POST` | `/cluster/failover-simulate` | `admin` | Broadcast a scripted failover demo |
| `GET` | `/metrics` | Public / `viewer` | Prometheus metrics scrape |
| `GET` | `/events` | Public / `viewer` | Server-sent event stream for UI |
| `GET` | `/` | Public | Service liveness JSON |
| `GET` | `/ui` | Public | Embedded UI dashboard |

### Authentication Error Responses

- **401 Unauthorized**: Missing, malformed, invalid signature, or expired JWT.
  ```json
  {
    "error": "Unauthorized",
    "message": "missing Authorization header"
  }
  ```
- **403 Forbidden**: Token is valid, but the user's role is insufficient for the requested endpoint.
  ```json
  {
    "error": "Forbidden",
    "message": "insufficient permissions for this resource"
  }
  ```

---

## Jobs

### POST /jobs

Submit a job. If `run_at` is omitted the job is queued immediately; if present it is
scheduled onto the delay queue.

Request body:

| Field | Type | Required | Notes |
|---|---|---|---|
| `type` | string | yes | Job type / topic, e.g. `email`. Used for worker routing. |
| `priority` | int | no | Must be `>= 0`. Higher values are dispatched first. Defaults to `0`. |
| `run_at` | string | no | RFC3339 timestamp, must be in the future. |

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"email","priority":5}'
```

`201 Created`

```json
{
  "id": 1,
  "status": "submitted"
}
```

Scheduled variant:

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"report","priority":1,"run_at":"2026-08-16T09:00:00Z"}'
```

`201 Created`

```json
{
  "id": 2,
  "status": "scheduled",
  "run_at": "2026-08-16T09:00:00Z"
}
```

Errors:

| Status | Body | Cause |
|---|---|---|
| `400` | `Invalid JSON` | body is not valid JSON |
| `400` | `type is required` | `type` empty or missing |
| `400` | `priority must be >= 0` | negative priority |
| `400` | `run_at must be RFC3339` | unparseable timestamp |
| `400` | `run_at must be in the future` | timestamp in the past |
| `405` | `Method Not Allowed` | method other than GET/POST on `/jobs` |
| `500` | `Failed to submit job` | Raft proposal or store write failed |

### GET /jobs

Returns every job in the store as a JSON array.

```bash
curl http://localhost:8080/jobs
```

`200 OK`

```json
[
  {
    "id": 1,
    "type": "email",
    "priority": 5,
    "status": "COMPLETED",
    "retry_count": 0,
    "max_retries": 3,
    "next_run_at": "0001-01-01T00:00:00Z",
    "last_error": ""
  },
  {
    "id": 2,
    "type": "payment",
    "priority": 9,
    "status": "FAILED",
    "retry_count": 3,
    "max_retries": 3,
    "next_run_at": "2026-08-15T21:41:12Z",
    "last_error": "mock execution failure"
  }
]
```

Job object fields:

| Field | Type | Notes |
|---|---|---|
| `id` | int | Monotonic, restored from the store on restart |
| `type` | string | Job type / topic |
| `priority` | int | Higher first |
| `status` | string | `PENDING`, `SCHEDULED`, `COMPLETED`, `FAILED`, `CANCELLED` |
| `retry_count` | int | Attempts already retried |
| `max_retries` | int | Default `3` |
| `next_run_at` | RFC3339 | Zero value when not scheduled |
| `last_error` | string | Error from the most recent failed attempt |

There is no `RUNNING` status by design — in-flight execution is tracked by leases
(`GET /jobs/leases`), not by persistent state. See `docs/adr/0001`.

### GET /jobs/{id}

```bash
curl http://localhost:8080/jobs/1
```

`200 OK` returns a single job object as above.

| Status | Body | Cause |
|---|---|---|
| `400` | `Invalid job ID` | id is not an integer |
| `404` | `Job not found` | unknown id |

### DELETE /jobs/{id}

Cancels a job. Cancellation is a Raft-replicated state transition and is only legal
from `PENDING` or `SCHEDULED`; terminal states are rejected.

```bash
curl -X DELETE http://localhost:8080/jobs/1
```

`200 OK`

```json
{ "status": "cancelled" }
```

| Status | Body | Cause |
|---|---|---|
| `400` | `invalid job id` | id is not an integer |
| `409` | error text from the engine | job missing or in a terminal state |

### GET /jobs/leases

Intended to expose the ephemeral execution claims held by the current leader — each
lease records `job_id`, `worker_id`, `term`, `attempt` and `started_at`, and is what
lets the scheduler discard stale results from a presumed-dead worker.

```bash
curl http://localhost:8080/jobs/leases
```

`200 OK`

```json
{}
```

**Currently always returns `{}`.** The handler encodes `scheduler.Leases`, a
`*job.LeaseManager` whose `leases` map is unexported, so `encoding/json` has nothing
to marshal. Exposing the leases requires a `List()` accessor or a `MarshalJSON`
method on `LeaseManager` — tracked as a known limitation in the README, not fixed
here because this document does not change runtime code.

---

## Cron jobs

### POST /cron

Only two schedule forms are supported: `* * * * *` (every minute) and
`*/N * * * *` (every N minutes, aligned to the top of the hour).

```bash
curl -X POST http://localhost:8080/cron \
  -H 'Content-Type: application/json' \
  -d '{"id":"nightly-report","schedule":"*/5 * * * *","type":"report","priority":3}'
```

`201 Created`

```json
{ "status": "created", "id": "nightly-report" }
```

| Status | Body | Cause |
|---|---|---|
| `400` | `Invalid JSON` | malformed body |
| `400` | `id is required` / `schedule is required` / `type is required` | missing field |
| `400` | `priority must be >= 0` | negative priority |
| `400` | `unsupported cron schedule` | schedule form not supported |
| `400` | `cron job id already exists` | duplicate id |

### GET /cron

```bash
curl http://localhost:8080/cron
```

`200 OK`

```json
[
  {
    "id": "nightly-report",
    "schedule": "*/5 * * * *",
    "type": "report",
    "priority": 3,
    "next_run": "2026-08-15T21:45:00Z"
  }
]
```

### DELETE /cron/{id}

```bash
curl -X DELETE http://localhost:8080/cron/nightly-report
```

`200 OK`

```json
{ "status": "removed", "id": "nightly-report" }
```

---

## Cluster

### GET /cluster/status

```bash
curl http://localhost:8080/cluster/status
```

`200 OK`

```json
{
  "total_nodes": 2,
  "active_nodes": 2,
  "dead_nodes": 0,
  "total_jobs": 128,
  "pending_jobs": 4,
  "completed_jobs": 120,
  "failed_jobs": 3,
  "cancelled_jobs": 1,
  "leader_node": "127.0.0.1:18088",
  "raft_term": 2
}
```

`pending_jobs` counts both `PENDING` and `SCHEDULED`. Errors use the JSON error shape:
`405 {"error":"method not allowed"}`.

### GET /cluster/nodes

Registered worker nodes and their liveness, as tracked by heartbeats
(`HeartbeatTimeout` = 5s).

```bash
curl http://localhost:8080/cluster/nodes
```

`200 OK`

```json
[
  {
    "id": 1,
    "address": "localhost:50052",
    "alive": true,
    "last_heartbeat": "2026-08-15T21:40:01Z",
    "topics": ["email", "video_processing"]
  },
  {
    "id": 2,
    "address": "localhost:50053",
    "alive": false,
    "last_heartbeat": "2026-08-15T21:39:44Z",
    "topics": ["*"]
  }
]
```

Field names follow `workermanager.NodeSnapshot`.

### DELETE /cluster/nodes/{id}

Evicts a worker from the registry and broadcasts a `WORKER_EVICTED` SSE event.

```bash
curl -X DELETE http://localhost:8080/cluster/nodes/2
```

`200 OK`

```json
{ "evicted": true, "node_id": 2, "message": "node evicted from cluster" }
```

| Status | Body | Cause |
|---|---|---|
| `400` | `{"error":"invalid node ID"}` | id is not an integer |
| `405` | `{"error":"method not allowed"}` | method other than DELETE |

A `GET` on `/cluster/nodes/` with no id falls through to the node list.

### GET /cluster/raft

```bash
curl http://localhost:8080/cluster/raft
```

`200 OK`

```json
{
  "is_leader": true,
  "term": 2,
  "leader_addr": "127.0.0.1:18088",
  "node_id": "node1"
}
```

Use this to find which control node is currently scheduling — followers return
`"is_leader": false` and do not dispatch jobs.

### POST /cluster/failover-simulate

Demo endpoint for the dashboard. It broadcasts a scripted sequence of SSE events
(leader crash → election → new leader → queue rebuild → worker re-registration →
dispatch resumed) over roughly 1.2 seconds. **It does not kill a real leader** — use
`scripts/chaos.sh` for a genuine failover.

```bash
curl -X POST http://localhost:8080/cluster/failover-simulate
```

`200 OK`

```json
{ "simulated": true, "message": "Failover simulation started" }
```

---

## Health and observability

### GET /

Liveness probe. Any path other than `/` that matches no route returns the same body;
`/` itself redirects (`302`) to the dashboard at `/ui/`.

```bash
curl http://localhost:8080/healthz
```

`200 OK`

```json
{ "service": "quorum", "status": "running" }
```

For a readiness signal that reflects scheduling capability, poll
`GET /cluster/raft` and check `is_leader`, or `GET /cluster/status` and check
`active_nodes`.

### GET /metrics

Prometheus text exposition format, served by `promhttp` with the default registry
(so Go runtime and process collectors are included alongside the Quorum metrics).

```bash
curl http://localhost:8080/metrics
```

```
# HELP quorum_jobs_submitted_total Total number of jobs submitted to the system.
# TYPE quorum_jobs_submitted_total counter
quorum_jobs_submitted_total 128
# HELP quorum_queue_depth Current number of jobs waiting in the queue.
# TYPE quorum_queue_depth gauge
quorum_queue_depth 4
# HELP quorum_job_execution_duration_seconds Histogram of job execution durations in seconds.
# TYPE quorum_job_execution_duration_seconds histogram
quorum_job_execution_duration_seconds_bucket{le="0.01"} 12
...
```

Exposed series: `quorum_jobs_submitted_total`, `quorum_jobs_completed_total`,
`quorum_jobs_failed_total`, `quorum_jobs_cancelled_total`, `quorum_queue_depth`,
`quorum_active_workers`, `quorum_job_execution_duration_seconds`.

Scrape config:

```yaml
scrape_configs:
  - job_name: quorum
    static_configs:
      - targets: ['localhost:8080']
```

### GET /events

Server-sent event stream consumed by the dashboard. Event types:
`WORKER_REGISTERED`, `WORKER_EVICTED`, `JOB_SUBMITTED`, `LEASE_GRANTED`,
`LEASE_RELEASED`, `LEADER_CHANGED`, `QUEUE_REBUILT`, `DISPATCH_RESUMED`,
`FAILOVER_TRIGGERED`, plus a `CONNECTED` event sent immediately on subscribe. Each
event carries `type`, `message`, `timestamp` and an optional `metadata` object.

```bash
curl -N http://localhost:8080/events
```

```
data: {"type":"CONNECTED","message":"Stream connected to Quorum Control Plane Event Broadcaster","timestamp":"2026-08-15T21:40:00Z"}

data: {"type":"LEASE_GRANTED","message":"Ephemeral lease granted for Job #7 to Worker-2 (attempt: 1)","timestamp":"2026-08-15T21:40:02Z"}

data: {"type":"LEASE_RELEASED","message":"Job #7 executed successfully; lease released","timestamp":"2026-08-15T21:40:03Z"}
```

---

## gRPC (worker protocol)

Workers do not use the HTTP API. They speak `WorkerService` over gRPC
(`internal/rpc/proto/worker.proto`):

| RPC | Direction | Purpose |
|---|---|---|
| `RegisterWorker` | worker → control node | announce id, address and topics |
| `Heartbeat` | worker → control node | liveness, every interval |
| `ReportResult` | worker → control node | job id, attempt, success, error |
| `SubmitJob` | control node → worker | dispatch a job for execution |

The control node listens on `ControllerGRPCPort` (50051); each worker listens on its
own `QUORUM_WORKER_PORT` (50052 by default). Connections use
`insecure.NewCredentials()` — there is no mTLS yet.
