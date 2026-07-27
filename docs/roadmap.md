# Quorum Roadmap (Execution Plan)

This roadmap tracks implementation status against the flagship plan.

## Current Status Snapshot

- Current implementation level: **Phase 5 (Sprint 5.1 completed)**
- Core local engine: implemented
- WAL recovery: implemented
- HTTP API: implemented
- Priority/retry/delay/scheduled/cron/DLQ: implemented
- Worker Abstraction (`worker.Client` interface): implemented (Sprint 5.1)
- Remaining in Phase 5: Worker registration, heartbeats, lease model, remote execution

---

## Phase-by-Phase Plan

## Phase 1 — Local Job Engine
**Goal:** Build a functioning background job engine without networking.

### Sprint 1.1 — Workers
- [x] Worker struct
- [x] Worker ID
- [x] Worker lifecycle
- [x] Execute()
- [x] Job completion result path
- [x] Worker logs

### Sprint 1.2 — Scheduler
- [x] Scheduler core
- [x] Assign jobs to workers
- [x] Queue polling/notification
- [x] Graceful shutdown with context + waitgroup

### Sprint 1.3 — Concurrency
- [x] Mutexes for shared state
- [x] Basic race-safe store/queue manager patterns
- [x] Benchmarks

Deliverable: **Multiple workers executing jobs concurrently** (achieved)

---

## Phase 2 — Storage
### Sprint 2.1 — WAL
- [x] WAL append
- [x] WAL replay
- [x] Recovery on restart

### Sprint 2.2 — Snapshots
- [x] Snapshot save/load primitives
- [x] Snapshot restore integrated in engine startup path
- [x] Log compaction strategy wired as routine operation (graceful-stop compaction)

Deliverable: **Restart server -> jobs still exist** (achieved via snapshot + WAL replay)

---

## Phase 3 — HTTP API
### Sprint 3.1
- [x] POST /jobs
- [x] GET /jobs
- [x] DELETE /jobs/{id}
- [x] JSON request/response
- [x] GET /jobs/{id} (extra)

### Sprint 3.2
- [x] Validation
- [x] Error handling
- [x] Request IDs
- [x] Logging middleware

Deliverable: **curl -> job submitted -> worker executes** (achieved)

---

## Phase 4 — Production Scheduler
### Sprint 4.1
- [x] Priority queue (heap)

### Sprint 4.2
- [x] Retry
- [x] Exponential backoff
- [x] Retry queue via delay queue

### Sprint 4.3
- [x] Delayed jobs
- [x] Scheduled one-time jobs (`run_at`)
- [x] Cron (integrated with engine lifecycle + submit path)

### Sprint 4.4
- [x] Rate limiter
- [x] Circuit breaker
- [x] DLQ

Deliverable: **Production scheduler** (achieved)

---

## Phase 5 — Distributed Workers
- [x] Sprint 5.1 — Worker Abstraction (`worker.Client` interface decoupling scheduler/manager)
- [ ] Sprint 5.2 — Worker Registration Protocol
- [ ] Sprint 5.3 — Heartbeats & Health Detection
- [ ] Sprint 5.4 — Remote Execution over RPC
- [ ] Sprint 5.5 — Job Rescheduling & Lease Expiration

---

## Phase 6 — Storage Engine (Redis-lite)
- [ ] KV store
- [ ] WAL + recovery for store
- [ ] Snapshot + TTL + memory management

---

## Phase 7 — Broker (Kafka/NATS-lite)
- [ ] Broker
- [ ] Producer/Consumer
- [ ] ACK/Requeue
- [ ] Partitioning
- [ ] Consumer groups

---

## Phase 8 — Control Plane
- [ ] RPC
- [ ] Multi-node control plane
- [ ] Replication
- [ ] Leader election

---

## Phase 9 — Raft
- [ ] Terms + elections
- [ ] AppendEntries
- [ ] Commit index + recovery

---

## Phase 10 — Observability
- [ ] Structured logging standardization
- [ ] Metrics
- [ ] Prometheus
- [ ] Grafana
- [ ] Tracing

---

## Phase 11 — Security
- [ ] JWT
- [ ] OAuth
- [ ] RBAC
- [ ] mTLS

---

## Phase 12 — Deployment
- [ ] Docker
- [ ] Docker Compose
- [ ] Kubernetes
- [ ] Helm
- [ ] Terraform
- [ ] GitHub Actions

---

## Phase 13 — Performance
- [ ] Benchmarks
- [ ] k6
- [ ] Chaos testing
- [ ] Toxiproxy
- [ ] Profiling

---

## Phase 14 — Research
- [ ] ADRs
- [ ] TLA+
- [ ] Benchmark report
- [ ] Architecture docs polishing
- [ ] Demo video
- [ ] Resume bullets
- [ ] Interview Q&A
