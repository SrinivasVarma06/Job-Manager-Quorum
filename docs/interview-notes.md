# Quorum Interview Notes

Use this to explain Quorum to senior engineers clearly and confidently.

## 1) 30-second pitch

Quorum is a self-built job orchestration engine in Go.  
It accepts jobs over HTTP, persists lifecycle events in a WAL, schedules via priority/delay queues, executes through a worker pool, and applies production safety features like retries, backoff, DLQ, rate limiting, and circuit breaking.

## 2) Current architecture in one flow

Client -> HTTP handler -> Engine -> JobStore + WAL -> Priority/Delay queues -> Scheduler -> Worker -> Executor -> Result -> Retry/Complete/Fail path

## 3) Key design choices (and how to defend them)

### At-least-once over exactly-once
- Chosen because exactly-once needs stronger coordination/transaction boundaries.
- Current system provides practical reliability with retries + failure tracking.
- Planned future: idempotency keys and explicit delivery guarantees in distributed phase.

### In-memory state + WAL recovery first
- Faster iteration for core scheduler semantics.
- WAL gives crash recovery without committing to a full storage engine too early.
- Snapshot support exists and will be fully integrated next.

### Layered executor wrappers
- Base executor handles actual job logic.
- Rate limiter controls throughput bursts.
- Circuit breaker protects system under repeated failure conditions.
- This composition keeps failure policy separate from business execution.

## 4) Concurrency model to explain

- Workers announce availability on a shared channel.
- Scheduler dispatches jobs to available workers.
- Results return on a separate channel.
- Scheduler has dedicated loops for dispatch, results, and delayed retries.
- Shared mutable state is protected with mutexes (`JobStore`, queues, DLQ, worker registry).

## 5) Failure handling story

- On submit: job written to WAL, then queued.
- On worker failure:
  - scheduler evaluates retry policy,
  - schedules delayed retry with exponential backoff,
  - eventually moves terminal failures to DLQ.
- On restart: WAL replay restores in-flight/pending jobs.

## 6) Current maturity (honest status)

- Strong local orchestration foundation (phases 1–4 mostly covered).
- Not yet distributed:
  - no cross-process leases/heartbeats/reassignment,
  - no broker/control-plane/raft yet.
- Observability/security/deployment/perf phases are planned, not complete.

## 7) Senior-level follow-up answers

### "How do you prevent double execution?"
- Currently: at-least-once model; duplicate-safe behavior depends on job logic.
- Planned: idempotency keys and durable claim/lease protocol in distributed workers.

### "What happens when a worker crashes mid-job?"
- Job does not emit success; scheduler retry policy requeues with backoff.
- WAL preserves job lifecycle events for restart recovery.

### "Why not use Kafka/Temporal?"
- Goal of this project is learning and demonstrating systems fundamentals.
- Building core mechanisms from scratch creates stronger design ownership and interview signal.

## 8) Next milestones to mention in interviews

1. Finish snapshot integration + compaction policy.
2. Add deterministic test coverage + benchmarks.
3. Add distributed worker protocol (heartbeat + lease + reassignment).
4. Introduce broker and control-plane replication.
5. Add observability and deployment story for production readiness.

