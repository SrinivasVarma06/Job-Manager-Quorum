# Quorum Changelog

Tracks meaningful engineering changes and why they were made.

## Unreleased

### Added
- Engine composition root with:
  - JobStore
  - WorkerManager
  - Priority queue + Delay queue
  - WAL-backed recovery
  - Scheduler lifecycle startup/shutdown
- Worker pool execution pipeline with availability signaling.
- HTTP API:
  - `POST /jobs`
  - `GET /jobs`
  - `GET /jobs/{id}`
  - `DELETE /jobs/{id}`
- Middleware:
  - request ID header
  - request logging
- Retry subsystem:
  - retry eligibility
  - exponential backoff
  - delayed requeue
- Dead-letter queue for terminal failures.
- Executor wrappers:
  - token-bucket rate limiter
  - circuit breaker
- Cron scheduler prototype package.
- Cron HTTP APIs:
  - `POST /cron`
  - `GET /cron`
  - `DELETE /cron/{id}`
- Scheduled one-time jobs via `POST /jobs` with `run_at` (RFC3339).

### Changed
- WAL evolved from simple job append to event-style records:
  - submit
  - retry
  - failed
  - complete
  - cancel
- Scheduler split into dedicated loops:
  - dispatch loop
  - result loop
  - delay loop
- Engine restore now merges snapshot state with WAL replay state.
- Engine graceful shutdown now snapshots state and truncates WAL (compaction).
- Cancel operation now appends a `cancel` WAL event for durable recovery.
- Cron scheduler was refactored to inject a submit function instead of mutating queues directly.
- Cron scheduler is now started/stopped as part of engine lifecycle and submits through the same durable path as API jobs.
- Engine now supports `SubmitJobAt` and persists scheduled jobs into delay queue with durable WAL submit events.
- Recovery path now handles both retrying and scheduled jobs when rebuilding queues.

### Why these changes matter
- Improves reliability and restart recovery behavior.
- Separates concerns (store, queue, scheduler, executor) for maintainability.
- Adds production-oriented controls (retry, DLQ, limiter, breaker) before distributed rollout.

### Known follow-ups
- Add unit tests and benchmarks for scheduler/store/replay/circuit-breaker behavior.
- Add observability instrumentation before distributed phase.
