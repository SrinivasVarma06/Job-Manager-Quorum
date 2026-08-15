# Contributing to Quorum

Thanks for taking a look. Quorum is a learning-oriented distributed systems project,
so changes are judged on clarity and correctness first: a small, well-reasoned patch
with a test beats a large one without.

## Getting set up

```bash
git clone https://github.com/SrinivasVarma06/Job-Manager-Quorum.git
cd Job-Manager-Quorum
go mod download
go test ./...
```

Requires Go 1.26+ (`go.mod` declares `go 1.26.5`). No other toolchain is needed;
`protoc` is only required if you change `internal/rpc/proto/worker.proto`.

Run the stack: [`docs/quickstart.md`](docs/quickstart.md).

## Before you open a pull request

```bash
gofmt -l $(git diff --name-only --diff-filter=d main | grep '\.go$')
go vet ./...
go build ./...
go test ./...
go test -race ./... # for anything touching goroutines, channels or shared state
```

`gofmt` must be clean for the files you touched — the tree as a whole is not fully
formatted yet, so run it on your diff rather than on `.` and do not reformat unrelated
files in a behavioural PR.

The scheduler, worker manager, lease manager and queues are all concurrent; run the
race detector on any change in those packages.

## Repository layout

| Path | What lives there |
|---|---|
| `cmd/` | `server`, `worker`, `benchmark` entry points |
| `internal/engine` | wiring, lifecycle, leadership watch |
| `internal/scheduler` | dispatch / result / delay / recovery loops |
| `internal/consensus` | Raft node and FSM |
| `internal/store`, `internal/storage` | BoltDB store, WAL, snapshots |
| `internal/rpc` | protobuf, gRPC client and server |
| `internal/executor`, `internal/runner` | worker-side execution pipeline |
| `internal/metrics`, `internal/tracing`, `internal/events` | observability |
| `docs/` | architecture, API, deployment, ADRs, handbook |

Everything is under `internal/` on purpose: Quorum is an application, not a library.

## Code conventions

- Standard `gofmt`; no custom linters configured.
- Structured logging with `log/slog` and key/value pairs
  (`slog.Info("Job completed", "worker_id", id, "job_id", j.ID)`), never `fmt.Printf`.
- Every long-running loop takes a `context.Context` and returns on `ctx.Done()`.
- Exported types and functions get a doc comment starting with the identifier name.
- Errors are wrapped with context: `fmt.Errorf("create tcp transport: %w", err)`.
- Guard shared state with the existing mutexes; do not widen a lock's scope without
  saying why in the PR.
- Comments explain *why*, not what the next line does.

## Testing conventions

- Table-driven tests where there is more than one case.
- Use `store.NewMemoryStore()` and `executor.MockExecutor` rather than touching disk.
- Tracing changes belong in the package's `tracing_test.go`, asserting span names,
  attributes and status with the in-memory recorder in `internal/oteltest`.
- Tests must not depend on wall-clock sleeps where a channel or a synchronous call
  will do; the existing suite runs in seconds and should stay that way.
- New behaviour needs a test. Bug fixes need a test that fails before the fix.

## Changes that need extra care

Some areas have invariants that are easy to break:

- **Job status transitions.** `job.IsValidTransition` is the single source of truth;
  terminal states are terminal. There is deliberately no persisted `RUNNING` status
  (see [`docs/adr/0001`](docs/adr/0001-temporal-desired-state-model.md)).
- **Leases and attempt numbers.** The attempt counter is what makes requeue-after-
  worker-death safe: a result whose attempt does not match the current lease must be
  discarded. Do not remove that check.
- **Leader-only scheduling.** Only the Raft leader dispatches. Anything you start in
  the leader startup sequence must be cancelled on step-down.
- **The FSM.** `internal/consensus/fsm.go` applies replicated commands; it must stay
  deterministic and side-effect free apart from the store write, or nodes diverge.
- **Delivery semantics.** Quorum guarantees at-least-once. A change that quietly
  assumes exactly-once is a bug, not a feature.

## Commits and pull requests

- Present-tense, scoped subject lines: `scheduler: discard stale results by attempt`.
- One logical change per PR.
- The PR description should say what changed, why, and how you verified it. Include
  the output of the tests you ran for behavioural changes.
- Update the docs in the same PR when behaviour changes — `README.md`,
  [`docs/api.md`](docs/api.md) and [`docs/deployment.md`](docs/deployment.md) are
  meant to describe the code as it actually is, including its limitations. The
  README's "Known Limitations" section is the place to record something you chose not
  to fix.
- Architectural decisions get an ADR in `docs/adr/`, numbered sequentially, following
  the format of the existing two.

## Reporting bugs

Include: what you ran, what you expected, what happened, the relevant `slog` output,
and your `go version`. For scheduling or failover issues, `GET /cluster/status`,
`GET /cluster/raft` and the last 50 lines of the control-node log are usually enough
to diagnose.

## Good first issues

The README's [Known Limitations](README.md#known-limitations) list is deliberately a
to-do list. Self-contained starting points:

- Read control-node configuration from the environment so `docker-compose.yml` works
  as written.
- Give `LeaseManager` a `List()` accessor so `GET /jobs/leases` returns real data.
- Parse the `-id` / `-controller` / `-topics` flags in `cmd/worker`, or drop them from
  `docker-compose.yml`.
- Swap the in-memory Raft log store for `raft-boltdb`.
- Add a GitHub Actions workflow running build, vet and test with `-race`.
