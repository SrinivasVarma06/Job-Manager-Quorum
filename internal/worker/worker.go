package worker

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"quorum/internal/executor"
	"quorum/internal/job"
	"quorum/internal/runner"
	"quorum/internal/store"
	"quorum/internal/tracing"
)

type Worker struct {
	id         int
	JobChannel chan job.Job
	Available  chan WorkerClient
	Results    chan job.Result
	Store      store.Store
	Runner     *runner.Runner
}

func NewWorker(id int, available chan WorkerClient, results chan job.Result, store store.Store, exec executor.Executor) *Worker {
	return &Worker{
		id:         id,
		JobChannel: make(chan job.Job),
		Available:  available,
		Results:    results,
		Store:      store,
		Runner: runner.New(
			store,
			results,
			exec,
		),
	}
}

func (w *Worker) ID() int {
	return w.id
}

var ErrWorkerBusy = errors.New("worker is busy")

func (w *Worker) Submit(ctx context.Context, j job.Job) error {
	select {
	case <-ctx.Done():
		return ctx.Err()

	case w.JobChannel <- j:
		return nil

	default:
		return ErrWorkerBusy
	}
}

func (w *Worker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case w.Available <- w:
			select {
			case <-ctx.Done():
				return
			case j := <-w.JobChannel:
				_, recvSpan := tracing.Tracer().Start(ctx, "worker.receive_job")
				recvSpan.SetAttributes(
					attribute.Int("worker.id", w.id),
					attribute.Int("job.id", j.ID),
					attribute.String("job.type", j.Type),
				)
				recvSpan.SetStatus(codes.Ok, "")
				recvSpan.End()

				execStart := time.Now()
				_, execSpan := tracing.Tracer().Start(ctx, "worker.execute_job")
				execSpan.SetAttributes(
					attribute.Int("worker.id", w.id),
					attribute.Int("job.id", j.ID),
					attribute.String("job.type", j.Type),
				)

				w.Runner.Execute(w.id, j)

				execSpan.SetAttributes(
					attribute.Int64("execution.duration_ms", time.Since(execStart).Milliseconds()),
				)

				_, completeSpan := tracing.Tracer().Start(ctx, "worker.complete_job")
				completeSpan.SetAttributes(
					attribute.Int("worker.id", w.id),
					attribute.Int("job.id", j.ID),
					attribute.String("job.type", j.Type),
				)
				if stored, ok := w.Store.Get(j.ID); ok {
					switch stored.Status {
					case job.Completed:
						execSpan.SetStatus(codes.Ok, "")
						completeSpan.SetStatus(codes.Ok, "")
					case job.Cancelled:
						err := context.Canceled
						execSpan.RecordError(err)
						execSpan.SetStatus(codes.Error, err.Error())
						completeSpan.RecordError(err)
						completeSpan.SetStatus(codes.Error, err.Error())
					default:
						err := errors.New(stored.LastError)
						if stored.LastError == "" {
							err = errors.New("job execution failed")
						}
						execSpan.RecordError(err)
						execSpan.SetStatus(codes.Error, err.Error())
						completeSpan.RecordError(err)
						completeSpan.SetStatus(codes.Error, err.Error())
					}
				} else {
					execSpan.SetStatus(codes.Ok, "")
					completeSpan.SetStatus(codes.Ok, "")
				}
				execSpan.End()
				completeSpan.End()
			}
		}
	}
}
