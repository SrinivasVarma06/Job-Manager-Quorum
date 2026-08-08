package worker

import (
	"context"
	"quorum/internal/executor"
	"quorum/internal/job"
	"quorum/internal/store"
	"errors"
	"quorum/internal/runner"
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
				w.Runner.Execute(w.id, j)
			}
		}
	}
}
