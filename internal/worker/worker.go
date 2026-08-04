package worker

import (
	"context"
	"fmt"
	"quorum/internal/executor"
	"quorum/internal/job"
	"quorum/internal/store"
	"errors"
)

type Worker struct {
	id         int
	JobChannel chan job.Job
	Available  chan WorkerClient
	Results    chan job.Result
	Store      *store.JobStore
	Executor   executor.Executor
}

func NewWorker(id int, available chan WorkerClient, results chan job.Result, store *store.JobStore, exec executor.Executor) *Worker {
	return &Worker{
		id:         id,
		JobChannel: make(chan job.Job),
		Available:  available,
		Results:    results,
		Store:      store,
		Executor:   exec,
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
				w.Execute(j)
			}
		}
	}
}

func (w *Worker) Execute(j job.Job) {
	fmt.Printf("Worker %d is processing Job %d\n", w.id, j.ID)
	err := w.Executor.Execute(j)
	if err != nil {
		j.Status = job.Failed
		j.LastError = err.Error()
		w.Store.Update(j)
		fmt.Printf("Worker %d failed Job %d: %v\n", w.id, j.ID, err)
		w.Results <- job.Result{
			JobID:   j.ID,
			Success: false,
			Error:   err,
		}
		return
	}

	j.Status = job.Completed
	w.Store.Update(j)
	fmt.Printf("Worker %d completed Job %d\n", w.id, j.ID)
	w.Results <- job.Result{
		JobID:   j.ID,
		Success: true,
	}
}
