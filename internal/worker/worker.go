package worker

import (
	"context"
	"fmt"
	"quorum/internal/job"
	"quorum/internal/store"
	"quorum/internal/executor"
)

type Worker struct {
	ID         int
	JobChannel chan job.Job
	Available  chan *Worker
	Results    chan job.Result
	Store      *store.JobStore
	Executor   executor.Executor
}

func NewWorker(id int, available chan *Worker, results chan job.Result, store *store.JobStore, exec executor.Executor) *Worker {
	return &Worker{
		ID:         id,
		JobChannel: make(chan job.Job),
		Available:  available,
		Results:    results,
		Store:      store,
		Executor:   exec,
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
			case job := <-w.JobChannel:
				w.Execute(job)
			}
		}
	}
}

func (w *Worker) Execute(j job.Job) {
	j.Status = job.Running
	w.Store.Update(j)
	fmt.Printf("Worker %d is processing Job %d\n", w.ID, j.ID)

	err := w.Executor.Execute(j)
	if err != nil {
		j.Status = job.Failed
		j.LastError = err.Error()
		w.Store.Update(j)
		fmt.Printf("Worker %d failed Job %d: %v\n", w.ID, j.ID, err)
		w.Results <- job.Result{
			JobID:   j.ID,
			Success: false,
			Error:   err,
		}
		return
	}

	j.Status = job.Completed
	w.Store.Update(j)
	fmt.Printf("Worker %d completed Job %d\n", w.ID, j.ID)
	w.Results <- job.Result{
		JobID:   j.ID,
		Success: true,
	}
}
