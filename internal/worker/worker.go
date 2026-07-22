package worker

import (
	"context"
	"fmt"
	"time"
	"quorum/internal/job"
	"quorum/internal/store"
)

type Worker struct {
	ID            int
	JobChannel    chan job.Job
	Available     chan *Worker
	Results 	  chan job.Result
	Store *store.JobStore
}

func NewWorker(id int, available chan *Worker, results chan job.Result, store *store.JobStore) *Worker {
	return &Worker{
		ID:            id,
		JobChannel:    make(chan job.Job),
		Available:     available,
		Results: 	  results,
		Store: store,
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
	time.Sleep(10 * time.Second)
	if j.ID%2 == 0 {
		j.Status = job.Failed
		w.Store.Update(j)
		fmt.Printf("Worker %d failed Job %d\n", w.ID, j.ID)
		w.Results <- job.Result{
			JobID:   j.ID,
			Success: false,
			Error:   fmt.Errorf("simulated failure"),
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
