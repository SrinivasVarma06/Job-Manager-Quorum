package worker

import (
	"context"
	"fmt"
	"quorum/internal/job"
	"time"
)

type Worker struct {
	ID            int
	JobChannel    chan job.Job
	Available     chan *Worker
	CompletedJobs chan int
}

func NewWorker(id int, available chan *Worker, completed chan int) *Worker {
	return &Worker{
		ID:            id,
		JobChannel:    make(chan job.Job),
		Available:     available,
		CompletedJobs: completed,
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
	fmt.Printf("Worker %d is processing Job %d\n", w.ID, j.ID)
	time.Sleep(2 * time.Second)
	j.Status = job.Completed
	fmt.Printf("Worker %d completed Job %d\n", w.ID, j.ID)
	w.CompletedJobs <- j.ID
}
