package worker

import (
	"fmt"
	"time"
	"quorum/internal/job"
)

type Worker struct {
	ID int
	JobChannel chan job.Job
}

func NewWorker(id int) *Worker {
	return &Worker{
		ID: id,
		JobChannel: make(chan job.Job),
	}
}

func (w *Worker) Start() {
	for{
		job:= <-w.JobChannel
		w.Execute(job)
	}
}

func (w* Worker) Execute(j job.Job){
	j.Status=job.Running
	fmt.Printf("Worker %d is processing Job %d\n",w.ID,j.ID)
	time.Sleep(2*time.Second)
	j.Status=job.Completed
	fmt.Printf("Worker %d completed Job %d\n",w.ID,j.ID)
}