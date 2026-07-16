package main

import (
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/scheduler"
	"quorum/internal/worker"
	"time"
)
func main(){
	q:=queue.NewQueue();
	q.Enqueue(job.Job{
		ID:1,
		Type: "email",
		Priority: 10,
	})
	q.Enqueue(job.Job{
		ID:2,
		Type: "resize-image",
		Priority: 5,
	})
	q.Enqueue(job.Job{
		ID:3,
		Type: "play games",
		Priority: 8,
	})
	q.Enqueue(job.Job{
		ID:4,
		Type: "draw arrows",
		Priority: 3,
	})
	q.Enqueue(job.Job{
		ID:5,
		Type: "study",
		Priority: 1,
	})

	w1:=worker.NewWorker(1)
	w2:=worker.NewWorker(2)

	go w1.Start()
	go w2.Start()

	s:=scheduler.NewScheduler(q, []*worker.Worker{w1,w2})
	go s.Start()

	time.Sleep(15*time.Second)
}