package main

import (
	"context"
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/scheduler"
	"quorum/internal/worker"
	"time"
	"quorum/internal/storage"
	"fmt"
)

func main(){
	ctx,cancel:=context.WithCancel(context.Background())
	defer cancel()
	q:=queue.NewJobQueue();
	wal,err:=storage.NewWal("jobs.log")
	if err!=nil{
		panic(err)
	}
	recoveredJobs, err := wal.Replay()
	for _, j := range recoveredJobs {
		q.Enqueue(j)
	}
	s:=scheduler.NewScheduler(q, nil)

	w1 := worker.NewWorker(1,s.AvailableWorkers,s.CompletedJobs)
	w2 := worker.NewWorker(2,s.AvailableWorkers,s.CompletedJobs)
	s.Workers=[]*worker.Worker{w1,w2}

	go w1.Start(ctx)
	go w2.Start(ctx)

	go s.Start(ctx)

	j1 := job.Job{
		ID:       1,
		Type:     "email",
		Priority: 10,
		Status:   job.Pending,
	}
	j2 := job.Job{
		ID:       2,
		Type:     "play games",
		Priority: 5,
		Status:   job.Pending,
	}
	j3 := job.Job{
		ID:       3,
		Type:     "study",
		Priority: 1,
		Status:   job.Pending,
	}
	newJobs := []job.Job{j1, j2, j3}

	for _, j := range newJobs {
		if err := wal.Append(j); err != nil {
			panic(err)
		}
		q.Enqueue(j)
	}
	fmt.Printf("Recovered %d jobs\n", len(recoveredJobs))
	time.Sleep(10 * time.Second)
	cancel()
	time.Sleep(1*time.Second)
}