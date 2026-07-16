package scheduler

import (
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/worker"
	"time"
)

type Scheduler struct {
	Queue   *queue.Queue
	Workers []*worker.Worker
	nextWorker int
}

func NewScheduler(q *queue.Queue,workers []*worker.Worker) *Scheduler{
	return &Scheduler{
		Queue:q,
		Workers: workers,
		nextWorker: 0,
	}
}

func (s *Scheduler) Dispatch(job job.Job){
	worker:=s.Workers[s.nextWorker]
	worker.JobChannel <- job
	s.nextWorker=(s.nextWorker+1)%len(s.Workers)
}

func (s *Scheduler) Start(){
	for{
		job,ok:=s.Queue.Dequeue()
		if !ok{
			time.Sleep(500*time.Millisecond)
			continue
		}
		s.Dispatch(job)
	}
}