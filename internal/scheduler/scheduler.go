package scheduler

import (
	"context"
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/worker"
	"quorum/internal/storage"
)

type Scheduler struct {
	JobQueue   *queue.JobQueue
	Workers []*worker.Worker
	AvailableWorkers chan *worker.Worker
	CompletedJobs    chan int
}

func NewScheduler(jq *queue.JobQueue, workers []*worker.Worker) *Scheduler {
	return &Scheduler{
		JobQueue:         jq,
		Workers:          workers,
		AvailableWorkers: make(chan *worker.Worker),
		CompletedJobs:    make(chan int),
	}
}

func (s *Scheduler) Dispatch(job job.Job) {
	worker := <-s.AvailableWorkers
	worker.JobChannel <- job
}

func (s *Scheduler) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-s.CompletedJobs:
			s.JobQueue.Complete(id)
		case job := <-s.JobQueue.Channel:
			s.Dispatch(job)
		}
	}
}

func (s *Scheduler) CreateSnapshot(snapshot *storage.Snapshot, wal *storage.WAL) error{
	jobs := s.JobQueue.Snapshot()

	if err := snapshot.Save(jobs); err != nil {
		return err
	}

	return wal.Reset()
}