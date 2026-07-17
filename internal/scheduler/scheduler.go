package scheduler

import (
	"context"
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/storage"
	"quorum/internal/worker"
)

type Scheduler struct {
	JobQueue      *queue.JobQueue
	Available     chan *worker.Worker
	CompletedJobs chan int
	WAL           *storage.WAL
}

func NewScheduler(jq *queue.JobQueue, available chan *worker.Worker, wal *storage.WAL) *Scheduler {
	return &Scheduler{
		JobQueue:      jq,
		Available:     available,
		CompletedJobs: make(chan int, 100),
		WAL:           wal,
	}
}

func (s *Scheduler) Dispatch(ctx context.Context, job job.Job) bool {
	select {
	case <-ctx.Done():
		return false
	case worker := <-s.Available:
		select {
		case <-ctx.Done():
			return false
		case worker.JobChannel <- job:
			return true
		}
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-s.CompletedJobs:
			s.JobQueue.Complete(id)
			if err := s.WAL.AppendCompletion(id); err != nil {
				panic(err)
			}
		case job := <-s.JobQueue.Channel:
			if ok := s.Dispatch(ctx, job); !ok {
				return
			}
		}
	}
}

func (s *Scheduler) CreateSnapshot(snapshot *storage.Snapshot, wal *storage.WAL) error {
	jobs := s.JobQueue.Snapshot()

	if err := snapshot.Save(jobs); err != nil {
		return err
	}

	return wal.Reset()
}
