package scheduler

import (
	"context"

	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/storage"
	"quorum/internal/store"
	"quorum/internal/worker"
)

type Scheduler struct {
	JobQueue      *queue.JobQueue
	Available     chan *worker.Worker
	Results chan job.Result
	WAL           *storage.WAL
	Store         *store.JobStore
}

func NewScheduler(
	jq *queue.JobQueue,
	available chan *worker.Worker,
	wal *storage.WAL,
	store *store.JobStore,
) *Scheduler {

	return &Scheduler{
		JobQueue:      jq,
		Available:     available,
		Results:         make(chan job.Result),
		WAL:           wal,
		Store:         store,
	}
}

func (s *Scheduler) Dispatch(ctx context.Context, j job.Job) bool {
	select {
	case <-ctx.Done():
		return false
	case worker := <-s.Available:
		select {
		case <-ctx.Done():
			return false

		case worker.JobChannel <- j:
			return true
		}
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case result := <-s.Results:
			if result.Success {
				if err := s.WAL.AppendCompletion(result.JobID); err != nil {
					panic(err)
				}
}
		case <-s.JobQueue.Wait():
			for {
				id, ok := s.JobQueue.Dequeue()
				if !ok {
					break
				}
				stored, ok := s.Store.Get(id)
				if !ok {
					continue
				}

				if stored.Status == job.Cancelled {
					continue
				}

				if ok := s.Dispatch(ctx, stored); !ok {
					return
				}
			}
		}
	}
}

func (s *Scheduler) CreateSnapshot(snapshot *storage.Snapshot, wal *storage.WAL) error {

	jobs := s.Store.List()

	if err := snapshot.Save(jobs); err != nil {
		return err
	}

	return wal.Reset()
}