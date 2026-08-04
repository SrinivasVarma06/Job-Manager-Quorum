package scheduler

import (
	"context"
	"quorum/internal/config"
	"quorum/internal/dlq"
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/retry"
	"quorum/internal/storage"
	"quorum/internal/store"
	"quorum/internal/worker"
	"time"
)

type Scheduler struct {
	PriorityQueue *queue.JobQueue
	DelayQueue    *queue.JobQueue
	Available     chan worker.WorkerClient
	Results       chan job.Result
	WAL           *storage.WAL
	Store         *store.JobStore
	DLQ		   *dlq.DeadLetterQueue
	DeadWorkers <-chan int
}

func NewScheduler(
	priorityQueue *queue.JobQueue, 
	delayQueue *queue.JobQueue, 
	available chan worker.WorkerClient, 
	wal *storage.WAL, store *store.JobStore, 
	dlq *dlq.DeadLetterQueue, 
	deadWorkers <-chan int,
	resultBuffer int,
	) *Scheduler {
	return &Scheduler{
		PriorityQueue: priorityQueue,
		DelayQueue:    delayQueue,
		Available:     available,
		Results:       make(chan job.Result, resultBuffer),
		WAL:           wal,
		Store:         store,
		DLQ:           dlq,
		DeadWorkers:   deadWorkers,
	}
}

func (s *Scheduler) Dispatch(ctx context.Context, j job.Job) bool {
	select {
	case <-ctx.Done():
		return false
	case w := <-s.Available:
		j.WorkerID = w.ID()
		j.Status = job.Running
		s.Store.Update(j)

		select {
		case <-ctx.Done():
			return false
		default:
			err := w.Submit(ctx, j)
			if err != nil {
				return false
			}
			return true
		}
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	go s.dispatchLoop(ctx)
	go s.resultLoop(ctx)
	go s.delayLoop(ctx)
	go s.recoveryLoop(ctx)
	<-ctx.Done()
}

func (s *Scheduler) CreateSnapshot(snapshot *storage.Snapshot, wal *storage.WAL) error {
	jobs := s.Store.List()
	if err := snapshot.Save(jobs); err != nil {
		return err
	}

	return wal.Reset()
}

func (s *Scheduler) ProcessDelayedJobs() {
	for {
		id, ok := s.DelayQueue.Peek()
		if !ok {
			return
		}
		j, ok := s.Store.Get(id)
		if !ok {
			s.DelayQueue.Dequeue()
			continue
		}

		if j.NextRunAt.After(time.Now()) {
			return
		}
		s.DelayQueue.Dequeue()
		j.Status = job.Pending
		s.Store.Update(j)
		s.PriorityQueue.Enqueue(id)
	}
}

func (s *Scheduler) dispatchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-s.PriorityQueue.Wait():
			for {
				id, ok := s.PriorityQueue.Dequeue()
				if !ok {
					break
				}

				j, ok := s.Store.Get(id)
				if !ok {
					continue
				}

				if j.Status == job.Cancelled {
					continue
				}

				if ok := s.Dispatch(ctx, j); !ok {
					return
				}
			}
		}
	}
}

func (s *Scheduler) resultLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case result := <-s.Results:
			j, ok := s.Store.Get(result.JobID)
			if !ok {
				continue
			}

			if result.Success {
				if err := s.WAL.AppendCompletion(result.JobID); err != nil {
					panic(err)
				}
				continue
			}

			if retry.ShouldRetry(j) {
				j.RetryCount++
				j.Status = job.Retrying
				j.LastError = result.Error.Error()
				j.NextRunAt = retry.NextRetryTime(j, config.Default().MaxBackoff)

				s.Store.Update(j)

				if err := s.WAL.AppendRetry(j); err != nil {
					panic(err)
				}
				s.DelayQueue.Enqueue(j.ID)
				continue
			}

			j.Status = job.Failed
			j.LastError = result.Error.Error()
			s.Store.Update(j)
			s.DLQ.Add(j)

			if err := s.WAL.AppendFailure(j); err != nil {
				panic(err)
			}
		}
	}
}

func (s *Scheduler) delayLoop(ctx context.Context) {
	for {
		id, ok := s.DelayQueue.Peek()
		if !ok {
			select {
			case <-ctx.Done():
				return

			case <-s.DelayQueue.Wait():
				continue
			}
		}

		j, ok := s.Store.Get(id)
		if !ok {
			s.DelayQueue.Dequeue()
			continue
		}
		wait := time.Until(j.NextRunAt)
		if wait <= 0 {
			s.ProcessDelayedJobs()
			continue
		}

		timer := time.NewTimer(wait)

		select {
		case <-ctx.Done():
			timer.Stop()
			return

		case <-timer.C:
			s.ProcessDelayedJobs()

		case <-s.DelayQueue.Wait():
			timer.Stop()
		}
	}
}

func (s *Scheduler) recoveryLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case workerID := <-s.DeadWorkers:
			jobs := s.Store.RunningJobs(workerID)
			for _, j := range jobs {
				j.Status = job.Pending
				j.WorkerID = 0
				s.Store.Update(j)
				s.PriorityQueue.Enqueue(j.ID)
			}
		}
	}
}