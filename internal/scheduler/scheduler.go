package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"quorum/internal/broker"
	"quorum/internal/config"
	"quorum/internal/dlq"
	"quorum/internal/events"
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/retry"
	"quorum/internal/store"
	"quorum/internal/worker"
)

type Scheduler struct {
	PriorityQueue *queue.JobQueue
	DelayQueue    *queue.JobQueue
	Available     chan worker.WorkerClient
	Results       chan job.Result
	Store         store.Store
	DLQ           *dlq.DeadLetterQueue
	DeadWorkers   <-chan int
	Broker        *broker.Broker
	Leases        *job.LeaseManager
}

func NewScheduler(
	priorityQueue *queue.JobQueue,
	delayQueue *queue.JobQueue,
	available chan worker.WorkerClient,
	st store.Store,
	dlq *dlq.DeadLetterQueue,
	deadWorkers <-chan int,
	resultBuffer int,
	br *broker.Broker,
) *Scheduler {
	return &Scheduler{
		PriorityQueue: priorityQueue,
		DelayQueue:    delayQueue,
		Available:     available,
		Results:       make(chan job.Result, resultBuffer),
		Store:         st,
		DLQ:           dlq,
		DeadWorkers:   deadWorkers,
		Broker:        br,
		Leases:        job.NewLeaseManager(),
	}
}

// RebuildQueuesFromStore queries Pending and Scheduled jobs from Store (O(k))
// and populates PriorityQueue and DelayQueue upon Raft leadership claim.
func (s *Scheduler) RebuildQueuesFromStore() error {
	pendingJobs, err := s.Store.ListByStatus(job.Pending)
	if err != nil {
		return err
	}
	for _, j := range pendingJobs {
		s.PriorityQueue.Enqueue(j.ID)
	}

	scheduledJobs, err := s.Store.ListByStatus(job.Scheduled)
	if err != nil {
		return err
	}
	for _, j := range scheduledJobs {
		s.DelayQueue.Enqueue(j.ID)
	}

	events.Global().Broadcast(events.Event{
		Type:      events.EventQueueRebuilt,
		Message:   fmt.Sprintf("Rebuilt queues from BoltDB status buckets: %d pending, %d scheduled", len(pendingJobs), len(scheduledJobs)),
		Timestamp: time.Now(),
	})

	slog.Info("Rebuilt scheduler queues from store", "pending_count", len(pendingJobs), "scheduled_count", len(scheduledJobs))
	return nil
}

func (s *Scheduler) Dispatch(ctx context.Context, j job.Job) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}

	var w worker.WorkerClient
	var ok bool

	if s.Broker != nil {
		w, ok = s.Broker.SelectWorker(j.Type, s.Available)
	} else {
		select {
		case w = <-s.Available:
			ok = true
		default:
			ok = false
		}
	}

	if !ok {
		// No worker available, re-enqueue
		s.PriorityQueue.Enqueue(j.ID)
		return false
	}

	// Acquire ephemeral execution lease on active leader
	lease, _ := s.Leases.Get(j.ID)
	attempt := 1
	if lease != nil {
		attempt = lease.Attempt + 1
	}

	// Note: Term can be set from Raft leader term when available
	s.Leases.Acquire(j.ID, w.ID(), 1, attempt)

	events.Global().Broadcast(events.Event{
		Type:      events.EventLeaseGranted,
		Message:   fmt.Sprintf("Ephemeral lease granted for Job #%d to Worker-%d (attempt: %d)", j.ID, w.ID(), attempt),
		Timestamp: time.Now(),
	})

	err := w.Submit(ctx, j)
	if err != nil {
		slog.Warn("Failed to dispatch job to worker, re-queueing", "job_id", j.ID, "worker_id", w.ID(), "error", err)
		s.Leases.Release(j.ID)
		s.PriorityQueue.Enqueue(j.ID)
		return false
	}

	return true
}

func (s *Scheduler) Start(ctx context.Context) {
	go s.dispatchLoop(ctx)
	go s.resultLoop(ctx)
	go s.delayLoop(ctx)
	go s.recoveryLoop(ctx)
	<-ctx.Done()
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
		_ = s.Store.Update(j)
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

				if j.Status == job.Cancelled || j.Status == job.Completed || j.Status == job.Failed {
					continue
				}

				if ok := s.Dispatch(ctx, j); !ok {
					if ctx.Err() != nil {
						return
					}
					continue
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

		case result, ok := <-s.Results:
			if !ok {
				return
			}

			j, ok := s.Store.Get(result.JobID)
			if !ok {
				continue
			}

			lease, hasLease := s.Leases.Get(result.JobID)
			if hasLease && result.Attempt > 0 && result.Attempt != lease.Attempt {
				slog.Warn("Discarding stale result from old attempt",
					"job_id", result.JobID,
					"result_attempt", result.Attempt,
					"lease_attempt", lease.Attempt)
				continue
			}

			s.Leases.Release(result.JobID)

			if result.Success {
				j.Status = job.Completed
				if err := s.Store.Update(j); err != nil {
					slog.Error("Failed to mark job completed", "job_id", j.ID, "error", err)
				}
				events.Global().Broadcast(events.Event{
					Type:      events.EventLeaseReleased,
					Message:   fmt.Sprintf("Job #%d executed successfully; lease released", j.ID),
					Timestamp: time.Now(),
				})
				continue
			}

			if retry.ShouldRetry(j) {
				j.RetryCount++
				j.Status = job.Scheduled
				j.LastError = result.Error.Error()
				j.NextRunAt = retry.NextRetryTime(j, config.Default().MaxBackoff)

				if err := s.Store.Update(j); err != nil {
					slog.Error("Failed to update retry job", "job_id", j.ID, "error", err)
				}
				s.DelayQueue.Enqueue(j.ID)
				continue
			}

			j.Status = job.Failed
			j.LastError = result.Error.Error()
			_ = s.Store.Update(j)
			_ = s.Store.AddDLQ(j)
			s.DLQ.Add(j)
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
			releasedJobIDs := s.Leases.ReleaseByWorker(workerID)
			slog.Warn("Processing failover for dead worker", "worker_id", workerID, "released_leases_count", len(releasedJobIDs))
			for _, jobID := range releasedJobIDs {
				if j, ok := s.Store.Get(jobID); ok && j.Status == job.Pending {
					s.PriorityQueue.Enqueue(jobID)
					slog.Info("Recovered lease from dead worker, re-queued", "job_id", jobID, "dead_worker_id", workerID)
				}
			}
		}
	}
}