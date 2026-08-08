package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"quorum/internal/config"
	"quorum/internal/consensus"
	"quorum/internal/cron"
	"quorum/internal/dlq"
	"quorum/internal/events"
	"quorum/internal/executor"
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/scheduler"
	"quorum/internal/store"
	"quorum/internal/worker"
	"quorum/internal/workermanager"
)

type Engine struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	PriorityQueue *queue.JobQueue
	DelayQueue    *queue.JobQueue

	Scheduler     *scheduler.Scheduler
	CronScheduler *cron.Scheduler
	WorkerManager *workermanager.Manager
	nextJobID     atomic.Int64
	JobStore      store.Store
	DLQ           *dlq.DeadLetterQueue
	Config        config.Config
	RaftNode      *consensus.RaftNode
}

func New(cfg config.Config) (*Engine, error) {
	var jobStore store.Store
	if cfg.StorageType == "bolt" {
		bs, err := store.NewBoltStore(cfg.StoragePath)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize bolt store: %w", err)
		}
		jobStore = bs
	} else {
		jobStore = store.NewMemoryStore()
	}

	ctx, cancel := context.WithCancel(context.Background())
	priorityQueue := queue.NewJobQueue(jobStore, queue.PriorityComparator)
	delayQueue := queue.NewJobQueue(jobStore, queue.DelayComparator)
	dead := dlq.New()

	wm := workermanager.NewManager()
	s := scheduler.NewScheduler(priorityQueue, delayQueue, wm.Available, jobStore, dead, wm.DeadWorkers, cfg.ResultBuffer, wm.Broker)

	var raftNode *consensus.RaftNode
	if cfg.RaftEnabled {
		fsm := consensus.NewFSM(jobStore)
		rn, err := consensus.NewRaftNode(cfg.RaftNodeID, cfg.RaftAddr, fsm, cfg.RaftDataDir)
		if err != nil {
			slog.Warn("Failed to initialize Raft consensus, continuing without Raft", "error", err)
		} else {
			raftNode = rn
		}
	}

	limiter := executor.NewTokenBucketLimiter(
		cfg.RateLimit,
		cfg.RateBurst,
	)

	for i := 1; i <= cfg.WorkerCount; i++ {
		baseExecutor := &executor.MockExecutor{}
		rateLimited := executor.NewRateLimitedExecutor(
			baseExecutor,
			limiter,
		)
		exec := executor.NewCircuitBreakerExecutor(
			rateLimited,
			cfg.BreakerFailureThreshold,
			cfg.BreakerResetTimeout,
		)
		w := worker.NewWorker(
			i,
			wm.Available,
			s.Results,
			jobStore,
			exec,
		)
		wm.Register(w, fmt.Sprintf("localhost:local-%d", i), "*")
	}

	e := &Engine{
		ctx:           ctx,
		cancel:        cancel,
		PriorityQueue: priorityQueue,
		DelayQueue:    delayQueue,
		Scheduler:     s,
		WorkerManager: wm,
		JobStore:      jobStore,
		DLQ:           dead,
		Config:        cfg,
		RaftNode:      raftNode,
	}

	e.CronScheduler = cron.New(func(jobType string, priority int) error {
		_, submitErr := e.SubmitJob(jobType, priority)
		return submitErr
	})

	return e, nil
}

func (e *Engine) Start() {
	for _, w := range e.WorkerManager.List() {
		e.wg.Add(1)
		go func(wk worker.WorkerClient) {
			defer e.wg.Done()
			wk.Start(e.ctx)
		}(w)
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.WorkerManager.Monitor(e.ctx, e.Config.HeartbeatTimeout)
	}()

	if e.RaftNode != nil {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.watchLeadership()
		}()
	} else {
		// Standalone single-node mode: start scheduler routines immediately
		_ = e.Scheduler.RebuildQueuesFromStore()
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.Scheduler.Start(e.ctx)
		}()
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.CronScheduler.Start(e.ctx)
		}()
	}
}

// watchLeadership monitors Raft leadership changes and executes the Leader Startup Sequence.
func (e *Engine) watchLeadership() {
	leaderCh := e.RaftNode.LeaderCh()
	var schedCancel context.CancelFunc

	for {
		select {
		case <-e.ctx.Done():
			if schedCancel != nil {
				schedCancel()
			}
			return

		case isLeader := <-leaderCh:
			if isLeader {
				slog.Info("Claimed Raft Leadership: executing Leader Startup Sequence...")
				events.Global().Broadcast(events.Event{
					Type:      events.EventLeaderChanged,
					Message:   fmt.Sprintf("Node %s claimed Raft Leadership: executing Leader Startup Sequence...", e.Config.RaftNodeID),
					Timestamp: time.Now(),
				})

				// Phase 1: Rebuild Queues from Store (Desired State)
				if err := e.Scheduler.RebuildQueuesFromStore(); err != nil {
					slog.Error("Queue reconstruction failed on leader claim", "error", err)
				}

				// Phase 2: Worker Re-registration Grace Period (1s)
				time.Sleep(1 * time.Second)

				// Phase 3: Enable Dispatch
				schedCtx, cancel := context.WithCancel(e.ctx)
				schedCancel = cancel

				go e.Scheduler.Start(schedCtx)
				go e.CronScheduler.Start(schedCtx)

				events.Global().Broadcast(events.Event{
					Type:      events.EventDispatchResumed,
					Message:   "Leader Startup Sequence completed: Schedulers & dispatch loops active",
					Timestamp: time.Now(),
				})

				slog.Info("Leader Startup Sequence completed: Schedulers active")
			} else {
				slog.Warn("Stepped down from Raft Leadership: pausing schedulers")
				events.Global().Broadcast(events.Event{
					Type:      events.EventLeaderChanged,
					Message:   fmt.Sprintf("Node %s stepped down from Raft Leadership: pausing schedulers", e.Config.RaftNodeID),
					Timestamp: time.Now(),
				})
				if schedCancel != nil {
					schedCancel()
					schedCancel = nil
				}
				e.Scheduler.Leases.Clear()
			}
		}
	}
}

func (e *Engine) Stop() error {
	e.cancel()
	e.wg.Wait()

	if e.RaftNode != nil {
		_ = e.RaftNode.Close()
	}

	if closer, ok := e.JobStore.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (e *Engine) Restore() error {
	maxID := int64(0)
	for _, j := range e.JobStore.List() {
		if int64(j.ID) > maxID {
			maxID = int64(j.ID)
		}
	}
	e.nextJobID.Store(maxID)
	return e.Scheduler.RebuildQueuesFromStore()
}

func (e *Engine) SubmitJob(jobType string, priority int) (job.Job, error) {
	id := int(e.nextJobID.Add(1))
	j := job.NewJob(id, jobType, priority)

	if e.RaftNode != nil {
		if err := e.RaftNode.ProposeAddJob(j); err != nil {
			return job.Job{}, fmt.Errorf("raft propose add_job failed: %w", err)
		}
	} else {
		if err := e.JobStore.Add(j); err != nil {
			return job.Job{}, err
		}
		e.PriorityQueue.Enqueue(j.ID)
	}

	events.Global().Broadcast(events.Event{
		Type:      events.EventJobSubmitted,
		Message:   fmt.Sprintf("Job #%d (type: %s, priority: %d) submitted to cluster", j.ID, j.Type, j.Priority),
		Timestamp: time.Now(),
	})

	return j, nil
}

func (e *Engine) SubmitJobAt(jobType string, priority int, runAt time.Time) (job.Job, error) {
	id := int(e.nextJobID.Add(1))
	j := job.NewJob(id, jobType, priority)
	j.Status = job.Scheduled
	j.NextRunAt = runAt

	if e.RaftNode != nil {
		if err := e.RaftNode.ProposeAddJob(j); err != nil {
			return job.Job{}, fmt.Errorf("raft propose add_job_at failed: %w", err)
		}
	} else {
		if err := e.JobStore.Add(j); err != nil {
			return job.Job{}, err
		}
		e.DelayQueue.Enqueue(j.ID)
	}

	return j, nil
}

func (e *Engine) Jobs() []job.Job {
	return e.JobStore.List()
}

func (e *Engine) Job(id int) (job.Job, bool) {
	return e.JobStore.Get(id)
}

func (e *Engine) DeleteJob(id int) bool {
	if e.RaftNode != nil {
		_ = e.RaftNode.ProposeDeleteJob(id)
		return true
	}
	ok, _ := e.JobStore.Delete(id)
	return ok
}

func (e *Engine) CancelJob(id int) error {
	if e.RaftNode != nil {
		return e.RaftNode.ProposeCancelJob(id)
	}
	return e.JobStore.Cancel(id)
}

func (e *Engine) DeadJobs() []job.Job {
	dlqJobs, err := e.JobStore.ListDLQ()
	if err != nil || len(dlqJobs) == 0 {
		return e.DLQ.List()
	}
	return dlqJobs
}

func (e *Engine) AddCronJob(id string, schedule string, jobType string, priority int) error {
	c := cron.CronJob{
		ID:       id,
		Schedule: schedule,
		Type:     jobType,
		Priority: priority,
	}

	if e.RaftNode != nil {
		return e.RaftNode.ProposeAddCron(c)
	}
	if err := e.JobStore.AddCron(c); err != nil {
		return err
	}
	return e.CronScheduler.Add(c)
}

func (e *Engine) RemoveCronJob(id string) {
	if e.RaftNode != nil {
		_ = e.RaftNode.ProposeDeleteCron(id)
	} else {
		_, _ = e.JobStore.DeleteCron(id)
		e.CronScheduler.Remove(id)
	}
}

func (e *Engine) ListCronJobs() []cron.CronJob {
	crons, err := e.JobStore.ListCrons()
	if err != nil || len(crons) == 0 {
		return e.CronScheduler.List()
	}
	return crons
}
