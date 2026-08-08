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
	"quorum/internal/executor"
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/scheduler"
	"quorum/internal/storage"
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

	WAL           *storage.WAL
	Snapshot      *storage.Snapshot
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

	wal, err := storage.NewWal("jobs.log")
	if err != nil {
		cancel()
		if closer, ok := jobStore.(io.Closer); ok {
			_ = closer.Close()
		}
		return nil, err
	}
	snapshot := storage.NewSnapshot("snapshot.json")
	wm := workermanager.NewManager()
	s := scheduler.NewScheduler(priorityQueue, delayQueue, wm.Available, wal, jobStore, dead, wm.DeadWorkers, cfg.ResultBuffer, wm.Broker)

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
		WAL:           wal,
		Snapshot:      snapshot,
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
		e.Scheduler.Start(e.ctx)
	}()
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.CronScheduler.Start(e.ctx)
	}()
	// Monitor detects heartbeat timeouts and sends dead worker IDs to
	// DeadWorkers channel. The scheduler's recoveryLoop reads from that
	// channel and requeues any in-flight jobs assigned to the dead worker.
	go e.WorkerManager.Monitor(e.ctx, e.Config.HeartbeatTimeout)
}

func (e *Engine) Stop() error {
	e.cancel()
	e.wg.Wait()

	if e.RaftNode != nil {
		_ = e.RaftNode.Close()
	}

	compactErr := e.Scheduler.CreateSnapshot(e.Snapshot, e.WAL)
	closeErr := e.WAL.Close()

	if closer, ok := e.JobStore.(io.Closer); ok {
		if err := closer.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}

	if compactErr != nil && closeErr != nil {
		return fmt.Errorf("snapshot compaction failed: %v; wal close failed: %v", compactErr, closeErr)
	}
	if compactErr != nil {
		return compactErr
	}
	return closeErr
}

func (e *Engine) Restore() error {
	snapshotJobs, err := e.Snapshot.Load()
	if err != nil {
		return err
	}

	walJobs, err := e.WAL.Replay()
	if err != nil {
		return err
	}

	recoveredJobs := make(map[int]job.Job, len(snapshotJobs)+len(walJobs))
	for _, j := range snapshotJobs {
		recoveredJobs[j.ID] = j
	}
	for _, j := range walJobs {
		recoveredJobs[j.ID] = j
	}

	for _, recovered := range recoveredJobs {
		j := normalizeRecoveredJob(recovered)
		if (j.Status == job.Retrying || j.Status == job.Scheduled) && !j.NextRunAt.After(time.Now()) {
			j.Status = job.Pending
		}
		e.JobStore.Add(j)
	}

	// Scan all jobs in JobStore (including persistent BoltStore state)
	maxID := int64(0)
	for _, j := range e.JobStore.List() {
		j = normalizeRecoveredJob(j)
		if (j.Status == job.Retrying || j.Status == job.Scheduled) && !j.NextRunAt.After(time.Now()) {
			j.Status = job.Pending
		}
		e.JobStore.Update(j)

		if int64(j.ID) > maxID {
			maxID = int64(j.ID)
		}
		enqueueRecoveredJob(e.PriorityQueue, e.DelayQueue, j)
	}
	e.nextJobID.Store(maxID)
	return nil
}

func (e *Engine) SubmitJob(jobType string, priority int) (job.Job, error) {
	id := int(e.nextJobID.Add(1))
	j := job.NewJob(id, jobType, priority)
	if err := e.WAL.Append(j); err != nil {
		return job.Job{}, err
	}
	e.JobStore.Add(j)
	e.PriorityQueue.Enqueue(j.ID)
	return j, nil
}

func (e *Engine) SubmitJobAt(jobType string, priority int, runAt time.Time) (job.Job, error) {
	id := int(e.nextJobID.Add(1))
	j := job.NewJob(id, jobType, priority)
	j.Status = job.Scheduled
	j.NextRunAt = runAt

	if err := e.WAL.Append(j); err != nil {
		return job.Job{}, err
	}
	e.JobStore.Add(j)
	e.DelayQueue.Enqueue(j.ID)
	return j, nil
}

func (e *Engine) Jobs() []job.Job {
	return e.JobStore.List()
}

func (e *Engine) Job(id int) (job.Job, bool) {
	return e.JobStore.Get(id)
}

func (e *Engine) DeleteJob(id int) bool {
	return e.JobStore.Delete(id)
}

func (e *Engine) CancelJob(id int) error {
	if err := e.JobStore.Cancel(id); err != nil {
		return err
	}
	return e.WAL.AppendCancel(id)
}

func (e *Engine) DeadJobs() []job.Job {
	return e.DLQ.List()
}

func (e *Engine) AddCronJob(id string, schedule string, jobType string, priority int) error {
	return e.CronScheduler.Add(cron.CronJob{
		ID:       id,
		Schedule: schedule,
		Type:     jobType,
		Priority: priority,
	})
}

func (e *Engine) RemoveCronJob(id string) {
	e.CronScheduler.Remove(id)
}

func (e *Engine) ListCronJobs() []cron.CronJob {
	return e.CronScheduler.List()
}

func normalizeRecoveredJob(j job.Job) job.Job {
	if j.Status == job.Running {
		j.Status = job.Pending
		j.WorkerID = 0
	}
	return j
}

func enqueueRecoveredJob(priorityQueue *queue.JobQueue, delayQueue *queue.JobQueue, j job.Job) {
	switch j.Status {
	case job.Pending:
		priorityQueue.Enqueue(j.ID)
	case job.Retrying, job.Scheduled:
		delayQueue.Enqueue(j.ID)
	}
}
