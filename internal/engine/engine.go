package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/scheduler"
	"quorum/internal/storage"
	"quorum/internal/worker"
	"quorum/internal/workermanager"
	"quorum/internal/store"
)

type Engine struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	Queue         *queue.JobQueue
	WAL           *storage.WAL
	Scheduler     *scheduler.Scheduler
	WorkerManager *workermanager.Manager
	nextJobID atomic.Int64
	JobStore *store.JobStore
}

func New() (*Engine, error) {
	jobStore := store.NewJobStore()	
	ctx, cancel := context.WithCancel(context.Background())
	q := queue.NewJobQueue(jobStore)

	wal, err := storage.NewWal("jobs.log")
	if err != nil {
		cancel()
		return nil, err
	}
	wm := workermanager.NewManager()
	s := scheduler.NewScheduler(q, wm.Available, wal, jobStore)

	w1 := worker.NewWorker(1, wm.Available, s.Results, jobStore)
	w2 := worker.NewWorker(2, wm.Available, s.Results, jobStore)
	wm.Register(w1)
	wm.Register(w2)

	e := &Engine{
		ctx:           ctx,
		cancel:        cancel,
		Queue:         q,
		WAL:           wal,
		Scheduler:     s,
		WorkerManager: wm,
		JobStore: jobStore,
	}

	return e, nil
}

func (e *Engine) Start() {
	for _, w := range e.WorkerManager.List() {
		e.wg.Add(1)
		go func(wk *worker.Worker) {
			defer e.wg.Done()
			wk.Start(e.ctx)
		}(w)
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.Scheduler.Start(e.ctx)
	}()
}

func (e *Engine) Stop() error {
	e.cancel()
	e.wg.Wait()
	return e.WAL.Close()
}

func (e *Engine) Restore() error {
	recoveredJobs, err := e.WAL.Replay()
	if err != nil {
		return err
	}
	maxID := int64(0)
	for _, j := range recoveredJobs {
		e.Queue.Enqueue(j.ID)
		if int64(j.ID) > maxID {
			maxID = int64(j.ID)
		}
	}
	e.nextJobID.Store(maxID)
	return nil
}

func (e *Engine) SubmitJob(jobType string, priority int) (job.Job, error) {
	id := int(e.nextJobID.Add(1))
	j := job.NewJob(id, jobType, priority)
	if err := e.WAL.Append(j); err != nil {
		return job.Job{},err
	}
	e.JobStore.Add(j)
	e.Queue.Enqueue(j.ID)
	return j,nil
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
	return e.JobStore.Cancel(id)
}