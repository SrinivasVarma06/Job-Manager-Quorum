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
)

type Engine struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	Queue         *queue.JobQueue
	WAL           *storage.WAL
	Scheduler     *scheduler.Scheduler
	Workers       []*worker.Worker
	WorkerManager *workermanager.Manager
	nextJobID atomic.Int64
}

func New() (*Engine, error) {

	ctx, cancel := context.WithCancel(context.Background())
	q := queue.NewJobQueue()

	wal, err := storage.NewWal("jobs.log")
	if err != nil {
		cancel()
		return nil, err
	}
	wm := workermanager.NewManager()
	s := scheduler.NewScheduler(q, wm.Available, wal)

	w1 := worker.NewWorker(1, wm.Available, s.CompletedJobs)
	w2 := worker.NewWorker(2, wm.Available, s.CompletedJobs)
	wm.Register(w1)
	wm.Register(w2)

	workers := []*worker.Worker{w1, w2}

	e := &Engine{
		ctx:           ctx,
		cancel:        cancel,
		Queue:         q,
		WAL:           wal,
		Scheduler:     s,
		Workers:       workers,
		WorkerManager: wm,
	}

	return e, nil
}

func (e *Engine) Start() {
	for _, w := range e.Workers {
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
		e.Queue.Enqueue(j)
		if int64(j.ID) > maxID {
			maxID = int64(j.ID)
		}
	}
	e.nextJobID.Store(maxID)
	return nil
}

func (e *Engine) SubmitJob(jobType string, priority int) error {
	id := int(e.nextJobID.Add(1))
	j := job.NewJob(id, jobType, priority)
	if err := e.WAL.Append(j); err != nil {
		return err
	}
	e.Queue.Enqueue(j)
	return nil
}
