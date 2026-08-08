package runner

import (
	"context"
	"log/slog"
	"sync"

	"quorum/internal/executor"
	"quorum/internal/job"
	"quorum/internal/store"
)

type Runner struct {
	Store       store.Store
	Results     chan job.Result
	Executor    executor.Executor
	mu          sync.Mutex
	activeTasks map[int]context.CancelFunc // jobID -> cancel function
}

func New(
	s store.Store,
	results chan job.Result,
	exec executor.Executor,
) *Runner {
	return &Runner{
		Store:       s,
		Results:     results,
		Executor:    exec,
		activeTasks: make(map[int]context.CancelFunc),
	}
}

// Cancel cancels the active execution context of a running job on this worker.
func (r *Runner) Cancel(jobID int) bool {
	r.mu.Lock()
	cancel, ok := r.activeTasks[jobID]
	if ok {
		delete(r.activeTasks, jobID)
	}
	r.mu.Unlock()

	if ok && cancel != nil {
		cancel()
		slog.Info("Cancelled active job execution", "job_id", jobID)
		return true
	}
	return false
}

func (r *Runner) sendResult(result job.Result) {
	select {
	case r.Results <- result:
	default:
		slog.Error("Results channel full, dropping job result",
			"job_id", result.JobID,
			"success", result.Success)
	}
}

func (r *Runner) Execute(workerID int, j job.Job) {
	ctx, cancel := context.WithCancel(context.Background())

	r.mu.Lock()
	r.activeTasks[j.ID] = cancel
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.activeTasks, j.ID)
		r.mu.Unlock()
		cancel()
	}()

	slog.Info("Executing job", "worker_id", workerID, "job_id", j.ID, "type", j.Type)

	err := r.Executor.Execute(ctx, j)
	if err != nil {
		j.Status = job.Failed
		j.LastError = err.Error()
		if r.Store != nil {
			_ = r.Store.Update(j)
		}

		slog.Warn("Job failed", "worker_id", workerID, "job_id", j.ID, "error", err)

		r.sendResult(job.Result{
			JobID:   j.ID,
			Success: false,
			Error:   err,
		})
		return
	}

	j.Status = job.Completed
	if r.Store != nil {
		_ = r.Store.Update(j)
	}

	slog.Info("Job completed", "worker_id", workerID, "job_id", j.ID)

	r.sendResult(job.Result{
		JobID:   j.ID,
		Success: true,
	})
}