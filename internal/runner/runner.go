package runner

import (
	"log/slog"

	"quorum/internal/executor"
	"quorum/internal/job"
	"quorum/internal/store"
)

type Runner struct {
	Store    store.Store
	Results  chan job.Result
	Executor executor.Executor
}

func New(
	store store.Store,
	results chan job.Result,
	exec executor.Executor,
) *Runner {
	return &Runner{
		Store:    store,
		Results:  results,
		Executor: exec,
	}
}

func (r *Runner) Execute(workerID int, j job.Job) {
	slog.Info("Executing job", "worker_id", workerID, "job_id", j.ID, "type", j.Type)

	err := r.Executor.Execute(j)
	if err != nil {
		j.Status = job.Failed
		j.LastError = err.Error()
		if r.Store != nil {
			r.Store.Update(j)
		}

		slog.Warn("Job failed", "worker_id", workerID, "job_id", j.ID, "error", err)

		r.Results <- job.Result{
			JobID:   j.ID,
			Success: false,
			Error:   err,
		}
		return
	}

	j.Status = job.Completed
	if r.Store != nil {
		r.Store.Update(j)
	}

	slog.Info("Job completed", "worker_id", workerID, "job_id", j.ID)

	r.Results <- job.Result{
		JobID:   j.ID,
		Success: true,
	}
}