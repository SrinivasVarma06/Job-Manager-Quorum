package runner

import (
	"fmt"

	"quorum/internal/executor"
	"quorum/internal/job"
	"quorum/internal/store"
)

type Runner struct {
	Store    *store.JobStore
	Results  chan job.Result
	Executor executor.Executor
}

func New(
	store *store.JobStore,
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
	fmt.Printf(
		"Worker %d is processing Job %d\n",
		workerID,
		j.ID,
	)
	err := r.Executor.Execute(j)
	if err != nil {
		j.Status = job.Failed
		j.LastError = err.Error()
		r.Store.Update(j)

		fmt.Printf(
			"Worker %d failed Job %d: %v\n",
			workerID,
			j.ID,
			err,
		)

		r.Results <- job.Result{
			JobID:   j.ID,
			Success: false,
			Error:   err,
		}
		return
	}
	j.Status = job.Completed
	r.Store.Update(j)
	fmt.Printf(
		"Worker %d completed Job %d\n",
		workerID,
		j.ID,
	)
	r.Results <- job.Result{
		JobID:   j.ID,
		Success: true,
	}
}