package executor

import "quorum/internal/job"

type Executor interface {
	Execute(job.Job) error
}