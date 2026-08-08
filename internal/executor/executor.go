package executor

import (
	"context"
	"quorum/internal/job"
)

type Executor interface {
	Execute(ctx context.Context, j job.Job) error
}