package worker

import (
	"context"
	"quorum/internal/job"
)

type WorkerClient interface {
	ID() int
	Start(ctx context.Context)
	Submit(ctx context.Context, j job.Job) error
}
