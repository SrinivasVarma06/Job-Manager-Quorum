package executor

import (
	"context"
	"quorum/internal/job"
)

type RateLimitedExecutor struct {
	next    Executor
	limiter Limiter
}

func NewRateLimitedExecutor(next Executor, limiter Limiter) *RateLimitedExecutor {
	return &RateLimitedExecutor{
		next:    next,
		limiter: limiter,
	}
}

func (r *RateLimitedExecutor) Execute(ctx context.Context, j job.Job) error {
	r.limiter.Acquire()
	return r.next.Execute(ctx, j)
}