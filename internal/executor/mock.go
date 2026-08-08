package executor

import (
	"context"
	"errors"
	"time"

	"quorum/internal/job"
)

type MockExecutor struct{}

func (m *MockExecutor) Execute(ctx context.Context, j job.Job) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
	}

	if j.ID%2 == 0 {
		return errors.New("simulated failure")
	}
	return nil
}