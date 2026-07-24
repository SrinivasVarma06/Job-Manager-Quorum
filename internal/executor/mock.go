package executor

import (
	"errors"
	"time"

	"quorum/internal/job"
)

type MockExecutor struct{}

func (m *MockExecutor) Execute(j job.Job) error {
	time.Sleep(time.Second)
	if j.ID%2 == 0 {
		return errors.New("simulated failure")
	}
	return nil
}