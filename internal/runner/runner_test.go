package runner_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"quorum/internal/job"
	"quorum/internal/runner"
	"quorum/internal/store"
)

type mockExecutor struct {
	fail bool
	wait time.Duration
}

func (m *mockExecutor) Execute(ctx context.Context, j job.Job) error {
	if m.wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.wait):
		}
	}

	if m.fail {
		return errors.New("execution failed")
	}

	return nil
}

func TestRunnerExecuteSuccess(t *testing.T) {
	jobStore := store.NewJobStore()
	results := make(chan job.Result, 1)

	j := job.NewJob(1, "email", 1)
	_ = jobStore.Add(j)

	r := runner.New(
		jobStore,
		results,
		&mockExecutor{},
	)

	r.Execute(1, j)

	res := <-results
	if !res.Success {
		t.Fatal("expected success result")
	}

	stored, ok := jobStore.Get(1)
	if !ok {
		t.Fatal("job missing from store")
	}

	if stored.Status != job.Completed {
		t.Fatalf("expected Completed, got %v", stored.Status)
	}
}

func TestRunnerExecuteFailure(t *testing.T) {
	jobStore := store.NewJobStore()
	results := make(chan job.Result, 1)

	j := job.NewJob(1, "email", 1)
	_ = jobStore.Add(j)

	r := runner.New(
		jobStore,
		results,
		&mockExecutor{fail: true},
	)

	r.Execute(1, j)

	res := <-results

	if res.Success {
		t.Fatal("expected failure result")
	}

	stored, ok := jobStore.Get(1)
	if !ok {
		t.Fatal("job missing from store")
	}

	if stored.Status != job.Failed {
		t.Fatalf("expected Failed, got %v", stored.Status)
	}
}

func TestRunnerCancelActiveJob(t *testing.T) {
	jobStore := store.NewJobStore()
	results := make(chan job.Result, 1)

	j := job.NewJob(1, "email", 1)
	_ = jobStore.Add(j)

	r := runner.New(
		jobStore,
		results,
		&mockExecutor{wait: 5 * time.Second},
	)

	go r.Execute(1, j)

	time.Sleep(100 * time.Millisecond)

	if !r.Cancel(1) {
		t.Fatal("expected cancel to succeed")
	}

	res := <-results

	if res.Success {
		t.Fatal("expected cancelled execution to fail")
	}
}

func TestRunnerCancelUnknownJob(t *testing.T) {
	jobStore := store.NewJobStore()
	results := make(chan job.Result, 1)

	r := runner.New(
		jobStore,
		results,
		&mockExecutor{},
	)

	if r.Cancel(999) {
		t.Fatal("expected cancel to return false")
	}
}

func TestRunnerDropsResultWhenChannelFull(t *testing.T) {
	jobStore := store.NewJobStore()

	results := make(chan job.Result, 1)

	results <- job.Result{
		JobID:   999,
		Success: true,
	}

	j := job.NewJob(1, "email", 1)

	r := runner.New(
		jobStore,
		results,
		&mockExecutor{},
	)

	r.Execute(1, j)

	if len(results) != 1 {
		t.Fatal("expected channel to remain full")
	}
}
