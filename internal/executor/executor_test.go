package executor_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"quorum/internal/executor"
	"quorum/internal/job"
)

type mockExecutor struct {
	fail  bool
	calls atomic.Int32
}

func (m *mockExecutor) Execute(ctx context.Context, j job.Job) error {
	m.calls.Add(1)

	if m.fail {
		return errors.New("execution failed")
	}
	return nil
}

func TestCircuitBreakerOpens(t *testing.T) {
	base := &mockExecutor{fail: true}

	cb := executor.NewCircuitBreakerExecutor(
		base,
		3,
		time.Second,
	)

	j := job.NewJob(1, "email", 1)

	for i := 0; i < 3; i++ {
		_ = cb.Execute(context.Background(), j)
	}

	err := cb.Execute(context.Background(), j)

	if !errors.Is(err, executor.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestCircuitBreakerRecoversAfterTimeout(t *testing.T) {
	base := &mockExecutor{fail: true}

	cb := executor.NewCircuitBreakerExecutor(
		base,
		1,
		100*time.Millisecond,
	)

	j := job.NewJob(1, "email", 1)

	_ = cb.Execute(context.Background(), j)

	time.Sleep(150 * time.Millisecond)

	base.fail = false

	err := cb.Execute(context.Background(), j)

	if err != nil {
		t.Fatalf("expected successful recovery, got %v", err)
	}
}

type mockLimiter struct {
	called bool
}

func (m *mockLimiter) Acquire() {
	m.called = true
}

func TestRateLimitedExecutor(t *testing.T) {
	base := &mockExecutor{}
	limiter := &mockLimiter{}

	r := executor.NewRateLimitedExecutor(
		base,
		limiter,
	)

	j := job.NewJob(1, "email", 1)

	if err := r.Execute(context.Background(), j); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !limiter.called {
		t.Fatal("expected limiter Acquire to be called")
	}

	if base.calls.Load() != 1 {
		t.Fatal("expected underlying executor to execute")
	}
}

func TestTokenBucketBurst(t *testing.T) {
	limiter := executor.NewTokenBucketLimiter(
		10,
		3,
	)

	start := time.Now()

	limiter.Acquire()
	limiter.Acquire()
	limiter.Acquire()

	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Fatalf("burst tokens should be immediate")
	}
}

func TestTokenBucketRefill(t *testing.T) {
	limiter := executor.NewTokenBucketLimiter(
		2,
		1,
	)

	limiter.Acquire()

	start := time.Now()

	limiter.Acquire()

	if time.Since(start) < 400*time.Millisecond {
		t.Fatal("expected refill delay")
	}
}
