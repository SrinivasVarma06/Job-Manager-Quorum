package worker_test

import (
	"context"
	"errors"
	"quorum/internal/job"
	"quorum/internal/store"
	"quorum/internal/worker"
	"testing"
	"time"
)

type mockExecutor struct {
	fail bool
}

func (m *mockExecutor) Execute(ctx context.Context, j job.Job) error {
	if m.fail {
		return errors.New("mock execution failure")
	}
	return nil
}

func TestWorkerClientInterface(t *testing.T) {
	available := make(chan worker.WorkerClient, 5)
	results := make(chan job.Result, 5)
	jobStore := store.NewJobStore()
	exec := &mockExecutor{}

	w := worker.NewWorker(1, available, results, jobStore, exec)

	var _ worker.WorkerClient = w

	if w.ID() != 1 {
		t.Fatalf("expected worker ID 1, got %d", w.ID())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx)

	select {
	case client := <-available:
		if client.ID() != 1 {
			t.Fatalf("expected available client ID 1, got %d", client.ID())
		}

		j := job.NewJob(100, "email", 1)
		_ = jobStore.Add(j)

		if err := client.Submit(ctx, j); err != nil {
			t.Fatalf("submit failed: %v", err)
		}

		select {
		case res := <-results:
			if res.JobID != 100 {
				t.Fatalf("expected JobID 100, got %d", res.JobID)
			}
			if !res.Success {
				t.Fatalf("expected execution success")
			}

		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for result")
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker availability")
	}
}

func TestWorkerClientFailureHandling(t *testing.T) {
	available := make(chan worker.WorkerClient, 5)
	results := make(chan job.Result, 5)
	jobStore := store.NewJobStore()
	exec := &mockExecutor{fail: true}

	w := worker.NewWorker(2, available, results, jobStore, exec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx)

	select {
	case client := <-available:
		j := job.NewJob(101, "email", 1)
		_ = jobStore.Add(j)

		if err := client.Submit(ctx, j); err != nil {
			t.Fatalf("submit failed: %v", err)
		}

		select {
		case res := <-results:
			if res.JobID != 101 {
				t.Fatalf("expected JobID 101, got %d", res.JobID)
			}
			if res.Success {
				t.Fatal("expected execution failure")
			}
			if res.Error == nil {
				t.Fatal("expected non-nil error")
			}

		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for result")
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker availability")
	}
}

func TestWorkerBusy(t *testing.T) {
	available := make(chan worker.WorkerClient, 1)
	results := make(chan job.Result, 10)

	jobStore := store.NewJobStore()
	exec := &mockExecutor{}

	w := worker.NewWorker(1, available, results, jobStore, exec)

	ctx := context.Background()

	j := job.NewJob(1, "email", 1)

	if err := w.Submit(ctx, j); err == nil {
		t.Fatal("expected worker to be busy because Start() not running")
	}
}

func TestSubmitCancelledContext(t *testing.T) {
	available := make(chan worker.WorkerClient, 1)
	results := make(chan job.Result, 1)

	jobStore := store.NewJobStore()
	exec := &mockExecutor{}

	w := worker.NewWorker(1, available, results, jobStore, exec)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	j := job.NewJob(1, "email", 1)

	err := w.Submit(ctx, j)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestWorkerBecomesAvailableAgain(t *testing.T) {
	available := make(chan worker.WorkerClient, 5)
	results := make(chan job.Result, 5)

	jobStore := store.NewJobStore()
	exec := &mockExecutor{}

	w := worker.NewWorker(1, available, results, jobStore, exec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx)

	client := <-available

	j := job.NewJob(1, "email", 1)
	_ = jobStore.Add(j)

	if err := client.Submit(ctx, j); err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	<-results

	select {
	case client2 := <-available:
		if client2.ID() != 1 {
			t.Fatalf("expected worker 1 available again")
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not become available again")
	}
}

func TestMultipleSequentialJobs(t *testing.T) {
	available := make(chan worker.WorkerClient, 5)
	results := make(chan job.Result, 10)

	jobStore := store.NewJobStore()
	exec := &mockExecutor{}

	w := worker.NewWorker(1, available, results, jobStore, exec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx)

	for i := 1; i <= 3; i++ {
		client := <-available

		j := job.NewJob(i, "email", 1)
		_ = jobStore.Add(j)

		if err := client.Submit(ctx, j); err != nil {
			t.Fatalf("submit failed: %v", err)
		}

		res := <-results

		if res.JobID != i {
			t.Fatalf("expected job %d result, got %d", i, res.JobID)
		}
	}
}

func TestWorkerStopsOnContextCancel(t *testing.T) {
	available := make(chan worker.WorkerClient, 5)
	results := make(chan job.Result, 5)

	jobStore := store.NewJobStore()
	exec := &mockExecutor{}

	w := worker.NewWorker(1, available, results, jobStore, exec)

	ctx, cancel := context.WithCancel(context.Background())

	go w.Start(ctx)

	<-available

	cancel()

	time.Sleep(100 * time.Millisecond)

	select {
	case <-available:
		t.Fatal("worker should have stopped advertising availability")
	default:
	}
}
