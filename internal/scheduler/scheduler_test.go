package scheduler_test

import (
	"context"
	"os"
	"quorum/internal/dlq"
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/scheduler"
	"quorum/internal/storage"
	"quorum/internal/store"
	"quorum/internal/worker"
	"testing"
)

type mockClient struct {
	id        int
	submitted []job.Job
}

func (m *mockClient) ID() int {
	return m.id
}

func (m *mockClient) Start(ctx context.Context) {}

func (m *mockClient) Submit(ctx context.Context, j job.Job) error {
	m.submitted = append(m.submitted, j)
	return nil
}

var _ worker.WorkerClient = (*mockClient)(nil)

func TestSchedulerDispatchWithWorkerClient(t *testing.T) {
	jobStore := store.NewJobStore()
	pq := queue.NewJobQueue(jobStore, queue.PriorityComparator)
	dq := queue.NewJobQueue(jobStore, queue.DelayComparator)

	available := make(chan worker.WorkerClient, 5)
	deadWorkers := make(chan int, 5)
	dead := dlq.New()

	walFile := "test_scheduler.log"
	defer os.Remove(walFile)

	wal, err := storage.NewWal(walFile)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}
	defer wal.Close()

	s := scheduler.NewScheduler(
		pq,
		dq,
		available,
		wal,
		jobStore,
		dead,
		deadWorkers,
		10,
	)

	mc := &mockClient{id: 10}
	available <- mc

	j := job.NewJob(1, "email", 5)
	jobStore.Add(j)

	ctx := context.Background()

	if !s.Dispatch(ctx, j) {
		t.Fatal("expected dispatch to succeed")
	}

	if len(mc.submitted) != 1 {
		t.Fatalf("expected 1 submitted job, got %d", len(mc.submitted))
	}

	if mc.submitted[0].ID != 1 {
		t.Fatalf("expected submitted job ID 1, got %d", mc.submitted[0].ID)
	}
}