package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"quorum/internal/broker"
	"quorum/internal/dlq"
	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/scheduler"
	"quorum/internal/store"
	"quorum/internal/worker"
)

type mockClient struct {
	mu        sync.Mutex
	id        int
	submitted []job.Job
}

func (m *mockClient) ID() int {
	return m.id
}

func (m *mockClient) Start(ctx context.Context) {}

func (m *mockClient) Submit(ctx context.Context, j job.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.submitted = append(m.submitted, j)
	return nil
}

func (m *mockClient) SubmittedJobs() []job.Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]job.Job, len(m.submitted))
	copy(copied, m.submitted)
	return copied
}

var _ worker.WorkerClient = (*mockClient)(nil)

func TestSchedulerDispatchWithWorkerClient(t *testing.T) {
	jobStore := store.NewJobStore()
	pq := queue.NewJobQueue(jobStore, queue.PriorityComparator)
	dq := queue.NewJobQueue(jobStore, queue.DelayComparator)

	available := make(chan worker.WorkerClient, 5)
	deadWorkers := make(chan int, 5)
	dead := dlq.New()

	s := scheduler.NewScheduler(
		pq,
		dq,
		available,
		jobStore,
		dead,
		deadWorkers,
		10,
		nil,
	)

	mc := &mockClient{id: 101}
	available <- mc

	j := job.NewJob(1, "email", 5)
	_ = jobStore.Add(j)

	ctx := context.Background()

	if !s.Dispatch(ctx, j) {
		t.Fatal("expected dispatch to succeed")
	}

	if len(mc.SubmittedJobs()) != 1 {
		t.Fatalf("expected 1 submitted job, got %d", len(mc.SubmittedJobs()))
	}

	if mc.SubmittedJobs()[0].ID != 1 {
		t.Fatalf("expected submitted job ID 1, got %d", mc.SubmittedJobs()[0].ID)
	}

	// Verify lease acquired on Leader
	lease, ok := s.Leases.Get(1)
	if !ok || lease.WorkerID != 101 {
		t.Fatalf("expected active lease for job 1 to worker 101, got %+v", lease)
	}
}

func TestSchedulerWorkerFailoverAndReDispatch(t *testing.T) {
	jobStore := store.NewJobStore()
	pq := queue.NewJobQueue(jobStore, queue.PriorityComparator)
	dq := queue.NewJobQueue(jobStore, queue.DelayComparator)

	available := make(chan worker.WorkerClient, 5)
	deadWorkers := make(chan int, 5)
	dead := dlq.New()

	s := scheduler.NewScheduler(
		pq,
		dq,
		available,
		jobStore,
		dead,
		deadWorkers,
		10,
		nil,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Worker 102 is healthy and available to pick up failed jobs
	worker102 := &mockClient{id: 102}
	available <- worker102

	// Start scheduler background loops
	go s.Start(ctx)

	// Job 42 was previously assigned to Worker 101 with an ephemeral lease
	j := job.NewJob(42, "data_process", 10)
	_ = jobStore.Add(j)
	s.Leases.Acquire(42, 101, 1, 1)

	// Trigger worker failover signal for dead Worker 101
	deadWorkers <- 101

	// Wait for recoveryLoop to release lease, re-enqueue, and dispatchLoop to send to worker 102
	time.Sleep(200 * time.Millisecond)

	// Verify Worker 102 received the re-dispatched job 42
	submitted := worker102.SubmittedJobs()
	if len(submitted) != 1 {
		t.Fatalf("expected healthy worker 102 to receive 1 recovered job, got %d", len(submitted))
	}

	if submitted[0].ID != 42 {
		t.Fatalf("expected job 42 to be dispatched to worker 102, got job %d", submitted[0].ID)
	}

	// Verify new lease active for Worker 102
	newLease, ok := s.Leases.Get(42)
	if !ok || newLease.WorkerID != 102 {
		t.Fatalf("expected new lease for worker 102, got %+v", newLease)
	}
}

func TestSchedulerTopicAwareRouting(t *testing.T) {
	jobStore := store.NewJobStore()
	pq := queue.NewJobQueue(jobStore, queue.PriorityComparator)
	dq := queue.NewJobQueue(jobStore, queue.DelayComparator)

	available := make(chan worker.WorkerClient, 10)
	deadWorkers := make(chan int, 5)
	dead := dlq.New()

	br := broker.New()
	br.RegisterWorker(101, []string{"email"})
	br.RegisterWorker(102, []string{"video_processing"})

	s := scheduler.NewScheduler(
		pq,
		dq,
		available,
		jobStore,
		dead,
		deadWorkers,
		10,
		br,
	)

	w101 := &mockClient{id: 101}
	w102 := &mockClient{id: 102}

	available <- w101
	available <- w102

	jVideo := job.NewJob(1, "video_processing", 10)
	_ = jobStore.Add(jVideo)

	ctx := context.Background()

	// Dispatch video_processing job -> Should route exclusively to worker 102
	if !s.Dispatch(ctx, jVideo) {
		t.Fatal("expected dispatch for video_processing to succeed")
	}

	if len(w102.SubmittedJobs()) != 1 || w102.SubmittedJobs()[0].ID != 1 {
		t.Fatalf("expected worker 102 to receive video_processing job 1, got %v", w102.SubmittedJobs())
	}

	if len(w101.SubmittedJobs()) != 0 {
		t.Fatalf("expected worker 101 to receive 0 jobs, got %d", len(w101.SubmittedJobs()))
	}
}