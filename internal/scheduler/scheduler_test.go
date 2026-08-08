package scheduler_test

import (
	"context"
	"os"
	"sync"
	"time"

	"quorum/internal/broker"
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
		nil,
	)

	mc := &mockClient{id: 101}
	available <- mc

	j := job.NewJob(1, "email", 5)
	jobStore.Add(j)

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
}

func TestSchedulerWorkerFailoverAndReDispatch(t *testing.T) {
	jobStore := store.NewJobStore()
	pq := queue.NewJobQueue(jobStore, queue.PriorityComparator)
	dq := queue.NewJobQueue(jobStore, queue.DelayComparator)

	available := make(chan worker.WorkerClient, 5)
	deadWorkers := make(chan int, 5)
	dead := dlq.New()

	walFile := "test_failover.log"
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
		nil,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Worker 102 is healthy and available to pick up failed jobs
	worker102 := &mockClient{id: 102}
	available <- worker102

	// Start scheduler background loops
	go s.Start(ctx)

	// Job 42 was previously assigned to Worker 101 which is now dead
	j := job.NewJob(42, "data_process", 10)
	j.Status = job.Running
	j.WorkerID = 101
	jobStore.Add(j)

	// Trigger worker failover signal for dead Worker 101
	deadWorkers <- 101

	// Wait for recoveryLoop to re-enqueue and dispatchLoop to send to worker 102
	time.Sleep(200 * time.Millisecond)

	// Verify Worker 102 received the re-dispatched job 42
	submitted := worker102.SubmittedJobs()
	if len(submitted) != 1 {
		t.Fatalf("expected healthy worker 102 to receive 1 recovered job, got %d", len(submitted))
	}

	if submitted[0].ID != 42 {
		t.Fatalf("expected job 42 to be dispatched to worker 102, got job %d", submitted[0].ID)
	}

	// Verify job state in store reflects running on worker 102
	updatedJob, ok := jobStore.Get(42)
	if !ok {
		t.Fatal("job 42 not found in store")
	}

	if updatedJob.WorkerID != 102 {
		t.Fatalf("expected job 42 worker ID to be updated to 102, got %d", updatedJob.WorkerID)
	}
}

func TestSchedulerTopicAwareRouting(t *testing.T) {
	jobStore := store.NewJobStore()
	pq := queue.NewJobQueue(jobStore, queue.PriorityComparator)
	dq := queue.NewJobQueue(jobStore, queue.DelayComparator)

	available := make(chan worker.WorkerClient, 10)
	deadWorkers := make(chan int, 5)
	dead := dlq.New()

	walFile := "test_topic.log"
	defer os.Remove(walFile)

	wal, err := storage.NewWal(walFile)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}
	defer wal.Close()

	br := broker.New()
	br.RegisterWorker(101, []string{"email"})
	br.RegisterWorker(102, []string{"video_processing"})

	s := scheduler.NewScheduler(
		pq,
		dq,
		available,
		wal,
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
	jobStore.Add(jVideo)

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