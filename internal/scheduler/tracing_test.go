package scheduler_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"quorum/internal/dlq"
	"quorum/internal/job"
	"quorum/internal/oteltest"
	"quorum/internal/queue"
	"quorum/internal/scheduler"
	"quorum/internal/store"
	"quorum/internal/worker"
)

func TestDispatchCreatesSchedulerDispatchSpan(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	jobStore := store.NewJobStore()
	pq := queue.NewJobQueue(jobStore, queue.PriorityComparator)
	dq := queue.NewJobQueue(jobStore, queue.DelayComparator)
	available := make(chan worker.WorkerClient, 1)
	deadWorkers := make(chan int, 1)

	s := scheduler.NewScheduler(
		pq, dq, available, jobStore, dlq.New(), deadWorkers, 10, nil,
	)

	mc := &mockClient{id: 101}
	available <- mc

	j := job.NewJob(1, "email", 5)
	_ = jobStore.Add(j)

	if !s.Dispatch(context.Background(), j) {
		t.Fatal("expected dispatch to succeed")
	}

	span := findSchedulerSpanByName(sr.Ended(), "scheduler.dispatch")
	if span == nil {
		t.Fatal("expected scheduler.dispatch span")
	}
	if !hasSchedulerAttr(span, "job.id", "1") {
		t.Fatal("expected job.id attribute")
	}
	if !hasSchedulerAttr(span, "job.type", "email") {
		t.Fatal("expected job.type attribute")
	}
	if !hasSchedulerAttr(span, "job.priority", "5") {
		t.Fatal("expected job.priority attribute")
	}
	if span.Status().Code != codes.Ok {
		t.Fatalf("expected Ok status, got %v", span.Status().Code)
	}
}

func TestEnqueueCreatesSchedulerEnqueueSpan(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	jobStore := store.NewJobStore()
	pq := queue.NewJobQueue(jobStore, queue.PriorityComparator)
	dq := queue.NewJobQueue(jobStore, queue.DelayComparator)

	s := scheduler.NewScheduler(
		pq, dq, make(chan worker.WorkerClient, 1), jobStore, dlq.New(), make(chan int, 1), 10, nil,
	)

	j := job.NewJob(7, "report", 3)
	_ = jobStore.Add(j)

	if err := s.RebuildQueuesFromStore(); err != nil {
		t.Fatalf("rebuild queues: %v", err)
	}

	span := findSchedulerSpanByName(sr.Ended(), "scheduler.enqueue")
	if span == nil {
		t.Fatal("expected scheduler.enqueue span")
	}
	if !hasSchedulerAttr(span, "job.id", "7") {
		t.Fatal("expected job.id attribute on enqueue span")
	}
	if span.Status().Code != codes.Ok {
		t.Fatalf("expected Ok status, got %v", span.Status().Code)
	}
}

func TestPromoteDelayedJobCreatesSpan(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	jobStore := store.NewJobStore()
	pq := queue.NewJobQueue(jobStore, queue.PriorityComparator)
	dq := queue.NewJobQueue(jobStore, queue.DelayComparator)

	s := scheduler.NewScheduler(
		pq, dq, make(chan worker.WorkerClient, 1), jobStore, dlq.New(), make(chan int, 1), 10, nil,
	)

	j := job.NewJob(1, "email", 5)
	j.Status = job.Scheduled
	j.NextRunAt = time.Now().Add(-2 * time.Second)
	_ = jobStore.Add(j)
	dq.Enqueue(j.ID)

	s.ProcessDelayedJobs()

	span := findSchedulerSpanByName(sr.Ended(), "scheduler.promote_delayed_job")
	if span == nil {
		t.Fatal("expected scheduler.promote_delayed_job span")
	}
	if !hasSchedulerAttr(span, "job.id", "1") {
		t.Fatal("expected job.id attribute")
	}
	if !hasSchedulerAttr(span, "job.type", "email") {
		t.Fatal("expected job.type attribute")
	}
	if !hasSchedulerWaitTimeAtLeast(span, 1900) {
		t.Fatal("expected scheduler.wait_time_ms >= 1900")
	}
	if span.Status().Code != codes.Ok {
		t.Fatalf("expected Ok status, got %v", span.Status().Code)
	}
}

func TestDispatchRecordsErrorOnSubmitFailure(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	jobStore := store.NewJobStore()
	pq := queue.NewJobQueue(jobStore, queue.PriorityComparator)
	dq := queue.NewJobQueue(jobStore, queue.DelayComparator)
	available := make(chan worker.WorkerClient, 1)
	deadWorkers := make(chan int, 1)

	s := scheduler.NewScheduler(
		pq, dq, available, jobStore, dlq.New(), deadWorkers, 10, nil,
	)

	failClient := &failingMockClient{id: 201}
	available <- failClient

	j := job.NewJob(2, "email", 1)
	_ = jobStore.Add(j)

	if s.Dispatch(context.Background(), j) {
		t.Fatal("expected dispatch to fail")
	}

	span := findSchedulerSpanByName(sr.Ended(), "scheduler.dispatch")
	if span == nil {
		t.Fatal("expected scheduler.dispatch span")
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("expected Error status, got %v", span.Status().Code)
	}
	if len(span.Events()) == 0 {
		t.Fatal("expected recorded error event")
	}
}

type failingMockClient struct {
	id int
}

func (m *failingMockClient) ID() int { return m.id }

func (m *failingMockClient) Start(ctx context.Context) {}

func (m *failingMockClient) Submit(ctx context.Context, j job.Job) error {
	return context.Canceled
}

var _ worker.WorkerClient = (*failingMockClient)(nil)

func findSchedulerSpanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, sp := range spans {
		if sp.Name() == name {
			return sp
		}
	}
	return nil
}

func hasSchedulerAttr(span sdktrace.ReadOnlySpan, key, expected string) bool {
	for _, kv := range span.Attributes() {
		if string(kv.Key) != key {
			continue
		}
		switch kv.Value.Type() {
		case attribute.STRING:
			return kv.Value.AsString() == expected
		case attribute.INT64:
			return strconv.FormatInt(kv.Value.AsInt64(), 10) == expected
		default:
			return false
		}
	}
	return false
}

func hasSchedulerWaitTimeAtLeast(span sdktrace.ReadOnlySpan, minMs int64) bool {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == "scheduler.wait_time_ms" {
			return kv.Value.AsInt64() >= minMs
		}
	}
	return false
}
