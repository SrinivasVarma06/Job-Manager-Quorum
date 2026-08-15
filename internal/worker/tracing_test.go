package worker_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"quorum/internal/job"
	"quorum/internal/oteltest"
	"quorum/internal/store"
	"quorum/internal/worker"
)

func TestWorkerJobLifecycleCreatesTracingSpans(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	available := make(chan worker.WorkerClient, 5)
	results := make(chan job.Result, 5)
	jobStore := store.NewJobStore()
	exec := &mockExecutor{}

	w := worker.NewWorker(7, available, results, jobStore, exec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx)

	client := <-available
	j := job.NewJob(100, "email", 1)
	_ = jobStore.Add(j)

	if err := client.Submit(ctx, j); err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	select {
	case res := <-results:
		if !res.Success {
			t.Fatal("expected success result")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}

	for _, name := range []string{"worker.receive_job", "worker.execute_job", "worker.complete_job"} {
		span := findWorkerSpanByName(sr.Ended(), name)
		if span == nil {
			t.Fatalf("expected %s span", name)
		}
		if !hasWorkerAttr(span, "worker.id", "7") {
			t.Fatalf("expected worker.id on %s", name)
		}
		if !hasWorkerAttr(span, "job.id", "100") {
			t.Fatalf("expected job.id on %s", name)
		}
		if !hasWorkerAttr(span, "job.type", "email") {
			t.Fatalf("expected job.type on %s", name)
		}
	}

	complete := findWorkerSpanByName(sr.Ended(), "worker.complete_job")
	if complete.Status().Code != codes.Ok {
		t.Fatalf("expected Ok on complete span, got %v", complete.Status().Code)
	}

	execute := findWorkerSpanByName(sr.Ended(), "worker.execute_job")
	if !hasWorkerDuration(execute) {
		t.Fatal("expected execution.duration_ms on execute span")
	}
}

func TestWorkerFailureRecordsErrorSpans(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	available := make(chan worker.WorkerClient, 5)
	results := make(chan job.Result, 5)
	jobStore := store.NewJobStore()
	exec := &mockExecutor{fail: true}

	w := worker.NewWorker(2, available, results, jobStore, exec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx)

	client := <-available
	j := job.NewJob(101, "email", 1)
	_ = jobStore.Add(j)

	if err := client.Submit(ctx, j); err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	<-results

	execute := findWorkerSpanByName(sr.Ended(), "worker.execute_job")
	if execute == nil {
		t.Fatal("expected worker.execute_job span")
	}
	if execute.Status().Code != codes.Error {
		t.Fatalf("expected Error on execute span, got %v", execute.Status().Code)
	}

	complete := findWorkerSpanByName(sr.Ended(), "worker.complete_job")
	if complete == nil {
		t.Fatal("expected worker.complete_job span")
	}
	if complete.Status().Code != codes.Error {
		t.Fatalf("expected Error on complete span, got %v", complete.Status().Code)
	}
}

func findWorkerSpanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, sp := range spans {
		if sp.Name() == name {
			return sp
		}
	}
	return nil
}

func hasWorkerAttr(span sdktrace.ReadOnlySpan, key, expected string) bool {
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

func hasWorkerDuration(span sdktrace.ReadOnlySpan) bool {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == "execution.duration_ms" {
			return kv.Value.AsInt64() >= 0
		}
	}
	return false
}
