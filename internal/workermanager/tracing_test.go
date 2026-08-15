package workermanager_test

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
	"quorum/internal/workermanager"
)

type tracingExecutor struct{}

func (tracingExecutor) Execute(ctx context.Context, j job.Job) error {
	return nil
}

func TestManagerRegisteredWorkerEmitsWorkerTracingSpans(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	wm := workermanager.NewManager()
	results := make(chan job.Result, 5)
	jobStore := store.NewJobStore()

	w := worker.NewWorker(5, wm.Available, results, jobStore, tracingExecutor{})
	wm.Register(w, "localhost:50055", "*")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Start(ctx)

	select {
	case client := <-wm.Available:
		j := job.NewJob(50, "video", 2)
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
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for available worker")
	}

	span := findWorkermanagerSpanByName(sr.Ended(), "worker.receive_job")
	if span == nil {
		t.Fatal("expected worker.receive_job span from managed worker")
	}
	if !hasWorkermanagerAttr(span, "worker.id", "5") {
		t.Fatal("expected worker.id attribute")
	}
	if !hasWorkermanagerAttr(span, "job.id", "50") {
		t.Fatal("expected job.id attribute")
	}
	if span.Status().Code != codes.Ok {
		t.Fatalf("expected Ok status, got %v", span.Status().Code)
	}
}

func findWorkermanagerSpanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, sp := range spans {
		if sp.Name() == name {
			return sp
		}
	}
	return nil
}

func hasWorkermanagerAttr(span sdktrace.ReadOnlySpan, key, expected string) bool {
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
