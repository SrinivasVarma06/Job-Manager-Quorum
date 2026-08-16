package runner_test

import (
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"quorum/internal/job"
	"quorum/internal/oteltest"
	"quorum/internal/runner"
	"quorum/internal/store"
)

func TestRunnerExecuteSuccessCreatesSpan(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	jobStore := store.NewJobStore()
	results := make(chan job.Result, 1)
	j := job.NewJob(1, "email", 1)
	_ = jobStore.Add(j)

	r := runner.New(jobStore, results, &mockExecutor{})
	r.Execute(1, j)

	<-results

	span := findRunnerSpanByName(sr, "runner.execute")
	if span == nil {
		t.Fatal("expected runner.execute span")
	}
	if !hasRunnerAttr(span, "job.id", "1") {
		t.Fatal("expected job.id attribute")
	}
	if !hasRunnerAttr(span, "job.type", "email") {
		t.Fatal("expected job.type attribute")
	}
	if !hasRunnerDuration(span) {
		t.Fatal("expected execution.duration_ms attribute")
	}
	if span.Status().Code != codes.Ok {
		t.Fatalf("expected Ok status, got %v", span.Status().Code)
	}
}

func TestRunnerExecuteFailureRecordsErrorSpan(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	jobStore := store.NewJobStore()
	results := make(chan job.Result, 1)
	j := job.NewJob(1, "email", 1)
	_ = jobStore.Add(j)

	r := runner.New(jobStore, results, &mockExecutor{fail: true})
	r.Execute(1, j)

	<-results

	span := findRunnerSpanByName(sr, "runner.execute")
	if span == nil {
		t.Fatal("expected runner.execute span")
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("expected Error status, got %v", span.Status().Code)
	}
	if len(span.Events()) == 0 {
		t.Fatal("expected recorded error event")
	}
}

func TestRunnerExecuteCancellationRecordsErrorSpan(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	jobStore := store.NewJobStore()
	results := make(chan job.Result, 1)
	j := job.NewJob(1, "email", 1)
	_ = jobStore.Add(j)

	r := runner.New(jobStore, results, &mockExecutor{wait: 5 * time.Second})

	go r.Execute(1, j)
	time.Sleep(100 * time.Millisecond)
	if !r.Cancel(1) {
		t.Fatal("expected cancel to succeed")
	}

	<-results

	span := findRunnerSpanByName(sr, "runner.execute")
	if span == nil {
		t.Fatal("expected runner.execute span")
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("expected Error status, got %v", span.Status().Code)
	}
}

func findRunnerSpanByName(sr *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	deadline := time.Now().Add(2 * time.Second)
	for {
		for _, sp := range sr.Ended() {
			if sp.Name() == name {
				return sp
			}
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func hasRunnerAttr(span sdktrace.ReadOnlySpan, key, expected string) bool {
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

func hasRunnerDuration(span sdktrace.ReadOnlySpan) bool {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == "execution.duration_ms" {
			return kv.Value.AsInt64() >= 0
		}
	}
	return false
}
