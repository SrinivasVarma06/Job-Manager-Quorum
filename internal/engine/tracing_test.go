package engine_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"quorum/internal/config"
	"quorum/internal/engine"
	"quorum/internal/oteltest"
)

func TestSubmitJobWithContextCreatesSpan(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	cfg := config.Default()
	cfg.RaftEnabled = false
	cfg.StorageType = "memory"
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer e.Stop()

	_, err = e.SubmitJobWithContext(context.Background(), "email", 7)
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}

	span := findSpanByName(sr.Ended(), "engine.submit_job")
	if span == nil {
		t.Fatal("expected engine.submit_job span")
	}
	if !span.SpanContext().TraceID().IsValid() {
		t.Fatal("expected valid trace id")
	}
	if !hasAttr(span, "job.type", "email") {
		t.Fatal("expected job.type attribute")
	}
	if !hasAttr(span, "job.priority", "7") {
		t.Fatal("expected job.priority attribute")
	}
	if span.Status().Code != codes.Ok {
		t.Fatalf("expected Ok status, got %v", span.Status().Code)
	}
}

func TestSubmitJobAtWithContextCreatesSpan(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	cfg := config.Default()
	cfg.RaftEnabled = false
	cfg.StorageType = "memory"
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer e.Stop()

	runAt := time.Now().Add(5 * time.Minute)
	_, err = e.SubmitJobAtWithContext(context.Background(), "scheduled", 3, runAt)
	if err != nil {
		t.Fatalf("submit scheduled job: %v", err)
	}

	span := findSpanByName(sr.Ended(), "engine.submit_job_at")
	if span == nil {
		t.Fatal("expected engine.submit_job_at span")
	}
	if !hasAttr(span, "job.type", "scheduled") {
		t.Fatal("expected job.type attribute")
	}
	if !hasAttr(span, "job.priority", "3") {
		t.Fatal("expected job.priority attribute")
	}
	if !hasAttr(span, "job.scheduled_at", runAt.Format(time.RFC3339)) {
		t.Fatal("expected job.scheduled_at attribute")
	}
}

func TestCancelJobWithContextRecordsError(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	cfg := config.Default()
	cfg.RaftEnabled = false
	cfg.StorageType = "memory"
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer e.Stop()

	err = e.CancelJobWithContext(context.Background(), 999)
	if err == nil {
		t.Fatal("expected cancel error for unknown job")
	}

	span := findSpanByName(sr.Ended(), "engine.cancel_job")
	if span == nil {
		t.Fatal("expected engine.cancel_job span")
	}
	if !hasAttr(span, "job.id", "999") {
		t.Fatal("expected job.id attribute")
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("expected Error status, got %v", span.Status().Code)
	}
	if len(span.Events()) == 0 {
		t.Fatal("expected recorded error event")
	}
}

func findSpanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, sp := range spans {
		if sp.Name() == name {
			return sp
		}
	}
	return nil
}

func hasAttr(span sdktrace.ReadOnlySpan, key, expected string) bool {
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
