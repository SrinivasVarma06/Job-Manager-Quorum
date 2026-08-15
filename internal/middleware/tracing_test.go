package middleware_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"quorum/internal/middleware"
)

func TestTracingMiddlewareCreatesSpanWithMetadata(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	oldTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(oldTP)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	handler := middleware.RequestID(
		middleware.Tracing(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			}),
		),
	)

	req := httptest.NewRequest(http.MethodGet, "/jobs/123", nil)
	req.Header.Set("User-Agent", "otel-test-agent")
	req.Header.Set("X-Request-ID", "req-123")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.Name() != "http.request" {
		t.Fatalf("expected span name http.request, got %s", span.Name())
	}

	if !span.SpanContext().IsValid() {
		t.Fatal("expected valid trace id")
	}

	if !hasAttribute(spans[0], "http.status_code", attribute.IntValue(http.StatusAccepted)) {
		t.Fatal("expected http.status_code attribute on span")
	}

	if !hasAttribute(spans[0], "request.id", attribute.StringValue("req-123")) {
		t.Fatal("expected request.id attribute on span")
	}
}

func hasAttribute(span sdktrace.ReadOnlySpan, key string, want attribute.Value) bool {
	for _, kv := range span.Attributes() {
		if string(kv.Key) != key {
			continue
		}

		switch want.Type() {
		case attribute.BOOL:
			return kv.Value.AsBool() == want.AsBool()
		case attribute.BOOLSLICE:
			return kv.Value.AsBoolSlice() != nil || want.AsBoolSlice() != nil
		case attribute.INT64:
			return kv.Value.AsInt64() == want.AsInt64()
		case attribute.FLOAT64:
			return kv.Value.AsFloat64() == want.AsFloat64()
		case attribute.STRING:
			return kv.Value.AsString() == want.AsString()
		case attribute.STRINGSLICE:
			return kv.Value.AsStringSlice() != nil || want.AsStringSlice() != nil
		default:
			return false
		}
	}
	return false
}
