package oteltest

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var mu sync.Mutex

// InstallSpanRecorder replaces the global TracerProvider with one backed by a
// SpanRecorder. A mutex serializes installation so parallel test packages do
// not clobber each other's tracer provider.
func InstallSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	mu.Lock()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(ctx)
		otel.SetTracerProvider(old)
		mu.Unlock()
	})

	return sr
}
