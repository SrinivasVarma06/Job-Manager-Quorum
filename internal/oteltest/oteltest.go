package oteltest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// lockPath is a cross-process advisory lock file. Each test binary that calls
// InstallSpanRecorder acquires exclusive ownership by atomically creating this
// file (O_EXCL). The file is removed when the lock is released, allowing the
// next binary to proceed.
//
// This serialises the global OTel tracer provider swap across all parallel
// test binaries produced by `go test ./...`.
var lockPath = filepath.Join(os.TempDir(), "quorum_oteltest_global.lock")

// InstallSpanRecorder replaces the global TracerProvider with one backed by a
// SpanRecorder and restores the previous provider in t.Cleanup.
//
// A cross-process file lock prevents concurrent test binaries (runner, worker,
// etc.) from clobbering each other's global provider when they run in parallel
// under `go test ./...`.
func InstallSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	acquireFileLock(t)

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(ctx)
		otel.SetTracerProvider(old)
		releaseFileLock()
	})

	return sr
}

// acquireFileLock spins until it can atomically create lockPath with O_EXCL,
// which guarantees at most one holder across all OS processes.
func acquireFileLock(t *testing.T) {
	t.Helper()
	const maxWait = 30 * time.Second
	deadline := time.Now().Add(maxWait)

	for {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			_ = f.Close()
			return
		}
		// Lock file exists — someone else holds it. Check for stale lock
		// (process crashed without cleanup).
		if info, statErr := os.Stat(lockPath); statErr == nil {
			if time.Since(info.ModTime()) > 60*time.Second {
				// Stale lock — remove and retry immediately.
				_ = os.Remove(lockPath)
				continue
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("oteltest: timed out waiting for global OTel lock after %v", maxWait)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func releaseFileLock() {
	_ = os.Remove(lockPath)
}
