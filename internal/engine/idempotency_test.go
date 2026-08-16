package engine_test

import (
	"context"
	"testing"

	"quorum/internal/config"
	"quorum/internal/engine"
)

// ---------------------------------------------------------------------------
// Engine idempotency tests
// ---------------------------------------------------------------------------

// TestEngineIdempotent_SameKey_OneJobCreated verifies that submitting the same
// idempotency key twice creates only one job and returns the same job both times.
func TestEngineIdempotent_SameKey_OneJobCreated(t *testing.T) {
	cfg := config.Default()
	cfg.StorageType = "memory"
	cfg.RaftEnabled = false

	e, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	defer e.Stop()

	const key = "idempotency-test-key-1"

	j1, dup1, err := e.SubmitJobIdempotent(context.Background(), key, "email", 5)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if dup1 {
		t.Fatal("first submit must not be a duplicate")
	}

	j2, dup2, err := e.SubmitJobIdempotent(context.Background(), key, "email", 5)
	if err != nil {
		t.Fatalf("second submit: %v", err)
	}
	if !dup2 {
		t.Fatal("second submit with same key must be a duplicate")
	}

	if j1.ID != j2.ID {
		t.Fatalf("expected same job ID, got %d and %d", j1.ID, j2.ID)
	}

	// Exactly one job must exist in the store.
	allJobs := e.Jobs()
	if len(allJobs) != 1 {
		t.Fatalf("expected 1 job in store, got %d", len(allJobs))
	}
}

// TestEngineIdempotent_DifferentKeys_TwoJobsCreated verifies that different
// idempotency keys each create a distinct job.
func TestEngineIdempotent_DifferentKeys_TwoJobsCreated(t *testing.T) {
	cfg := config.Default()
	cfg.StorageType = "memory"
	cfg.RaftEnabled = false

	e, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	defer e.Stop()

	j1, _, err := e.SubmitJobIdempotent(context.Background(), "key-A", "email", 5)
	if err != nil {
		t.Fatalf("submit key-A: %v", err)
	}

	j2, _, err := e.SubmitJobIdempotent(context.Background(), "key-B", "sms", 3)
	if err != nil {
		t.Fatalf("submit key-B: %v", err)
	}

	if j1.ID == j2.ID {
		t.Fatal("different keys must produce different job IDs")
	}

	allJobs := e.Jobs()
	if len(allJobs) != 2 {
		t.Fatalf("expected 2 jobs in store, got %d", len(allJobs))
	}
}

// TestEngineIdempotent_EmptyKey_NoDedupEachCallCreatesNewJob verifies that
// empty idempotency key disables deduplication — every call creates a new job.
func TestEngineIdempotent_EmptyKey_NoDedupEachCallCreatesNewJob(t *testing.T) {
	cfg := config.Default()
	cfg.StorageType = "memory"
	cfg.RaftEnabled = false

	e, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	defer e.Stop()

	j1, _, err := e.SubmitJobIdempotent(context.Background(), "", "email", 5)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	j2, _, err := e.SubmitJobIdempotent(context.Background(), "", "email", 5)
	if err != nil {
		t.Fatalf("second submit: %v", err)
	}

	if j1.ID == j2.ID {
		t.Fatal("empty key submissions must create separate jobs")
	}

	if len(e.Jobs()) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(e.Jobs()))
	}
}

// TestEngineIdempotent_BoltStore_SameKey_OneJobCreated exercises the same
// dedup logic against the persistent BoltStore backend.
func TestEngineIdempotent_BoltStore_SameKey_OneJobCreated(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StorageType = "bolt"
	cfg.StoragePath = dir + "/idem_engine.db"
	cfg.RaftEnabled = false

	e, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	defer e.Stop()

	const key = "bolt-engine-key"

	j1, dup1, _ := e.SubmitJobIdempotent(context.Background(), key, "report", 1)
	j2, dup2, _ := e.SubmitJobIdempotent(context.Background(), key, "report", 1)

	if dup1 {
		t.Fatal("first submit must not be duplicate")
	}
	if !dup2 {
		t.Fatal("second submit must be duplicate")
	}
	if j1.ID != j2.ID {
		t.Fatalf("expected same job, got IDs %d and %d", j1.ID, j2.ID)
	}
	if len(e.Jobs()) != 1 {
		t.Fatalf("expected 1 job in store, got %d", len(e.Jobs()))
	}
}
