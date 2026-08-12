package engine_test

import (
	"fmt"
	"path/filepath"
	"quorum/internal/config"
	"quorum/internal/engine"
	"quorum/internal/job"
	"quorum/internal/store"
	"testing"
	"time"
)

func TestEngineRestoreWithBoltStore(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_engine.db")

	cfg := config.Default()
	cfg.StorageType = "bolt"
	cfg.StoragePath = dbPath
	cfg.RaftEnabled = false

	// Phase 1: Boot engine, submit jobs using explicit test dbPath config
	e1, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("failed to create engine 1: %v", err)
	}

	j1, err := e1.SubmitJob("export_csv", 10)
	if err != nil {
		t.Fatalf("submit job 1 failed: %v", err)
	}

	j2, err := e1.SubmitJob("render_video", 20)
	if err != nil {
		t.Fatalf("submit job 2 failed: %v", err)
	}

	if err := e1.Stop(); err != nil {
		t.Fatalf("stop engine 1 failed: %v", err)
	}

	// Phase 2: Boot engine 2 with same config, call Restore(), verify jobs persisted
	e2, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("failed to create engine 2: %v", err)
	}
	defer e2.Stop()

	if err := e2.Restore(); err != nil {
		t.Fatalf("restore engine 2 failed: %v", err)
	}

	// Verify both jobs recovered from persistent BoltStore
	recovered1, ok1 := e2.Job(j1.ID)
	if !ok1 || recovered1.Type != "export_csv" {
		t.Fatalf("expected job 1 recovered, got ok=%v job=%+v", ok1, recovered1)
	}

	recovered2, ok2 := e2.Job(j2.ID)
	if !ok2 || recovered2.Type != "render_video" {
		t.Fatalf("expected job 2 recovered, got ok=%v job=%+v", ok2, recovered2)
	}

	// Submit job 3 in new engine instance, verify ID auto-increment continues seamlessly
	j3, err := e2.SubmitJob("send_notification", 5)
	if err != nil {
		t.Fatalf("submit job 3 failed: %v", err)
	}

	if j3.ID <= j2.ID {
		t.Fatalf("expected job 3 ID > job 2 ID (%d), got %d", j2.ID, j3.ID)
	}
}

func TestEngineRecoveryWhenWorkerDiesAndServerRestarts(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_worker_dies_server_restarts.db")

	cfg := config.Default()
	cfg.StorageType = "bolt"
	cfg.StoragePath = dbPath
	cfg.RaftEnabled = false

	// 1. Manually populate BoltStore with a job in Pending state
	bs1, err := store.NewBoltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create BoltStore: %v", err)
	}

	inFlightJob := job.NewJob(100, "critical_task", 50)
	inFlightJob.Status = job.Pending
	_ = bs1.Add(inFlightJob)
	_ = bs1.Close()

	// 2. Control node boots up (server restart scenario) with injected custom dbPath
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("failed to boot engine: %v", err)
	}
	defer e.Stop()

	if err := e.Restore(); err != nil {
		t.Fatalf("failed to restore engine: %v", err)
	}

	// 3. Verify job 100 status was restored to Pending
	recoveredJob, ok := e.Job(100)
	if !ok {
		t.Fatalf("expected job 100 to be found after restore")
	}

	if recoveredJob.Status != job.Pending {
		t.Fatalf("expected recovered job status Pending, got %v", recoveredJob.Status)
	}

	// 4. Verify job 100 was re-enqueued into PriorityQueue for execution
	enqueuedID, ok := e.PriorityQueue.Dequeue()
	if !ok || enqueuedID != 100 {
		t.Fatalf("expected job 100 to be re-enqueued in PriorityQueue, got id=%d, ok=%v", enqueuedID, ok)
	}
}

func TestEngineBulkDurabilityRecovery1000Jobs(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_bulk_1000.db")

	cfg := config.Default()
	cfg.StorageType = "bolt"
	cfg.StoragePath = dbPath
	cfg.RaftEnabled = false

	// Phase 1: Boot engine, submit 1000 jobs
	e1, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("failed to create engine 1: %v", err)
	}

	for i := 1; i <= 1000; i++ {
		_, err := e1.SubmitJob(fmt.Sprintf("job_type_%d", i%10), i%100)
		if err != nil {
			t.Fatalf("failed to submit job %d: %v", i, err)
		}
	}

	if len(e1.Jobs()) != 1000 {
		t.Fatalf("expected 1000 jobs in engine 1, got %d", len(e1.Jobs()))
	}

	if err := e1.Stop(); err != nil {
		t.Fatalf("failed to stop engine 1: %v", err)
	}

	// Phase 2: Boot engine 2 (simulated restart), call Restore()
	e2, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("failed to create engine 2: %v", err)
	}
	defer e2.Stop()

	if err := e2.Restore(); err != nil {
		t.Fatalf("failed to restore engine 2: %v", err)
	}

	// Verify all 1000 jobs recovered cleanly from test dbPath
	allJobs := e2.Jobs()
	if len(allJobs) != 1000 {
		t.Fatalf("expected 1000 jobs recovered in engine 2, got %d", len(allJobs))
	}

	// Submit job 1001 to verify nextJobID calculation
	j1001, err := e2.SubmitJob("next_batch", 50)
	if err != nil {
		t.Fatalf("failed to submit job 1001: %v", err)
	}

	if j1001.ID != 1001 {
		t.Fatalf("expected next job ID to be 1001, got %d", j1001.ID)
	}
}

func TestEngineRestoreScheduledJobsToDelayQueue(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "scheduled.db")

	cfg := config.Default()
	cfg.StorageType = "bolt"
	cfg.StoragePath = dbPath

	e1, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	runAt := time.Now().Add(10 * time.Minute)

	j, err := e1.SubmitJobAt(
		"scheduled_task",
		10,
		runAt,
	)
	if err != nil {
		t.Fatal(err)
	}

	_ = e1.Stop()

	e2, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Stop()

	if err := e2.Restore(); err != nil {
		t.Fatal(err)
	}

	id, ok := e2.DelayQueue.Dequeue()
	if !ok {
		t.Fatal("expected scheduled job in delay queue")
	}

	if id != j.ID {
		t.Fatalf("expected %d got %d", j.ID, id)
	}
}

func TestEngineRestoreCancelledJobsNotQueued(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "cancelled.db")

	cfg := config.Default()
	cfg.StorageType = "bolt"
	cfg.StoragePath = dbPath

	bs, _ := store.NewBoltStore(dbPath)

	j := job.NewJob(1, "email", 1)
	j.Status = job.Cancelled

	_ = bs.Add(j)
	_ = bs.Close()

	e, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Stop()

	if err := e.Restore(); err != nil {
		t.Fatal(err)
	}

	_, ok := e.PriorityQueue.Dequeue()

	if ok {
		t.Fatal("cancelled job should not be queued")
	}
}

func TestEngineRestoreCompletedJobsNotQueued(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "completed.db")

	cfg := config.Default()
	cfg.StorageType = "bolt"
	cfg.StoragePath = dbPath

	bs, _ := store.NewBoltStore(dbPath)

	j := job.NewJob(1, "email", 1)
	j.Status = job.Completed

	_ = bs.Add(j)
	_ = bs.Close()

	e, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Stop()

	if err := e.Restore(); err != nil {
		t.Fatal(err)
	}

	_, ok := e.PriorityQueue.Dequeue()

	if ok {
		t.Fatal("completed job should not be queued")
	}
}

func TestEngineRestoreFailedJobsNotQueued(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "failed.db")

	cfg := config.Default()
	cfg.StorageType = "bolt"
	cfg.StoragePath = dbPath

	bs, _ := store.NewBoltStore(dbPath)

	j := job.NewJob(1, "email", 1)
	j.Status = job.Failed

	_ = bs.Add(j)
	_ = bs.Close()

	e, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Stop()

	if err := e.Restore(); err != nil {
		t.Fatal(err)
	}

	_, ok := e.PriorityQueue.Dequeue()

	if ok {
		t.Fatal("failed job should not be queued")
	}
}

func TestEngineRestoreEmptyDatabase(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "empty.db")

	cfg := config.Default()
	cfg.StorageType = "bolt"
	cfg.StoragePath = dbPath

	e, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Stop()

	if err := e.Restore(); err != nil {
		t.Fatal(err)
	}

	if len(e.Jobs()) != 0 {
		t.Fatalf("expected 0 jobs got %d", len(e.Jobs()))
	}
}

func TestEngineMultipleRestarts(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "multi.db")

	cfg := config.Default()
	cfg.StorageType = "bolt"
	cfg.StoragePath = dbPath

	e1, _ := engine.New(cfg)

	for i := 0; i < 50; i++ {
		_, _ = e1.SubmitJob("email", 1)
	}

	_ = e1.Stop()

	for i := 0; i < 5; i++ {
		e, err := engine.New(cfg)
		if err != nil {
			t.Fatal(err)
		}

		if err := e.Restore(); err != nil {
			t.Fatal(err)
		}

		if len(e.Jobs()) != 50 {
			t.Fatalf("restart %d lost jobs", i)
		}

		_ = e.Stop()
	}
}

func TestEngineCancelPersistsAcrossRestart(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "cancelpersist.db")

	cfg := config.Default()
	cfg.StorageType = "bolt"
	cfg.StoragePath = dbPath

	e1, _ := engine.New(cfg)

	j, _ := e1.SubmitJob("email", 1)

	if err := e1.CancelJob(j.ID); err != nil {
		t.Fatal(err)
	}

	_ = e1.Stop()

	e2, _ := engine.New(cfg)
	defer e2.Stop()

	_ = e2.Restore()

	recovered, ok := e2.Job(j.ID)

	if !ok {
		t.Fatal("job missing")
	}

	if recovered.Status != job.Cancelled {
		t.Fatalf("expected cancelled got %v", recovered.Status)
	}
}

func TestEngineDeletePersistsAcrossRestart(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "deletepersist.db")

	cfg := config.Default()
	cfg.StorageType = "bolt"
	cfg.StoragePath = dbPath

	e1, _ := engine.New(cfg)

	j, _ := e1.SubmitJob("email", 1)

	e1.DeleteJob(j.ID)

	_ = e1.Stop()

	e2, _ := engine.New(cfg)
	defer e2.Stop()

	_ = e2.Restore()

	_, ok := e2.Job(j.ID)

	if ok {
		t.Fatal("deleted job should not exist")
	}
}
