package store_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"quorum/internal/job"
	"quorum/internal/store"
)

func TestBoltStoreCRUD(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_store.db")

	bs, err := store.NewBoltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create BoltStore: %v", err)
	}
	defer bs.Close()

	// 1. Add job
	j1 := job.NewJob(1, "email", 5)
	if err := bs.Add(j1); err != nil {
		t.Fatalf("failed to add job 1: %v", err)
	}

	// 2. Get job
	retrieved, ok := bs.Get(1)
	if !ok {
		t.Fatalf("expected job 1 to be found")
	}
	if retrieved.ID != 1 || retrieved.Type != "email" || retrieved.Priority != 5 {
		t.Fatalf("unexpected job retrieved: %+v", retrieved)
	}

	// 3. Update job
	retrieved.Status = job.Completed
	if err := bs.Update(retrieved); err != nil {
		t.Fatalf("failed to update job 1: %v", err)
	}

	updated, ok := bs.Get(1)
	if !ok || updated.Status != job.Completed {
		t.Fatalf("expected updated status Completed, got %+v", updated)
	}

	// 4. List jobs by status (O(k))
	j2 := job.NewJob(2, "image_resize", 10)
	_ = bs.Add(j2)

	pendingList, err := bs.ListByStatus(job.Pending)
	if err != nil || len(pendingList) != 1 || pendingList[0].ID != 2 {
		t.Fatalf("expected 1 pending job (ID 2), got %d (%v)", len(pendingList), err)
	}

	// 5. Cancel pending job
	j3 := job.NewJob(3, "report", 1)
	_ = bs.Add(j3)
	if err := bs.Cancel(3); err != nil {
		t.Fatalf("failed to cancel job 3: %v", err)
	}
	cancelled, _ := bs.Get(3)
	if cancelled.Status != job.Cancelled {
		t.Fatalf("expected job 3 status Cancelled, got %v", cancelled.Status)
	}

	// 6. Delete job
	deleted, err := bs.Delete(2)
	if err != nil || !deleted {
		t.Fatalf("expected delete job 2 to return true")
	}
	if _, ok := bs.Get(2); ok {
		t.Fatalf("expected job 2 to be deleted")
	}
}

func TestBoltStorePersistenceAcrossReopen(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_persist.db")

	// Open DB 1, write jobs, close
	{
		bs1, err := store.NewBoltStore(dbPath)
		if err != nil {
			t.Fatalf("failed to create BoltStore 1: %v", err)
		}
		j := job.NewJob(42, "payment_process", 99)
		j.NextRunAt = time.Now().Add(5 * time.Minute)
		_ = bs1.Add(j)
		_ = bs1.Close()
	}

	// Verify file exists on disk
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("db file does not exist at %s", dbPath)
	}

	// Open DB 2 (simulating process restart) and verify job persists
	{
		bs2, err := store.NewBoltStore(dbPath)
		if err != nil {
			t.Fatalf("failed to reopen BoltStore: %v", err)
		}
		defer bs2.Close()

		reopenedJob, ok := bs2.Get(42)
		if !ok {
			t.Fatalf("expected job 42 to persist across DB reopen")
		}

		if reopenedJob.Type != "payment_process" || reopenedJob.Priority != 99 {
			t.Fatalf("reopened job attributes mismatch: %+v", reopenedJob)
		}
	}
}
