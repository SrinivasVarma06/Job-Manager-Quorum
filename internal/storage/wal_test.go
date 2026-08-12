package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"quorum/internal/job"
	"quorum/internal/storage"
)

func TestWALCorruptedFileRecovery(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "corrupted.log")

	wal, err := storage.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}

	// Write 5 valid job records
	for i := 1; i <= 5; i++ {
		j := job.NewJob(i, "task", i)
		if err := wal.Append(j); err != nil {
			t.Fatalf("failed to append job %d: %v", i, err)
		}
	}

	// Close WAL file to flush writes
	if err := wal.Close(); err != nil {
		t.Fatalf("failed to close wal: %v", err)
	}

	// Append trailing corrupted garbage bytes to simulate abrupt power loss mid-write
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open wal for corrupting: %v", err)
	}
	_, err = file.WriteString(`{"kind":"submit","job":{"id":6,"type":"broken_json_incomplete...`)
	if err != nil {
		t.Fatalf("failed to write corrupted bytes: %v", err)
	}
	_ = file.Close()

	// Re-open WAL and perform replay
	reopenedWAL, err := storage.NewWal(walPath)
	if err != nil {
		t.Fatalf("failed to reopen wal: %v", err)
	}
	defer reopenedWAL.Close()

	recoveredJobs, err := reopenedWAL.Replay()
	if err != nil {
		t.Fatalf("expected WAL replay to succeed despite trailing corrupted bytes, got error: %v", err)
	}

	if len(recoveredJobs) != 5 {
		t.Fatalf("expected 5 valid jobs recovered, got %d", len(recoveredJobs))
	}
}

func TestSnapshotSaveLoad(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "snapshot.json")

	s := storage.NewSnapshot(path)

	original := []job.Job{
		job.NewJob(1, "email", 1),
		job.NewJob(2, "video", 2),
	}

	if err := s.Save(original); err != nil {
		t.Fatal(err)
	}

	loaded, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}

	if len(loaded) != 2 {
		t.Fatalf("expected 2 jobs got %d", len(loaded))
	}

	if loaded[0].ID != 1 || loaded[1].ID != 2 {
		t.Fatal("snapshot data mismatch")
	}
}

func TestSnapshotLoadMissingFile(t *testing.T) {
	tempDir := t.TempDir()

	s := storage.NewSnapshot(
		filepath.Join(tempDir, "missing.json"),
	)

	jobs, err := s.Load()

	if err != nil {
		t.Fatal(err)
	}

	if len(jobs) != 0 {
		t.Fatalf("expected empty jobs slice got %d", len(jobs))
	}
}

func TestWALMixedOperationsReplay(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "wal.log")

	wal, err := storage.NewWal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	j1 := job.NewJob(1, "email", 1)
	j2 := job.NewJob(2, "video", 1)
	j3 := job.NewJob(3, "report", 1)

	_ = wal.Append(j1)
	_ = wal.Append(j2)
	_ = wal.Append(j3)

	_ = wal.AppendCompletion(1)
	_ = wal.AppendCancel(2)

	jobs, err := wal.Replay()
	if err != nil {
		t.Fatal(err)
	}

	if len(jobs) != 1 {
		t.Fatalf("expected 1 surviving job got %d", len(jobs))
	}

	if jobs[0].ID != 3 {
		t.Fatalf("expected remaining job id=3 got %d", jobs[0].ID)
	}
}
