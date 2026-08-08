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
