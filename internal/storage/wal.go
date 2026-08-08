package storage

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"sync"

	"quorum/internal/job"
)

// WAL is a Write-Ahead Log for crash recovery.
// It is safe for concurrent use — all writes are serialized via mu and fsynced
// after each record so the OS page cache cannot silently lose entries.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

type EventType string

const (
	EventSubmit   EventType = "submit"
	EventRetry    EventType = "retry"
	EventFailure  EventType = "failed"
	EventComplete EventType = "complete"
	EventCancel   EventType = "cancel"
)

type record struct {
	Kind  EventType `json:"kind"`
	Job   *job.Job  `json:"job,omitempty"`
	JobID int       `json:"job_id,omitempty"`
}

func NewWal(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file: file,
		path: path,
	}, nil
}

// appendRecord serializes record as one JSON line, writes it atomically under
// the mutex, and fsyncs the file descriptor before returning.
// A power-loss after Write but before Sync is safe: the record is treated as
// truncated/corrupted by Replay() and ignored, which is the standard WAL
// recovery behaviour.
func (w *WAL) appendRecord(record record) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err = w.file.Write(data); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Append(j job.Job) error {
	return w.appendRecord(record{
		Kind: EventSubmit,
		Job:  &j,
	})
}

func (w *WAL) AppendRetry(j job.Job) error {
	return w.appendRecord(record{
		Kind: EventRetry,
		Job:  &j,
	})
}

func (w *WAL) AppendFailure(j job.Job) error {
	return w.appendRecord(record{
		Kind: EventFailure,
		Job:  &j,
	})
}

func (w *WAL) AppendCompletion(jobID int) error {
	return w.appendRecord(record{
		Kind:  EventComplete,
		JobID: jobID,
	})
}

func (w *WAL) AppendCancel(jobID int) error {
	return w.appendRecord(record{
		Kind:  EventCancel,
		JobID: jobID,
	})
}

func (w *WAL) Replay() ([]job.Job, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	pending := make(map[int]job.Job)

	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		line := scanner.Bytes()
		var r record
		if err := json.Unmarshal(line, &r); err == nil && r.Kind != "" {
			switch r.Kind {
			case "submit":
				if r.Job != nil {
					pending[r.Job.ID] = *r.Job
				}
			case "retry":
				if r.Job != nil {
					pending[r.Job.ID] = *r.Job
				}
			case "failed":
				if r.Job != nil {
					delete(pending, r.Job.ID)
				}
			case "cancel":
				delete(pending, r.JobID)
			case "complete":
				delete(pending, r.JobID)
			}
			continue
		}
		var legacyJob job.Job
		if err := json.Unmarshal(line, &legacyJob); err != nil {
			// Corrupted or truncated tail — stop and recover whatever we have.
			break
		}
		pending[legacyJob.ID] = legacyJob
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		return nil, err
	}

	jobs := make([]job.Job, 0, len(pending))
	for _, j := range pending {
		jobs = append(jobs, j)
	}

	return jobs, nil
}

func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Close(); err != nil {
		return err
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	w.file = file
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
