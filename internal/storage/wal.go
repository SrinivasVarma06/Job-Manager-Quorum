package storage

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"quorum/internal/job"
)

type WAL struct {
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

func (w *WAL) appendRecord(record record) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.file.Write(data)
	return err
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
					delete(pending, r.Job.ID)

				case "cancel":
					delete(pending, r.JobID)

				case "complete":
					delete(pending, r.JobID)
				}
			continue
		}
		var legacyJob job.Job
		if err := json.Unmarshal(line, &legacyJob); err != nil {
			// If unmarshaling fails on a line, it indicates trailing corrupted/truncated
			// data (e.g. from an abrupt power outage during append). We stop scanning
			// and return all valid records read up to this point.
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
	return w.file.Close()
}
