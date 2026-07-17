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

type walRecord struct {
	Kind  string   `json:"kind"`
	Job   *job.Job `json:"job,omitempty"`
	JobID int      `json:"job_id,omitempty"`
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

func (w *WAL) Append(j job.Job) error {
	record := walRecord{
		Kind: "submit",
		Job:  &j,
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.file.Write(data)
	return err
}

func (w *WAL) AppendCompletion(jobID int) error {
	record := walRecord{
		Kind:  "complete",
		JobID: jobID,
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.file.Write(data)
	return err
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

		var record walRecord
		if err := json.Unmarshal(line, &record); err == nil && record.Kind != "" {
			switch record.Kind {
			case "submit":
				if record.Job != nil {
					pending[record.Job.ID] = *record.Job
				}
			case "complete":
				delete(pending, record.JobID)
			}
			continue
		}

		// Backward compatibility for legacy WAL format where each line is a plain Job.
		var legacyJob job.Job
		if err := json.Unmarshal(line, &legacyJob); err != nil {
			return nil, err
		}
		pending[legacyJob.ID] = legacyJob
	}

	if err := scanner.Err(); err != nil {
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
