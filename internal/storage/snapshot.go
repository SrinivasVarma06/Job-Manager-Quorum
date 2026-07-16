package storage

import (
	"encoding/json"
	"os"

	"quorum/internal/job"
)

type Snapshot struct {
	Path string
}

func NewSnapshot(path string) *Snapshot {
	return &Snapshot{
		Path: path,
	}
}

func (s *Snapshot) Save(jobs []job.Job) error {

	data, err := json.MarshalIndent(jobs, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(s.Path, data, 0644)
}

func (s *Snapshot) Load() ([]job.Job, error) {
	data, err := os.ReadFile(s.Path)
	if os.IsNotExist(err) {
		return []job.Job{}, nil
	}
	if err != nil {
		return nil, err
	}
	var jobs []job.Job
	err = json.Unmarshal(data, &jobs)
	if err != nil {
		return nil, err
	}

	return jobs, nil
}