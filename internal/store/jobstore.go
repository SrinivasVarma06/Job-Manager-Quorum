package store

import (
	"errors"
	"quorum/internal/job"
	"sync"
)

type JobStore struct {
	mu   sync.RWMutex
	jobs map[int]job.Job
}

func NewJobStore() *JobStore {
	return &JobStore{
		jobs: make(map[int]job.Job),
	}
}

func (s *JobStore) Add(j job.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[j.ID] = j
}

func (s *JobStore) Get(id int) (job.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	j, ok := s.jobs[id]
	return j, ok
}

func (s *JobStore) List() []job.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]job.Job, 0, len(s.jobs))

	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}

	return jobs
}

func (s *JobStore) Update(j job.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[j.ID] = j
}

func (s *JobStore) Delete(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[id]; !ok {
		return false
	}

	delete(s.jobs, id)
	return true
}

func (s *JobStore) Cancel(id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return errors.New("job not found")
	}

	switch j.Status {
	case job.Completed:
		return errors.New("job already completed")

	case job.Running:
		return errors.New("job already running")

	case job.Cancelled:
		return errors.New("job already cancelled")
	}

	j.Status = job.Cancelled
	s.jobs[id] = j

	return nil
}
