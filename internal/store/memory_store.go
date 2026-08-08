package store

import (
	"errors"
	"quorum/internal/job"
	"sync"
)

// MemoryStore is an in-memory implementation of Store. It is suitable for
// development, testing, and local single-node deployments where durability
// across restarts is not required. All state is lost when the process exits.
//
// Thread safety: all methods are protected by a single RWMutex.
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[int]job.Job
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		jobs: make(map[int]job.Job),
	}
}

func (s *MemoryStore) Add(j job.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[j.ID] = j
}

func (s *MemoryStore) Get(id int) (job.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	return j, ok
}

func (s *MemoryStore) List() []job.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := make([]job.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (s *MemoryStore) Update(j job.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[j.ID] = j
}

func (s *MemoryStore) Delete(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[id]; !ok {
		return false
	}
	delete(s.jobs, id)
	return true
}

func (s *MemoryStore) Cancel(id int) error {
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

func (s *MemoryStore) RunningJobs(workerID int) []job.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var jobs []job.Job
	for _, j := range s.jobs {
		if j.WorkerID == workerID && j.Status == job.Running {
			jobs = append(jobs, j)
		}
	}
	return jobs
}
