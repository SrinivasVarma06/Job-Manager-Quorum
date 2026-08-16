package store

import (
	"errors"
	"fmt"
	"sync"

	"quorum/internal/cron"
	"quorum/internal/job"
)

// MemoryStore is an in-memory implementation of Store with O(k) secondary status indexing
// and an O(1) idempotency-key index.
type MemoryStore struct {
	mu               sync.RWMutex
	jobs             map[int]job.Job
	statusIndex      map[job.Status]map[int]struct{}
	crons            map[string]cron.CronJob
	dlq              map[int]job.Job
	idempotencyIndex map[string]int // idempotency key → job ID
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		jobs:             make(map[int]job.Job),
		statusIndex:      make(map[job.Status]map[int]struct{}),
		crons:            make(map[string]cron.CronJob),
		dlq:              make(map[int]job.Job),
		idempotencyIndex: make(map[string]int),
	}
}

func (s *MemoryStore) Add(j job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[j.ID]; exists {
		return fmt.Errorf("job %d already exists", j.ID)
	}

	s.jobs[j.ID] = j
	if j.IdempotencyKey != "" {
		s.idempotencyIndex[j.IdempotencyKey] = j.ID
	}
	s.addToStatusIndex(j.Status, j.ID)
	return nil
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

func (s *MemoryStore) ListByStatus(status job.Status) ([]job.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	idsSet, ok := s.statusIndex[status]
	if !ok {
		return []job.Job{}, nil
	}

	jobs := make([]job.Job, 0, len(idsSet))
	for id := range idsSet {
		if j, exists := s.jobs[id]; exists {
			jobs = append(jobs, j)
		}
	}
	return jobs, nil
}

func (s *MemoryStore) Update(j job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.jobs[j.ID]
	if !ok {
		return fmt.Errorf("job %d not found for update", j.ID)
	}

	if !job.IsValidTransition(existing.Status, j.Status) {
		return fmt.Errorf("invalid job status transition from %s to %s for job %d", existing.Status, j.Status, j.ID)
	}

	s.removeFromStatusIndex(existing.Status, j.ID)
	s.jobs[j.ID] = j
	s.addToStatusIndex(j.Status, j.ID)
	return nil
}

func (s *MemoryStore) Delete(id int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return false, nil
	}

	s.removeFromStatusIndex(j.Status, id)
	// Remove from idempotency index if present
	if j.IdempotencyKey != "" {
		delete(s.idempotencyIndex, j.IdempotencyKey)
	}
	delete(s.jobs, id)
	return true, nil
}

func (s *MemoryStore) Cancel(id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return errors.New("job not found")
	}

	if j.Status == job.Completed || j.Status == job.Failed || j.Status == job.Cancelled {
		return fmt.Errorf("job already in terminal status %s", j.Status)
	}

	s.removeFromStatusIndex(j.Status, id)
	j.Status = job.Cancelled
	s.jobs[id] = j
	s.addToStatusIndex(job.Cancelled, id)
	return nil
}

func (s *MemoryStore) AddCron(c cron.CronJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.crons[c.ID] = c
	return nil
}

func (s *MemoryStore) DeleteCron(id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.crons[id]; !ok {
		return false, nil
	}
	delete(s.crons, id)
	return true, nil
}

func (s *MemoryStore) ListCrons() ([]cron.CronJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	crons := make([]cron.CronJob, 0, len(s.crons))
	for _, c := range s.crons {
		crons = append(crons, c)
	}
	return crons, nil
}

func (s *MemoryStore) AddDLQ(j job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.dlq[j.ID] = j
	return nil
}

func (s *MemoryStore) ListDLQ() ([]job.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dlqJobs := make([]job.Job, 0, len(s.dlq))
	for _, j := range s.dlq {
		dlqJobs = append(dlqJobs, j)
	}
	return dlqJobs, nil
}

// FindByIdempotencyKey returns the job associated with key, if any.
// Empty key always returns (zero, false) — bypassing deduplication.
// This is O(1): it consults the in-memory index under a read-lock.
func (s *MemoryStore) FindByIdempotencyKey(key string) (job.Job, bool) {
	if key == "" {
		return job.Job{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	id, ok := s.idempotencyIndex[key]
	if !ok {
		return job.Job{}, false
	}
	j, ok := s.jobs[id]
	return j, ok
}

func (s *MemoryStore) addToStatusIndex(status job.Status, id int) {
	if s.statusIndex[status] == nil {
		s.statusIndex[status] = make(map[int]struct{})
	}
	s.statusIndex[status][id] = struct{}{}
}

func (s *MemoryStore) removeFromStatusIndex(status job.Status, id int) {
	if set, ok := s.statusIndex[status]; ok {
		delete(set, id)
	}
}
