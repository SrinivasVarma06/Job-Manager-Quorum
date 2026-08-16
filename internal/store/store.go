package store

import (
	"quorum/internal/cron"
	"quorum/internal/job"
)

// Store is the interface all job storage backends must implement.
//
// Quorum shipped two implementations:
//   - MemoryStore — in-memory store with status & worker indexes (testing / local mode)
//   - BoltStore   — bbolt-backed durable store with dedicated status/cron/dlq buckets (production mode)
type Store interface {
	// Add persists a new job.
	Add(j job.Job) error

	// Get returns the job with the given ID. Returns false if not found.
	Get(id int) (job.Job, bool)

	// List returns all jobs in arbitrary order.
	List() []job.Job

	// ListByStatus returns all jobs matching the given desired status (O(k)).
	ListByStatus(status job.Status) ([]job.Job, error)

	// Update overwrites the stored state for an existing job. Enforces state transitions.
	Update(j job.Job) error

	// Delete removes a job. Returns false if not found.
	Delete(id int) (bool, error)

	// Cancel marks a job as cancelled. Returns error if in a terminal state.
	Cancel(id int) error

	// Cron persistence
	AddCron(c cron.CronJob) error
	DeleteCron(id string) (bool, error)
	ListCrons() ([]cron.CronJob, error)

	// DLQ persistence
	AddDLQ(j job.Job) error
	ListDLQ() ([]job.Job, error)

	// FindByIdempotencyKey returns the existing job for the given key.
	// Returns (zero, false) when the key is empty or not found.
	FindByIdempotencyKey(key string) (job.Job, bool)
}
