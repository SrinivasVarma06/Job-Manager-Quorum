package store

import "quorum/internal/job"

// Store is the interface all job storage backends must implement.
//
// Quorum currently ships two implementations:
//   - MemoryStore  — in-memory, zero latency, lost on restart (testing / local mode)
//   - BoltStore    — bbolt-backed, durable across restarts (production mode)
//
// The scheduler, engine, runner and handlers all depend on this interface so
// the backing store can be swapped without touching any of those packages.
type Store interface {
	// Add persists a new job. Callers must ensure the ID is unique.
	Add(j job.Job)

	// Get returns the job with the given ID. Returns false if not found.
	Get(id int) (job.Job, bool)

	// List returns all jobs in arbitrary order.
	List() []job.Job

	// Update overwrites the stored state for an existing job.
	Update(j job.Job)

	// Delete removes a job. Returns false if the job was not found.
	Delete(id int) bool

	// Cancel marks a job as cancelled. Returns an error if the job is in a
	// terminal state (completed, running, or already cancelled).
	Cancel(id int) error

	// RunningJobs returns all jobs currently assigned to the given worker
	// with status Running. Used by the recovery loop after a worker dies.
	RunningJobs(workerID int) []job.Job
}
