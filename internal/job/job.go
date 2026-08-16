package job

import (
	"time"
)

// Status represents the persistent desired state of a Job stored in BoltDB via Raft.
//
// Following the Temporal / Kubernetes model, Status contains ONLY persistent control-plane states:
//   - Pending: Job submitted, ready for execution.
//   - Scheduled: Job delayed for future execution.
//   - Completed: Job finished successfully (terminal state).
//   - Failed: Job exhausted retries and failed (terminal state).
//   - Cancelled: Job cancelled by user (terminal state).
//
// Execution tracking (which worker is running a job, attempt counter, leases) is kept in
// ephemeral leader memory (LeaseManager) so a leader crash never leaves stale "RUNNING"
// entries on disk or in the Raft log.
type Status string

const (
	Pending   Status = "PENDING"
	Scheduled Status = "SCHEDULED"
	Completed Status = "COMPLETED"
	Failed    Status = "FAILED"
	Cancelled Status = "CANCELLED"
)

// Job represents the persistent definition and desired state of a unit of work in Quorum.
type Job struct {
	ID             int       `json:"id"`
	Type           string    `json:"type"`
	Priority       int       `json:"priority"`
	Status         Status    `json:"status"`
	RetryCount     int       `json:"retry_count"`
	MaxRetries     int       `json:"max_retries"`
	NextRunAt      time.Time `json:"next_run_at"`
	LastError      string    `json:"last_error"`
	IdempotencyKey string    `json:"idempotency_key"`
}

func NewJob(id int, jobType string, priority int) Job {
	return Job{
		ID:         id,
		Type:       jobType,
		Priority:   priority,
		Status:     Pending,
		MaxRetries: 3,
	}
}

// IsValidTransition checks whether transitioning from current to next status is legal.
func IsValidTransition(current, next Status) bool {
	if current == next {
		return true
	}
	switch current {
	case Pending:
		return next == Scheduled || next == Completed || next == Failed || next == Cancelled
	case Scheduled:
		return next == Pending || next == Cancelled
	case Completed, Failed, Cancelled:
		// Terminal states cannot transition to anything else
		return false
	default:
		return false
	}
}
