package job

import (
	"sync"
	"time"
)

// Lease represents an ephemeral execution ownership claim held by a worker.
// It exists ONLY in the active Raft Leader's memory. If the leader crashes,
// all leases evaporate cleanly without leaving corrupted states in persistent storage.
type Lease struct {
	JobID     int       `json:"job_id"`
	WorkerID  int       `json:"worker_id"`
	Term      uint64    `json:"term"`
	Attempt   int       `json:"attempt"`
	StartedAt time.Time `json:"started_at"`
}

// LeaseManager tracks active execution leases on the active Raft Leader node.
type LeaseManager struct {
	mu     sync.RWMutex
	leases map[int]*Lease // jobID -> Lease
}

func NewLeaseManager() *LeaseManager {
	return &LeaseManager{
		leases: make(map[int]*Lease),
	}
}

// Acquire creates or updates an execution lease for jobID to workerID.
func (lm *LeaseManager) Acquire(jobID int, workerID int, term uint64, attempt int) *Lease {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lease := &Lease{
		JobID:     jobID,
		WorkerID:  workerID,
		Term:      term,
		Attempt:   attempt,
		StartedAt: time.Now(),
	}
	lm.leases[jobID] = lease
	return lease
}

// Get retrieves the active execution lease for a jobID.
func (lm *LeaseManager) Get(jobID int) (*Lease, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	lease, ok := lm.leases[jobID]
	if !ok {
		return nil, false
	}
	return lease, true
}

// Release removes the lease for jobID.
func (lm *LeaseManager) Release(jobID int) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	delete(lm.leases, jobID)
}

// ReleaseByWorker removes all leases owned by workerID (e.g. on worker heartbeat timeout).
func (lm *LeaseManager) ReleaseByWorker(workerID int) []int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	var released []int
	for jobID, lease := range lm.leases {
		if lease.WorkerID == workerID {
			released = append(released, jobID)
			delete(lm.leases, jobID)
		}
	}
	return released
}

// Clear drops all active leases (e.g. when stepping down from Raft Leadership).
func (lm *LeaseManager) Clear() {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.leases = make(map[int]*Lease)
}
