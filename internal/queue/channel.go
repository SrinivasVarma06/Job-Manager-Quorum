package queue

import (
	"sync"
	"quorum/internal/job"
)

type JobQueue struct {
	Channel chan job.Job
	Pending map[int]job.Job
	mu sync.RWMutex
}

func NewJobQueue() *JobQueue {
	return &JobQueue{
		Channel: make(chan job.Job, 100),
		Pending: make(map[int]job.Job),
	}
}

func (q *JobQueue) Enqueue(j job.Job) {

	q.mu.Lock()
	q.Pending[j.ID] = j
	q.mu.Unlock()

	q.Channel <- j
}

func (q *JobQueue) Complete(id int) {
	q.mu.Lock()
	delete(q.Pending, id)
	q.mu.Unlock()
}

func (q *JobQueue) Snapshot() []job.Job {

	q.mu.RLock()
	defer q.mu.RUnlock()

	jobs := make([]job.Job, 0, len(q.Pending))

	for _, j := range q.Pending {
		jobs = append(jobs, j)
	}

	return jobs
}