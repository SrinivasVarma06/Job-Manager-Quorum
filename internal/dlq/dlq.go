package dlq

import (
	"sync"

	"quorum/internal/job"
)

type DeadLetterQueue struct {
	mu   sync.RWMutex
	jobs map[int]job.Job
}

func New() *DeadLetterQueue {
	return &DeadLetterQueue{
		jobs: make(map[int]job.Job),
	}
}

func (d *DeadLetterQueue) Add(j job.Job) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.jobs[j.ID] = j
}

func (d *DeadLetterQueue) Get(id int) (job.Job, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	j, ok := d.jobs[id]
	return j, ok
}

func (d *DeadLetterQueue) List() []job.Job {
	d.mu.RLock()
	defer d.mu.RUnlock()
	jobs := make([]job.Job, 0, len(d.jobs))
	for _, j := range d.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (d *DeadLetterQueue) Delete(id int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.jobs, id)
}