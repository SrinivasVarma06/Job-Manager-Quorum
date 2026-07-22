package queue

import (
	"container/heap"
	"quorum/internal/store"
	"sync"
)

type JobQueue struct {
	pq      PriorityQueue
	notify  chan struct{}
	mu      sync.Mutex
}

func NewJobQueue(store *store.JobStore) *JobQueue {
	q := &JobQueue{
		pq:NewPriorityQueue(store),	
		notify: make(chan struct{}, 1),
	}
	heap.Init(&q.pq)
	return q
}

func (q *JobQueue) Enqueue(id int) {
	q.mu.Lock()
	heap.Push(&q.pq, id)
	q.mu.Unlock()
	select {
		case q.notify <- struct{}{}:
		default:
	}
}

func (q *JobQueue) Dequeue() (int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pq.Len() == 0 {
		return 0, false
	}
	id := heap.Pop(&q.pq).(int)
	return id, true
}

func (q *JobQueue) Wait() <-chan struct{} {
	return q.notify
}
