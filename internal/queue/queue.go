package queue

import (
	"container/heap"
	"quorum/internal/store"
	"sync"
)

type JobQueue struct {
	heap   Heap
	notify chan struct{}
	mu     sync.RWMutex
}

func NewJobQueue(store store.Store, cmp Comparator) *JobQueue {
	q := &JobQueue{
		heap: Heap{
			items:      make([]int, 0),
			store:      store,
			comparator: cmp,
		},
		notify: make(chan struct{}, 1),
	}
	heap.Init(&q.heap)
	return q
}

func (q *JobQueue) Enqueue(id int) {
	q.mu.Lock()
	heap.Push(&q.heap, id)
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *JobQueue) Dequeue() (int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.heap.Len() == 0 {
		return 0, false
	}
	id := heap.Pop(&q.heap).(int)
	return id, true
}

func (q *JobQueue) Wait() <-chan struct{} {
	return q.notify
}

func (q *JobQueue) Peek() (int, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.heap.Len() == 0 {
		return 0, false
	}
	return q.heap.items[0], true
}
