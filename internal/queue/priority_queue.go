package queue

import (
	"quorum/internal/store"
)

type PriorityQueue struct {
	items []int
	store *store.JobStore
}

func NewPriorityQueue(store *store.JobStore) PriorityQueue {
	return PriorityQueue{
		items: make([]int, 0),
		store: store,
	}
}

func (pq PriorityQueue) Len() int {
	return len(pq.items)
}

func (pq PriorityQueue) Less(i, j int) bool {
	left, _ := pq.store.Get(pq.items[i])
	right, _ := pq.store.Get(pq.items[j])

	return left.Priority > right.Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

func (pq *PriorityQueue) Push(x any) {
	id := x.(int)
	pq.items = append(pq.items, id)
}

func (pq *PriorityQueue) Pop() any {
	old := pq.items
	n := len(old)
	id := old[n-1]
	pq.items = old[:n-1]
	return id
}