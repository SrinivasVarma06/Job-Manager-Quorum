package queue

import (
	"quorum/internal/store"
)

type DelayQueue struct {
	items []int
	store *store.JobStore
}

func NewDelayQueue(store *store.JobStore) DelayQueue {
	return DelayQueue{
		items: make([]int, 0),
		store: store,
	}
}

func (dq DelayQueue) Len() int {
	return len(dq.items)
}

func (dq DelayQueue) Less(i, j int) bool {
	left, _ := dq.store.Get(dq.items[i])
	right, _ := dq.store.Get(dq.items[j])
	return left.NextRunAt.Before(right.NextRunAt)
}

func (dq DelayQueue) Swap(i, j int) {
	dq.items[i], dq.items[j] = dq.items[j], dq.items[i]
}

func (dq *DelayQueue) Push(x any) {
	id := x.(int)
	dq.items = append(dq.items, id)
}

func (dq *DelayQueue) Pop() any {
	old := dq.items
	n := len(old)
	id := old[n-1]
	dq.items = old[:n-1]
	return id
}