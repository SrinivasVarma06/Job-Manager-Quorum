package queue

import "quorum/internal/store"

type Comparator func(aID, bID int, store *store.JobStore) bool

type Heap struct {
	items      []int
	store      *store.JobStore
	comparator Comparator
}

func (h Heap) Len() int {
	return len(h.items)
}

func (h Heap) Less(i, j int) bool {
	return h.comparator(h.items[i], h.items[j], h.store)
}

func (h Heap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *Heap) Push(x any) {
	h.items = append(h.items, x.(int))
}

func (h *Heap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}
