package queue

import "quorum/internal/store"

func PriorityComparator(aID, bID int, store store.Store) bool {
	a, _ := store.Get(aID)
	b, _ := store.Get(bID)
	return a.Priority > b.Priority
}
