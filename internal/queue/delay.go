package queue

import "quorum/internal/store"

func DelayComparator(aID, bID int, store *store.JobStore) bool {
	a, _ := store.Get(aID)
	b, _ := store.Get(bID)
	return a.NextRunAt.Before(b.NextRunAt)
}
