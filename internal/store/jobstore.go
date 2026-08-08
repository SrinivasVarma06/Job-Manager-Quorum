package store

// JobStore is a type alias for MemoryStore for backward compatibility.
type JobStore = MemoryStore

// NewJobStore is a backward-compatible alias for NewMemoryStore.
func NewJobStore() *MemoryStore {
	return NewMemoryStore()
}