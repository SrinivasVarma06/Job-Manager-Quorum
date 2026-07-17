package workermanager

import (
	"quorum/internal/worker"
	"sync"
)

type Manager struct {
	workers   map[int]*worker.Worker
	Available chan *worker.Worker
	mu        sync.RWMutex
}

func NewManager() *Manager {
	return &Manager{
		workers:   make(map[int]*worker.Worker),
		Available: make(chan *worker.Worker, 100),
	}
}

func (m *Manager) Register(w *worker.Worker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workers[w.ID] = w
}

func (m *Manager) Remove(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.workers, id)
}

func (m *Manager) Get(id int) (*worker.Worker, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w, ok := m.workers[id]
	return w, ok
}

func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.workers)
}
