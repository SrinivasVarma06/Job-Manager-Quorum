package workermanager

import (
	"quorum/internal/worker"
	"sync"
)

type Manager struct {
	workers   map[int]worker.WorkerClient
	Available chan worker.WorkerClient
	mu        sync.RWMutex
}

func NewManager() *Manager {
	return &Manager{
		workers:   make(map[int]worker.WorkerClient),
		Available: make(chan worker.WorkerClient, 100),
	}
}

func (m *Manager) Register(w worker.WorkerClient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workers[w.ID()] = w	
}

func (m *Manager) Remove(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.workers, id)
}

func (m *Manager) Get(id int) (worker.WorkerClient, bool) {
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

func (m *Manager) List() []worker.WorkerClient {
	m.mu.RLock()
	defer m.mu.RUnlock()
	workers := make([]worker.WorkerClient, 0, len(m.workers))
	for _, w := range m.workers {
		workers = append(workers, w)
	}
	return workers
}
