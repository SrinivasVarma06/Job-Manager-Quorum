package workermanager

import (
	"quorum/internal/worker"
	"quorum/internal/config"
	"sync"
	"time"
	"context"
	"fmt"
)

type WorkerInfo struct {
	Client        worker.WorkerClient
	LastHeartbeat time.Time
	Alive         bool
}

type Manager struct {
	workers map[int]*WorkerInfo
	Available chan worker.WorkerClient
	mu        sync.RWMutex
	DeadWorkers chan int
}

func NewManager() *Manager {
	return &Manager{
		workers:   make(map[int]*WorkerInfo),
		Available: make(chan worker.WorkerClient, 100),
		DeadWorkers: make(chan int, config.Default().WorkerCount),
	}
}

func (m *Manager) Register(w worker.WorkerClient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workers[w.ID()] = &WorkerInfo{
		Client:        w,
		LastHeartbeat: time.Now(),
		Alive:         true,
	}
}

func (m *Manager) Remove(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.workers, id)
}

func (m *Manager) Get(id int) (worker.WorkerClient, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	info, ok := m.workers[id]
	if !ok {
		return nil, false
	}
	return info.Client, true
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
	for _, info := range m.workers {
		if info.Alive {
			workers = append(workers, info.Client)
		}
	}
	return workers
}

func (m *Manager) Heartbeat(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	info, ok := m.workers[id]
	if !ok {
		return
	}
	info.LastHeartbeat = time.Now()
	info.Alive = true
}

func (m *Manager) Monitor(ctx context.Context, timeout time.Duration) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			m.mu.Lock()
			for _, info := range m.workers {
				if time.Since(info.LastHeartbeat) > timeout {
					if info.Alive {
						info.Alive = false
						fmt.Printf("Worker %d timed out\n", info.Client.ID())
						select {
							case m.DeadWorkers <- info.Client.ID():
							default:
						}
					}
				}

			}
			m.mu.Unlock()
		}
	}

}