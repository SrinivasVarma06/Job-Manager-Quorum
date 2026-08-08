package workermanager

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"quorum/internal/broker"
	"quorum/internal/config"
	"quorum/internal/worker"
)

type WorkerInfo struct {
	Client        worker.WorkerClient
	LastHeartbeat time.Time
	Alive         bool
	Topics        []string
	Address       string
}

type NodeSnapshot struct {
	ID            int       `json:"id"`
	Address       string    `json:"address"`
	Alive         bool      `json:"alive"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Topics        []string  `json:"topics"`
}

type Manager struct {
	workers     map[int]*WorkerInfo
	Available   chan worker.WorkerClient
	mu          sync.RWMutex
	DeadWorkers chan int
	Broker      *broker.Broker
}

func NewManager() *Manager {
	return &Manager{
		workers:     make(map[int]*WorkerInfo),
		Available:   make(chan worker.WorkerClient, 100),
		DeadWorkers: make(chan int, config.Default().WorkerCount),
		Broker:      broker.New(),
	}
}

func (m *Manager) Register(w worker.WorkerClient, address string, topics ...string) {
	m.mu.Lock()
	m.workers[w.ID()] = &WorkerInfo{
		Client:        w,
		LastHeartbeat: time.Now(),
		Alive:         true,
		Topics:        topics,
		Address:       address,
	}
	m.mu.Unlock()

	m.Broker.RegisterWorker(w.ID(), topics)
}

// MakeAvailable sends the worker into the Available channel so the scheduler
// can pick it up for dispatch.
func (m *Manager) MakeAvailable(w worker.WorkerClient) {
	select {
	case m.Available <- w:
	default:
	}
}

func (m *Manager) Remove(id int) {
	m.mu.Lock()
	delete(m.workers, id)
	m.mu.Unlock()

	m.Broker.UnregisterWorker(id)
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

func (m *Manager) Nodes() []NodeSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := make([]NodeSnapshot, 0, len(m.workers))
	for id, info := range m.workers {
		nodes = append(nodes, NodeSnapshot{
			ID:            id,
			Address:       info.Address,
			Alive:         info.Alive,
			LastHeartbeat: info.LastHeartbeat,
			Topics:        info.Topics,
		})
	}
	return nodes
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
						slog.Warn("Worker timed out", "worker_id", info.Client.ID())
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
