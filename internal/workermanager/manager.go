package workermanager

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"quorum/internal/broker"
	"quorum/internal/metrics"
	"quorum/internal/worker"
)

// deadWorkersBufSize is the capacity of the DeadWorkers channel.
// It must be > 0. In fully-distributed mode WorkerCount defaults to 0,
// so using that as the buffer was an unintentional unbuffered channel.
const deadWorkersBufSize = 100

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
		DeadWorkers: make(chan int, deadWorkersBufSize),
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

	// Update active workers metric
	metrics.ActiveWorkers.Inc()

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
	info, ok := m.workers[id]
	if ok {
		if info.Alive {
			// Decrement active workers metric when removing an alive worker
			metrics.ActiveWorkers.Dec()
		}
		delete(m.workers, id)
	}
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
	wasAlive := info.Alive
	info.LastHeartbeat = time.Now()
	info.Alive = true
	// If recovering from previously dead state, increment active workers metric
	if !wasAlive {
		metrics.ActiveWorkers.Inc()
	}
}

// Monitor polls worker liveness every timeout/2 (or 100ms, whichever is
// smaller) so that tests with short timeouts get sub-millisecond detection
// instead of waiting for a hardcoded 1-second tick.
func (m *Manager) Monitor(ctx context.Context, timeout time.Duration) {
	interval := timeout / 2
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
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
						// Decrement active workers metric on timeout
						metrics.ActiveWorkers.Dec()
						slog.Warn("Worker timed out", "worker_id", info.Client.ID())
						select {
						case m.DeadWorkers <- info.Client.ID():
						default:
							slog.Error("DeadWorkers channel full, dropping timeout signal", "worker_id", info.Client.ID())
						}
					}
				}
			}
			m.mu.Unlock()
		}
	}
}
