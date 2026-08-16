package broker

import (
	"log/slog"
	"strings"
	"sync"

	"quorum/internal/worker"
)

// Broker manages topic subscriptions and capability-based job routing for workers.
//
// In Quorum, workers can subscribe to specific job topics (e.g., "email",
// "video_processing", "data_indexing") or wildcard topics ("*").
//
// Responsibilities:
//   - Track worker topic capabilities.
//   - Route pending jobs to compatible workers.
//   - Enforce isolation between specialized worker pools.
type Broker struct {
	mu           sync.RWMutex
	workerTopics map[int]map[string]struct{} // workerID -> set of topics
	workerIsAll  map[int]bool                // workerID -> supports all topics ("*")
}

// New creates a new, empty Broker instance.
func New() *Broker {
	return &Broker{
		workerTopics: make(map[int]map[string]struct{}),
		workerIsAll:  make(map[int]bool),
	}
}

// RegisterWorker records the topics supported by workerID.
// If topics is empty or contains "*", the worker is registered as a wildcard
// worker that can process any job type.
func (b *Broker) RegisterWorker(workerID int, topics []string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	topicSet := make(map[string]struct{})
	isAll := len(topics) == 0

	for _, t := range topics {
		trimmed := strings.TrimSpace(t)
		if trimmed == "*" || trimmed == "all" || trimmed == "" {
			isAll = true
			break
		}
		topicSet[trimmed] = struct{}{}
	}

	b.workerTopics[workerID] = topicSet
	b.workerIsAll[workerID] = isAll

	if isAll {
		slog.Info("Worker subscribed to all topics (*)", "worker_id", workerID)
	} else {
		slog.Info("Worker subscribed to topics", "worker_id", workerID, "topics", topics)
	}
}

// UnregisterWorker removes all topic subscriptions for workerID.
func (b *Broker) UnregisterWorker(workerID int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.workerTopics, workerID)
	delete(b.workerIsAll, workerID)
	slog.Info("Worker unsubscribed from broker", "worker_id", workerID)
}

// CanHandle returns true if workerID can process jobs of type jobType.
func (b *Broker) CanHandle(workerID int, jobType string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.workerIsAll[workerID] {
		return true
	}

	topics, ok := b.workerTopics[workerID]
	if !ok {
		// If worker is not registered in broker yet, default to wildcard for backward compatibility
		return true
	}

	_, supported := topics[jobType]
	return supported
}

// SelectWorker attempts to select a worker from availableChan capable of executing jobType.
// If a capable worker is found, it is returned. If un-matching workers are popped from the channel
// during search, they are put back into availableChan.
func (b *Broker) SelectWorker(jobType string, availableChan chan worker.WorkerClient) (worker.WorkerClient, bool) {
	n := len(availableChan)
	if n == 0 {
		return nil, false
	}

	var skipped []worker.WorkerClient

Loop:
	for i := 0; i < n; i++ {
		select {
		case w := <-availableChan:
			if b.CanHandle(w.ID(), jobType) {
				// Re-enqueue any workers that were popped but did not match jobType
				for _, unmatching := range skipped {
					select {
					case availableChan <- unmatching:
					default:
					}
				}
				return w, true
			}
			skipped = append(skipped, w)
		default:
			break Loop
		}
	}

	// Re-enqueue all skipped workers if no matching worker was found
	for _, unmatching := range skipped {
		select {
		case availableChan <- unmatching:
		default:
		}
	}

	return nil, false
}
