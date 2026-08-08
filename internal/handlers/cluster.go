package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"quorum/internal/engine"
	"quorum/internal/events"
	"quorum/internal/httpapi"
	"quorum/internal/job"
)

type ClusterHandler struct {
	Engine *engine.Engine
}

func NewClusterHandler(e *engine.Engine) *ClusterHandler {
	return &ClusterHandler{Engine: e}
}

type ClusterStatus struct {
	TotalNodes    int    `json:"total_nodes"`
	ActiveNodes   int    `json:"active_nodes"`
	DeadNodes     int    `json:"dead_nodes"`
	TotalJobs     int    `json:"total_jobs"`
	PendingJobs   int    `json:"pending_jobs"`
	CompletedJobs int    `json:"completed_jobs"`
	FailedJobs    int    `json:"failed_jobs"`
	CancelledJobs int    `json:"cancelled_jobs"`
	LeaderNode    string `json:"leader_node"`
	RaftTerm      uint64 `json:"raft_term"`
}

// Nodes returns a JSON list of all registered nodes in the cluster.
// GET /cluster/nodes
func (h *ClusterHandler) Nodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpapi.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	nodes := h.Engine.WorkerManager.Nodes()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(nodes)
}

// Status returns aggregated metrics for the cluster.
// GET /cluster/status
func (h *ClusterHandler) Status(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpapi.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	nodes := h.Engine.WorkerManager.Nodes()
	activeNodes := 0
	deadNodes := 0
	for _, n := range nodes {
		if n.Alive {
			activeNodes++
		} else {
			deadNodes++
		}
	}

	jobs := h.Engine.Jobs()
	pending := 0
	completed := 0
	failed := 0
	cancelled := 0

	for _, j := range jobs {
		switch j.Status {
		case job.Pending, job.Scheduled:
			pending++
		case job.Completed:
			completed++
		case job.Failed:
			failed++
		case job.Cancelled:
			cancelled++
		}
	}

	leaderNode := "node-1"
	term := uint64(1)
	if h.Engine.RaftNode != nil {
		leaderNode = h.Engine.RaftNode.LeaderAddr()
		if leaderNode == "" {
			leaderNode = "node-1"
		}
		term = h.Engine.RaftNode.LeaderTerm()
	}

	status := ClusterStatus{
		TotalNodes:    len(nodes),
		ActiveNodes:   activeNodes,
		DeadNodes:     deadNodes,
		TotalJobs:     len(jobs),
		PendingJobs:   pending,
		CompletedJobs: completed,
		FailedJobs:    failed,
		CancelledJobs: cancelled,
		LeaderNode:    leaderNode,
		RaftTerm:      term,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

// RaftStatus returns Raft node telemetry.
// GET /cluster/raft
func (h *ClusterHandler) RaftStatus(w http.ResponseWriter, r *http.Request) {
	isLeader := true
	term := uint64(1)
	leaderAddr := "127.0.0.1:18088"

	if h.Engine.RaftNode != nil {
		isLeader = h.Engine.RaftNode.IsLeader()
		term = h.Engine.RaftNode.LeaderTerm()
		leaderAddr = h.Engine.RaftNode.LeaderAddr()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"is_leader":   isLeader,
		"term":        term,
		"leader_addr": leaderAddr,
		"node_id":     h.Engine.Config.RaftNodeID,
	})
}

// Leases returns active ephemeral execution claims from LeaseManager.
// GET /jobs/leases
func (h *ClusterHandler) Leases(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// Returns leases map or list
	_ = json.NewEncoder(w).Encode(h.Engine.Scheduler.Leases)
}

// FailoverSimulate triggers a simulated leader failover sequence for UI demo.
// POST /cluster/failover-simulate
func (h *ClusterHandler) FailoverSimulate(w http.ResponseWriter, r *http.Request) {
	b := events.Global()

	go func() {
		b.Broadcast(events.Event{
			Type:      events.EventFailoverTriggered,
			Message:   "Simulated Leader Crash triggered: Leader node-1 terminated",
			Timestamp: time.Now(),
		})

		time.Sleep(200 * time.Millisecond)
		b.Broadcast(events.Event{
			Type:      events.EventLeaderChanged,
			Message:   "Raft election started across active peers",
			Timestamp: time.Now(),
		})

		time.Sleep(400 * time.Millisecond)
		b.Broadcast(events.Event{
			Type:      events.EventLeaderChanged,
			Message:   "New Raft Leader elected: node-2 (Term incremented)",
			Timestamp: time.Now(),
		})

		time.Sleep(200 * time.Millisecond)
		b.Broadcast(events.Event{
			Type:      events.EventQueueRebuilt,
			Message:   "Rebuilt scheduler queues from BoltDB status buckets (O(k) scan)",
			Timestamp: time.Now(),
		})

		time.Sleep(200 * time.Millisecond)
		b.Broadcast(events.Event{
			Type:      events.EventWorkerRegistered,
			Message:   "Active remote workers re-registered during 1s grace period",
			Timestamp: time.Now(),
		})

		time.Sleep(200 * time.Millisecond)
		b.Broadcast(events.Event{
			Type:      events.EventDispatchResumed,
			Message:   "Scheduler & Cron dispatch loops resumed cleanly (Failover duration: 1.2s)",
			Timestamp: time.Now(),
		})
	}()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"simulated": true,
		"message":   "Failover simulation started",
	})
}

// NodeRoute handles node-specific operations (e.g. node eviction).
// DELETE /cluster/nodes/{id}
func (h *ClusterHandler) NodeRoute(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/cluster/nodes/")
	if path == "" || path == r.URL.Path {
		h.Nodes(w, r)
		return
	}

	id, err := strconv.Atoi(path)
	if err != nil {
		httpapi.WriteError(w, http.StatusBadRequest, "invalid node ID")
		return
	}

	switch r.Method {
	case http.MethodDelete:
		h.Engine.WorkerManager.Remove(id)
		events.Global().Broadcast(events.Event{
			Type:    events.EventWorkerEvicted,
			Message: fmt.Sprintf("Worker %d evicted from cluster", id),
		})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"evicted": true,
			"node_id": id,
			"message": "node evicted from cluster",
		})
	default:
		httpapi.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}
