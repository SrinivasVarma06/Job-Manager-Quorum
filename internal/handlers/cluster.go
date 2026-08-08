package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"quorum/internal/engine"
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
	TotalNodes    int `json:"total_nodes"`
	ActiveNodes   int `json:"active_nodes"`
	DeadNodes     int `json:"dead_nodes"`
	TotalJobs     int `json:"total_jobs"`
	PendingJobs   int `json:"pending_jobs"`
	RunningJobs   int `json:"running_jobs"`
	CompletedJobs int `json:"completed_jobs"`
	FailedJobs    int `json:"failed_jobs"`
	CancelledJobs int `json:"cancelled_jobs"`
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
	running := 0
	completed := 0
	failed := 0
	cancelled := 0

	for _, j := range jobs {
		switch j.Status {
		case job.Pending, job.Scheduled, job.Retrying:
			pending++
		case job.Running:
			running++
		case job.Completed:
			completed++
		case job.Failed:
			failed++
		case job.Cancelled:
			cancelled++
		}
	}

	status := ClusterStatus{
		TotalNodes:    len(nodes),
		ActiveNodes:   activeNodes,
		DeadNodes:     deadNodes,
		TotalJobs:     len(jobs),
		PendingJobs:   pending,
		RunningJobs:   running,
		CompletedJobs: completed,
		FailedJobs:    failed,
		CancelledJobs: cancelled,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
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
