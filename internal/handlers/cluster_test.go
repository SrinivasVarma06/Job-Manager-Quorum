package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"quorum/internal/config"
	"quorum/internal/engine"
	"quorum/internal/handlers"
	"quorum/internal/workermanager"
)

func TestClusterHandlerNodesAndStatus(t *testing.T) {
	defer func() {
		_ = os.Remove("jobs.log")
		_ = os.Remove("snapshot.json")
		_ = os.Remove("quorum.db")
	}()

	cfg := config.Default()
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer e.Stop()

	// Submit a test job
	_, err = e.SubmitJob("email", 5)
	if err != nil {
		t.Fatalf("failed to submit job: %v", err)
	}

	handler := handlers.NewClusterHandler(e)

	// 1. Test GET /cluster/status
	reqStatus := httptest.NewRequest(http.MethodGet, "/cluster/status", nil)
	rrStatus := httptest.NewRecorder()
	handler.Status(rrStatus, reqStatus)

	if rrStatus.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", rrStatus.Code)
	}

	var statusResp handlers.ClusterStatus
	if err := json.Unmarshal(rrStatus.Body.Bytes(), &statusResp); err != nil {
		t.Fatalf("failed to unmarshal cluster status response: %v", err)
	}

	if statusResp.TotalJobs != 1 || statusResp.PendingJobs != 1 {
		t.Fatalf("expected 1 total & 1 pending job, got %+v", statusResp)
	}

	// 2. Test GET /cluster/nodes
	reqNodes := httptest.NewRequest(http.MethodGet, "/cluster/nodes", nil)
	rrNodes := httptest.NewRecorder()
	handler.Nodes(rrNodes, reqNodes)

	if rrNodes.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK for /cluster/nodes, got %d", rrNodes.Code)
	}

	var nodesResp []workermanager.NodeSnapshot
	if err := json.Unmarshal(rrNodes.Body.Bytes(), &nodesResp); err != nil {
		t.Fatalf("failed to unmarshal nodes response: %v", err)
	}

	// 3. Test DELETE /cluster/nodes/{id} (Node Eviction)
	reqEvict := httptest.NewRequest(http.MethodDelete, "/cluster/nodes/101", nil)
	rrEvict := httptest.NewRecorder()
	handler.NodeRoute(rrEvict, reqEvict)

	if rrEvict.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK for node eviction, got %d", rrEvict.Code)
	}

	var evictResp map[string]any
	if err := json.Unmarshal(rrEvict.Body.Bytes(), &evictResp); err != nil {
		t.Fatalf("failed to unmarshal eviction response: %v", err)
	}

	if evictResp["evicted"] != true || evictResp["node_id"] != float64(101) {
		t.Fatalf("unexpected eviction response: %+v", evictResp)
	}
}
