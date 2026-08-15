package handlers_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"quorum/internal/config"
	"quorum/internal/engine"
	"quorum/internal/handlers"
	"quorum/internal/job"
	"quorum/internal/worker"
	"quorum/internal/workermanager"
)

type dummyClient struct {
	id int
}

func (d *dummyClient) ID() int { return d.id }

func (d *dummyClient) Start(ctx context.Context) {}

func (d *dummyClient) Submit(ctx context.Context, j job.Job) error {
	return nil
}

var _ worker.WorkerClient = (*dummyClient)(nil)

func getFreeRaftAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("failed to close free-port listener: %v", err)
	}
	return addr
}

func waitForLeader(t *testing.T, e *engine.Engine) {
	t.Helper()
	if e.RaftNode == nil {
		t.Fatal("raft initialization failed")
	}

	deadline := time.Now().Add(5 * time.Second)

	for !e.RaftNode.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("raft leader election timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestClusterHandlerNodesAndStatus(t *testing.T) {
	defer func() {
		_ = os.Remove("jobs.log")
		_ = os.Remove("snapshot.json")
		_ = os.Remove("quorum.db")
	}()

	cfg := config.Default()
	tempDir := t.TempDir()
	cfg.RaftAddr = getFreeRaftAddr(t)
	cfg.RaftDataDir = filepath.Join(tempDir, "raft")

	e, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer e.Stop()

	waitForLeader(t, e)

	// Register a worker so node APIs have something to return
	w := &dummyClient{id: 101}
	e.WorkerManager.Register(w, "localhost:50052", "email")

	// Submit a job
	_, err = e.SubmitJob("email", 5)
	if err != nil {
		t.Fatalf("failed to submit job: %v", err)
	}

	handler := handlers.NewClusterHandler(e)

	t.Run("Status", func(t *testing.T) {
		req := httptest.NewRequest(
			http.MethodGet,
			"/cluster/status",
			nil,
		)

		rr := httptest.NewRecorder()

		handler.Status(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}

		var resp handlers.ClusterStatus

		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}

		if resp.TotalJobs < 1 {
			t.Fatalf("expected at least 1 job, got %d", resp.TotalJobs)
		}

		if resp.ActiveNodes != 1 {
			t.Fatalf("expected 1 active node, got %d", resp.ActiveNodes)
		}
	})

	t.Run("Nodes", func(t *testing.T) {
		req := httptest.NewRequest(
			http.MethodGet,
			"/cluster/nodes",
			nil,
		)

		rr := httptest.NewRecorder()

		handler.Nodes(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}

		var nodes []workermanager.NodeSnapshot

		if err := json.Unmarshal(rr.Body.Bytes(), &nodes); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}

		if len(nodes) != 1 {
			t.Fatalf("expected 1 node, got %d", len(nodes))
		}
	})

	t.Run("NodeEviction", func(t *testing.T) {
		req := httptest.NewRequest(
			http.MethodDelete,
			"/cluster/nodes/101",
			nil,
		)

		rr := httptest.NewRecorder()

		handler.NodeRoute(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}

		var resp map[string]any

		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}

		if resp["evicted"] != true {
			t.Fatalf("expected evicted=true, got %+v", resp)
		}

		if e.WorkerManager.Count() != 0 {
			t.Fatalf(
				"expected worker count 0 after eviction, got %d",
				e.WorkerManager.Count(),
			)
		}
	})
}
