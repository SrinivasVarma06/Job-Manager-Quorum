package workermanager_test

import (
	"context"
	"time"
	"quorum/internal/job"
	"quorum/internal/worker"
	"quorum/internal/workermanager"
	"testing"
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

func TestWorkerManagerRegistry(t *testing.T) {
	wm := workermanager.NewManager()

	if wm.Count() != 0 {
		t.Fatalf("expected count 0, got %d", wm.Count())
	}

	w1 := &dummyClient{id: 101}
	w2 := &dummyClient{id: 102}

	wm.Register(w1, "localhost:50052", "email")
	wm.Register(w2, "localhost:50053", "video")

	if wm.Count() != 2 {
		t.Fatalf("expected count 2, got %d", wm.Count())
	}

	nodes := wm.Nodes()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}

	retrieved, ok := wm.Get(101)
	if !ok || retrieved.ID() != 101 {
		t.Fatalf("expected worker 101")
	}

	list := wm.List()
	if len(list) != 2 {
		t.Fatalf("expected list length 2, got %d", len(list))
	}

	wm.Remove(101)

	if wm.Count() != 1 {
		t.Fatalf("expected count 1 after removal, got %d", wm.Count())
	}

	_, ok = wm.Get(101)
	if ok {
		t.Fatal("expected worker 101 to be removed")
	}
}

func TestWorkerManagerHeartbeatTimeout(t *testing.T) {
	wm := workermanager.NewManager()

	w1 := &dummyClient{id: 101}
	wm.Register(w1, "localhost:50052", "*")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start monitor with very short timeout (50ms)
	go wm.Monitor(ctx, 50*time.Millisecond)

	// Sleep past timeout without sending heartbeats
	time.Sleep(200 * time.Millisecond)

	select {
	case deadID := <-wm.DeadWorkers:
		if deadID != 101 {
			t.Fatalf("expected dead worker ID 101, got %d", deadID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for DeadWorkers signal")
	}

	// Verify worker is marked not alive in list
	list := wm.List()
	if len(list) != 0 {
		t.Fatalf("expected active worker list length 0, got %d", len(list))
	}
}