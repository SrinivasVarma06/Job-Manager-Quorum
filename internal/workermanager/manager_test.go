package workermanager_test

import (
	"context"
	"quorum/internal/job"
	"quorum/internal/worker"
	"quorum/internal/workermanager"
	"testing"
	"time"
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

func TestManagerRegisterAndGet(t *testing.T) {
	m := workermanager.NewManager()

	w := &dummyClient{id: 101}

	m.Register(w, "localhost:9001", "email")

	if m.Count() != 1 {
		t.Fatalf("expected count 1, got %d", m.Count())
	}

	got, ok := m.Get(101)
	if !ok {
		t.Fatal("expected worker 101 to exist")
	}

	if got.ID() != 101 {
		t.Fatalf("expected worker 101, got %d", got.ID())
	}
}

func TestManagerRemove(t *testing.T) {
	m := workermanager.NewManager()

	w := &dummyClient{id: 101}
	m.Register(w, "localhost:9001", "email")

	m.Remove(101)

	if m.Count() != 0 {
		t.Fatalf("expected count 0, got %d", m.Count())
	}

	if _, ok := m.Get(101); ok {
		t.Fatal("expected worker removed")
	}
}

func TestManagerHeartbeat(t *testing.T) {
	m := workermanager.NewManager()

	w := &dummyClient{id: 101}
	m.Register(w, "localhost:9001", "email")

	before := m.Nodes()[0].LastHeartbeat

	time.Sleep(20 * time.Millisecond)

	m.Heartbeat(101)

	after := m.Nodes()[0].LastHeartbeat

	if !after.After(before) {
		t.Fatal("expected heartbeat timestamp to advance")
	}
}

func TestManagerMonitorDetectsDeadWorker(t *testing.T) {
	m := workermanager.NewManager()

	w := &dummyClient{id: 101}
	m.Register(w, "localhost:9001", "email")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go m.Monitor(ctx, 200*time.Millisecond)

	select {
	case deadID := <-m.DeadWorkers:
		if deadID != 101 {
			t.Fatalf("expected dead worker 101, got %d", deadID)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for dead worker detection")
	}
}

func TestManagerMonitorEmitsSingleDeadSignal(t *testing.T) {
	m := workermanager.NewManager()

	w := &dummyClient{id: 101}
	m.Register(w, "localhost:9001", "email")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go m.Monitor(ctx, 200*time.Millisecond)

	var first int

	select {
	case first = <-m.DeadWorkers:
	case <-time.After(2 * time.Second):
		t.Fatal("expected dead worker signal")
	}

	if first != 101 {
		t.Fatalf("expected 101, got %d", first)
	}

	time.Sleep(500 * time.Millisecond)

	select {
	case id := <-m.DeadWorkers:
		t.Fatalf("unexpected duplicate dead worker signal %d", id)

	default:
		// success
	}
}

func TestManagerListAliveWorkersOnly(t *testing.T) {
	m := workermanager.NewManager()

	w1 := &dummyClient{id: 101}
	w2 := &dummyClient{id: 102}

	m.Register(w1, "localhost:9001", "email")
	m.Register(w2, "localhost:9002", "video")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go m.Monitor(ctx, 200*time.Millisecond)

	select {
	case <-m.DeadWorkers:
	case <-time.After(2 * time.Second):
		t.Fatal("worker timeout not detected")
	}

	alive := m.List()

	if len(alive) != 0 {
		t.Fatalf("expected 0 alive workers, got %d", len(alive))
	}
}

func TestManagerNodesSnapshot(t *testing.T) {
	m := workermanager.NewManager()

	w := &dummyClient{id: 101}

	m.Register(
		w,
		"10.0.0.1:9000",
		"email",
		"sms",
	)

	nodes := m.Nodes()

	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}

	n := nodes[0]

	if n.ID != 101 {
		t.Fatalf("expected ID 101, got %d", n.ID)
	}

	if n.Address != "10.0.0.1:9000" {
		t.Fatalf("unexpected address %s", n.Address)
	}

	if len(n.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(n.Topics))
	}

	if !n.Alive {
		t.Fatal("expected worker alive")
	}
}

func TestManagerRegistersWorkerWithBroker(t *testing.T) {
	m := workermanager.NewManager()

	w := &dummyClient{id: 101}

	m.Register(
		w,
		"localhost:9001",
		"email",
	)

	if !m.Broker.CanHandle(101, "email") {
		t.Fatal("expected broker to know worker handles email")
	}

	if m.Broker.CanHandle(101, "video_processing") {
		t.Fatal("worker should not handle video_processing")
	}
}
