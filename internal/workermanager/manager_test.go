package workermanager_test

import (
	"context"
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

	w1 := &dummyClient{id: 1}
	w2 := &dummyClient{id: 2}

	wm.Register(w1)
	wm.Register(w2)

	if wm.Count() != 2 {
		t.Fatalf("expected count 2, got %d", wm.Count())
	}

	retrieved, ok := wm.Get(1)
	if !ok || retrieved.ID() != 1 {
		t.Fatalf("expected worker 1")
	}

	list := wm.List()
	if len(list) != 2 {
		t.Fatalf("expected list length 2, got %d", len(list))
	}

	wm.Remove(1)

	if wm.Count() != 1 {
		t.Fatalf("expected count 1 after removal, got %d", wm.Count())
	}

	_, ok = wm.Get(1)
	if ok {
		t.Fatal("expected worker 1 to be removed")
	}
}