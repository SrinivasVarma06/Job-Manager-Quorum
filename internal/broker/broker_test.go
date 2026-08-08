package broker_test

import (
	"context"
	"quorum/internal/broker"
	"quorum/internal/job"
	"quorum/internal/worker"
	"testing"
)

type dummyWorker struct {
	id int
}

func (d *dummyWorker) ID() int                                 { return d.id }
func (d *dummyWorker) Start(ctx context.Context)               {}
func (d *dummyWorker) Submit(ctx context.Context, j job.Job) error { return nil }

var _ worker.WorkerClient = (*dummyWorker)(nil)

func TestBrokerTopicMatching(t *testing.T) {
	b := broker.New()

	// Worker 101 subscribes to "email" and "sms"
	b.RegisterWorker(101, []string{"email", "sms"})

	// Worker 102 subscribes to "video_processing"
	b.RegisterWorker(102, []string{"video_processing"})

	// Worker 103 subscribes to all ("*")
	b.RegisterWorker(103, []string{"*"})

	// Verify Worker 101
	if !b.CanHandle(101, "email") {
		t.Fatal("expected worker 101 to handle email")
	}
	if !b.CanHandle(101, "sms") {
		t.Fatal("expected worker 101 to handle sms")
	}
	if b.CanHandle(101, "video_processing") {
		t.Fatal("expected worker 101 NOT to handle video_processing")
	}

	// Verify Worker 102
	if !b.CanHandle(102, "video_processing") {
		t.Fatal("expected worker 102 to handle video_processing")
	}
	if b.CanHandle(102, "email") {
		t.Fatal("expected worker 102 NOT to handle email")
	}

	// Verify Worker 103 (wildcard)
	if !b.CanHandle(103, "email") || !b.CanHandle(103, "video_processing") || !b.CanHandle(103, "anything") {
		t.Fatal("expected worker 103 (wildcard) to handle any job type")
	}
}

func TestBrokerSelectWorkerTopicRouting(t *testing.T) {
	b := broker.New()

	w101 := &dummyWorker{id: 101}
	w102 := &dummyWorker{id: 102}

	b.RegisterWorker(101, []string{"email"})
	b.RegisterWorker(102, []string{"video_processing"})

	availableChan := make(chan worker.WorkerClient, 10)
	availableChan <- w101
	availableChan <- w102

	// Select worker for "video_processing" -> Should skip w101 and pick w102
	selected, ok := b.SelectWorker("video_processing", availableChan)
	if !ok || selected.ID() != 102 {
		t.Fatalf("expected worker 102 selected for video_processing, got ok=%v, id=%v", ok, selected)
	}

	// w101 should still be in availableChan
	if len(availableChan) != 1 {
		t.Fatalf("expected 1 worker left in availableChan, got %d", len(availableChan))
	}

	wLeft := <-availableChan
	if wLeft.ID() != 101 {
		t.Fatalf("expected remaining worker in channel to be 101, got %d", wLeft.ID())
	}
}
