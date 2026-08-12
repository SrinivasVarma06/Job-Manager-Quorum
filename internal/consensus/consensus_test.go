package consensus_test

import (
	"bytes"
	"encoding/json"
	"io"
	"path/filepath"
	"testing"
	"time"

	"quorum/internal/consensus"
	"quorum/internal/cron"
	"quorum/internal/job"
	"quorum/internal/store"
)

func TestRaftNodeSingleNodeCluster(t *testing.T) {
	tempDir := t.TempDir()

	memoryStore := store.NewMemoryStore()
	fsm := consensus.NewFSM(memoryStore)

	raftAddr := "127.0.0.1:18088"
	dataDir := filepath.Join(tempDir, "raft")

	rn, err := consensus.NewRaftNode("node1", raftAddr, fsm, dataDir)
	if err != nil {
		t.Fatalf("failed to create RaftNode: %v", err)
	}
	defer rn.Close()

	// Wait for single-node cluster to elect itself leader via LeaderCh
	leaderCh := rn.LeaderCh()
	select {
	case isLeader := <-leaderCh:
		if !isLeader && !rn.IsLeader() {
			t.Fatalf("expected node1 to become leader")
		}
	case <-time.After(3 * time.Second):
		if !rn.IsLeader() {
			t.Fatalf("timed out waiting for single-node Raft cluster to elect node1 as leader")
		}
	}

	// Propose adding a job through Raft
	j := job.NewJob(1, "email", 5)
	if err := rn.ProposeAddJob(j); err != nil {
		t.Fatalf("failed to propose AddJob: %v", err)
	}

	// Wait briefly for FSM apply
	time.Sleep(50 * time.Millisecond)

	// Verify job was committed by Raft and applied to FSM store
	retrieved, ok := memoryStore.Get(1)
	if !ok || retrieved.Type != "email" {
		t.Fatalf("expected job 1 applied to store via Raft FSM, got ok=%v, job=%+v", ok, retrieved)
	}

	// Propose updating job status through Raft
	j.Status = job.Completed
	if err := rn.ProposeUpdateJob(j); err != nil {
		t.Fatalf("failed to propose UpdateJob: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	updated, ok := memoryStore.Get(1)
	if !ok || updated.Status != job.Completed {
		t.Fatalf("expected job 1 status Completed, got %+v", updated)
	}
}

func TestRaftDeleteJob(t *testing.T) {
	tempDir := t.TempDir()

	store := store.NewMemoryStore()
	fsm := consensus.NewFSM(store)

	rn, err := consensus.NewRaftNode(
		"node1",
		"127.0.0.1:18089",
		fsm,
		filepath.Join(tempDir, "raft"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rn.Close()

	for !rn.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	j := job.NewJob(1, "email", 5)

	if err := rn.ProposeAddJob(j); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	if err := rn.ProposeDeleteJob(1); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	_, ok := store.Get(1)

	if ok {
		t.Fatal("job should have been deleted")
	}
}

func TestRaftCancelJob(t *testing.T) {
	tempDir := t.TempDir()

	store := store.NewMemoryStore()
	fsm := consensus.NewFSM(store)

	rn, err := consensus.NewRaftNode(
		"node1",
		"127.0.0.1:18090",
		fsm,
		filepath.Join(tempDir, "raft"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rn.Close()

	for !rn.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	j := job.NewJob(1, "email", 5)

	if err := rn.ProposeAddJob(j); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	if err := rn.ProposeCancelJob(1); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	cancelled, ok := store.Get(1)

	if !ok {
		t.Fatal("job not found")
	}

	if cancelled.Status != job.Cancelled {
		t.Fatalf("expected cancelled, got %v", cancelled.Status)
	}
}

func TestFSMSnapshot(t *testing.T) {
	store := store.NewMemoryStore()

	j1 := job.NewJob(1, "email", 5)
	j2 := job.NewJob(2, "video", 10)

	_ = store.Add(j1)
	_ = store.Add(j2)

	fsm := consensus.NewFSM(store)

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	if snapshot == nil {
		t.Fatal("snapshot is nil")
	}
}

func TestFSMRestore(t *testing.T) {
	store := store.NewMemoryStore()

	fsm := consensus.NewFSM(store)

	jobs := []job.Job{
		job.NewJob(1, "email", 1),
		job.NewJob(2, "video", 2),
	}

	data, _ := json.Marshal(jobs)

	err := fsm.Restore(io.NopCloser(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}

	if len(store.List()) != 2 {
		t.Fatalf("expected 2 jobs restored")
	}
}

func TestRaftAddCron(t *testing.T) {
	tempDir := t.TempDir()

	store := store.NewMemoryStore()
	fsm := consensus.NewFSM(store)

	rn, err := consensus.NewRaftNode(
		"node1",
		"127.0.0.1:18091",
		fsm,
		filepath.Join(tempDir, "raft"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rn.Close()

	for !rn.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	c := cron.CronJob{
		ID:       "daily",
		Schedule: "@every 1m",
		Type:     "email",
		Priority: 1,
	}

	if err := rn.ProposeAddCron(c); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	cronJobs, err := store.ListCrons()
	if err != nil {
		t.Fatal(err)
	}

	if len(cronJobs) != 1 {
		t.Fatalf("expected 1 cron job")
	}
}

func TestRaftDeleteCron(t *testing.T) {
	tempDir := t.TempDir()

	store := store.NewMemoryStore()
	fsm := consensus.NewFSM(store)

	rn, err := consensus.NewRaftNode(
		"node1",
		"127.0.0.1:18092",
		fsm,
		filepath.Join(tempDir, "raft"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rn.Close()

	for !rn.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	c := cron.CronJob{
		ID:       "daily",
		Schedule: "@every 1m",
		Type:     "email",
		Priority: 1,
	}

	_ = rn.ProposeAddCron(c)

	time.Sleep(100 * time.Millisecond)

	_ = rn.ProposeDeleteCron("daily")

	time.Sleep(100 * time.Millisecond)

	cronJobs, err := store.ListCrons()
	if err != nil {
		t.Fatal(err)
	}

	if len(cronJobs) != 0 {
		t.Fatal("cron should be deleted")
	}
}
