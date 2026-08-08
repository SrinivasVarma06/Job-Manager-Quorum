package consensus_test

import (
	"path/filepath"
	"testing"
	"time"

	"quorum/internal/consensus"
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
