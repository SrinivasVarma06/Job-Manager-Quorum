package consensus

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
	"quorum/internal/cron"
	"quorum/internal/job"
)

type RaftNode struct {
	raft   *raft.Raft
	fsm    *FSM
	nodeID string
}

// NewRaftNode initializes a Raft node instance.
func NewRaftNode(nodeID string, raftAddr string, fsm *FSM, dataDir string) (*RaftNode, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.HeartbeatTimeout = 1000 * time.Millisecond
	config.ElectionTimeout = 1000 * time.Millisecond

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create raft data dir %q: %w", dataDir, err)
	}

	addr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve raft addr %q: %w", raftAddr, err)
	}

	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create tcp transport: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	// Persistent Raft Log & Stable Store
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft instance: %w", err)
	}

	// Bootstrap a single-node cluster if no state exists
	hasState, err := raft.HasExistingState(logStore, stableStore, snapshots)
	if err != nil {
		return nil, err
	}

	if !hasState {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
		slog.Info("Bootstrapped Raft cluster", "node_id", nodeID, "addr", raftAddr)
	}

	return &RaftNode{
		raft:   r,
		fsm:    fsm,
		nodeID: nodeID,
	}, nil
}

// IsLeader returns true if this control node is the elected Raft leader.
func (rn *RaftNode) IsLeader() bool {
	return rn.raft.State() == raft.Leader
}

// LeaderTerm returns the current Raft term used for transient fencing tokens.
func (rn *RaftNode) LeaderTerm() uint64 {
	termStr := rn.raft.Stats()["term"]
	if termStr == "" {
		return 0
	}
	return parseTerm(termStr)
}

func parseTerm(s string) uint64 {
	var term uint64
	fmt.Sscanf(s, "%d", &term)
	return term
}

// LeaderAddr returns the current Raft leader address.
func (rn *RaftNode) LeaderAddr() string {
	addr, _ := rn.raft.LeaderWithID()
	return string(addr)
}

// LeaderCh returns a channel that receives notification when leadership changes.
func (rn *RaftNode) LeaderCh() <-chan bool {
	return rn.raft.LeaderCh()
}

// AddVoter adds a new voting node to the Raft cluster.
func (rn *RaftNode) AddVoter(id string, addr string) error {
	if !rn.IsLeader() {
		return fmt.Errorf("not leader, leader is %s", rn.LeaderAddr())
	}
	f := rn.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second)
	return f.Error()
}

// RemoveServer removes a node from the Raft cluster.
func (rn *RaftNode) RemoveServer(id string) error {
	if !rn.IsLeader() {
		return fmt.Errorf("not leader, leader is %s", rn.LeaderAddr())
	}
	f := rn.raft.RemoveServer(raft.ServerID(id), 0, 10*time.Second)
	return f.Error()
}

// Propose applies a command to the Raft cluster log.
func (rn *RaftNode) Propose(cmd Command) error {
	if !rn.IsLeader() {
		return fmt.Errorf("not leader, leader is %s", rn.LeaderAddr())
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	future := rn.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply error: %w", err)
	}

	if res := future.Response(); res != nil {
		if err, ok := res.(error); ok {
			return err
		}
	}

	return nil
}

func (rn *RaftNode) ProposeAddJob(j job.Job) error {
	return rn.Propose(Command{Type: CmdAddJob, Job: j})
}

func (rn *RaftNode) ProposeUpdateJob(j job.Job) error {
	return rn.Propose(Command{Type: CmdUpdateJob, Job: j})
}

func (rn *RaftNode) ProposeDeleteJob(id int) error {
	return rn.Propose(Command{Type: CmdDeleteJob, ID: id})
}

func (rn *RaftNode) ProposeCancelJob(id int) error {
	return rn.Propose(Command{Type: CmdCancelJob, ID: id})
}

func (rn *RaftNode) ProposeAddCron(c cron.CronJob) error {
	return rn.Propose(Command{Type: CmdAddCronJob, Cron: c})
}

func (rn *RaftNode) ProposeDeleteCron(id string) error {
	return rn.Propose(Command{Type: CmdDeleteCronJob, CronID: id})
}

func (rn *RaftNode) Close() error {
	future := rn.raft.Shutdown()
	return future.Error()
}
