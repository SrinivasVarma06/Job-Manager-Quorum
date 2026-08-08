package consensus

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"

	"github.com/hashicorp/raft"
	"quorum/internal/cron"
	"quorum/internal/job"
	"quorum/internal/store"
)

type CommandType string

const (
	CmdAddJob        CommandType = "add_job"
	CmdUpdateJob     CommandType = "update_job"
	CmdDeleteJob     CommandType = "delete_job"
	CmdCancelJob     CommandType = "cancel_job"
	CmdAddCronJob    CommandType = "add_cron_job"
	CmdDeleteCronJob CommandType = "delete_cron_job"
)

type Command struct {
	Type   CommandType  `json:"type"`
	Job    job.Job      `json:"job,omitempty"`
	ID     int          `json:"id,omitempty"`
	Cron   cron.CronJob `json:"cron,omitempty"`
	CronID string       `json:"cron_id,omitempty"`
}

// FSM implements the raft.FSM interface for Quorum.
//
// FSM writes EXCLUSIVELY to store.Store (desired state). Follower nodes do not
// maintain dispatch queues; queue reconstruction occurs strictly on the Raft Leader.
type FSM struct {
	store store.Store
}

func NewFSM(s store.Store) *FSM {
	return &FSM{store: s}
}

// Apply is called by Raft once a log entry is committed by a cluster quorum.
func (f *FSM) Apply(l *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		slog.Error("FSM unmarshal error", "error", err)
		return fmt.Errorf("unmarshal command: %w", err)
	}

	switch cmd.Type {
	case CmdAddJob:
		if err := f.store.Add(cmd.Job); err != nil {
			slog.Error("FSM failed AddJob", "job_id", cmd.Job.ID, "error", err)
			return err
		}
		slog.Debug("FSM applied AddJob", "job_id", cmd.Job.ID)

	case CmdUpdateJob:
		if err := f.store.Update(cmd.Job); err != nil {
			slog.Error("FSM failed UpdateJob", "job_id", cmd.Job.ID, "error", err)
			return err
		}
		slog.Debug("FSM applied UpdateJob", "job_id", cmd.Job.ID)

	case CmdDeleteJob:
		if _, err := f.store.Delete(cmd.ID); err != nil {
			slog.Error("FSM failed DeleteJob", "job_id", cmd.ID, "error", err)
			return err
		}
		slog.Debug("FSM applied DeleteJob", "job_id", cmd.ID)

	case CmdCancelJob:
		if err := f.store.Cancel(cmd.ID); err != nil {
			slog.Error("FSM failed CancelJob", "job_id", cmd.ID, "error", err)
			return err
		}
		slog.Debug("FSM applied CancelJob", "job_id", cmd.ID)

	case CmdAddCronJob:
		if err := f.store.AddCron(cmd.Cron); err != nil {
			slog.Error("FSM failed AddCronJob", "cron_id", cmd.Cron.ID, "error", err)
			return err
		}
		slog.Debug("FSM applied AddCronJob", "cron_id", cmd.Cron.ID)

	case CmdDeleteCronJob:
		if _, err := f.store.DeleteCron(cmd.CronID); err != nil {
			slog.Error("FSM failed DeleteCronJob", "cron_id", cmd.CronID, "error", err)
			return err
		}
		slog.Debug("FSM applied DeleteCronJob", "cron_id", cmd.CronID)

	default:
		slog.Warn("FSM unknown command type", "type", cmd.Type)
	}

	return nil
}

// Snapshot returns a snapshot of the current FSM state for Raft log compaction.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	jobs := f.store.List()
	return &fsmSnapshot{jobs: jobs}, nil
}

// Restore resets the FSM state from a Raft snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var jobs []job.Job
	if err := json.NewDecoder(rc).Decode(&jobs); err != nil {
		return fmt.Errorf("decode fsm snapshot: %w", err)
	}

	for _, j := range jobs {
		_ = f.store.Add(j)
	}

	slog.Info("FSM restored state from snapshot", "job_count", len(jobs))
	return nil
}

type fsmSnapshot struct {
	jobs []job.Job
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		data, err := json.Marshal(s.jobs)
		if err != nil {
			return err
		}
		if _, err := sink.Write(data); err != nil {
			return err
		}
		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
