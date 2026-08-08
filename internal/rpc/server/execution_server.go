package server

import (
	"context"
	"log/slog"

	"quorum/internal/job"
	workerpb "quorum/internal/rpc/proto"
	"quorum/internal/runner"
)

// ExecutionServer implements the WorkerService on the worker node.
type ExecutionServer struct {
	workerpb.UnimplementedWorkerServiceServer
	runner *runner.Runner
}

func NewExecutionServer(r *runner.Runner) *ExecutionServer {
	return &ExecutionServer{runner: r}
}

func (s *ExecutionServer) SubmitJob(
	ctx context.Context,
	req *workerpb.SubmitJobRequest,
) (*workerpb.SubmitJobResponse, error) {
	j := job.Job{
		ID:       int(req.Id),
		Type:     req.Type,
		Priority: int(req.Priority),
	}

	slog.Info("Job received", "job_id", j.ID, "type", j.Type, "worker_id", req.WorkerId, "attempt", req.Attempt)
	go s.runner.Execute(int(req.WorkerId), j)

	return &workerpb.SubmitJobResponse{Accepted: true}, nil
}
