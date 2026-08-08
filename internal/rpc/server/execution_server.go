package server

import (
	"context"
	"log/slog"

	workerpb "quorum/internal/rpc/proto"
	"quorum/internal/runner"
	"quorum/internal/job"
)

// ExecutionServer implements the WorkerService on the worker node.
//
// The worker node only handles SubmitJob — it never handles RegisterWorker,
// Heartbeat, or ReportResult. Those RPCs run on the control node's WorkerServer.
// Both services share one proto definition; unused RPCs return an appropriate
// error to make any accidental misrouting obvious.
type ExecutionServer struct {
	workerpb.UnimplementedWorkerServiceServer
	runner *runner.Runner
}

func NewExecutionServer(r *runner.Runner) *ExecutionServer {
	return &ExecutionServer{runner: r}
}

// SubmitJob receives a job from the control node and executes it locally.
// Execution is asynchronous: the RPC returns immediately and the result is
// reported back via ReportResult on the control node once execution completes.
func (s *ExecutionServer) SubmitJob(
	ctx context.Context,
	req *workerpb.SubmitJobRequest,
) (*workerpb.SubmitJobResponse, error) {
	j := job.Job{
		ID:       int(req.Id),
		Type:     req.Type,
		Priority: int(req.Priority),
	}

	slog.Info("Job received", "job_id", j.ID, "type", j.Type, "worker_id", req.WorkerId)
	go s.runner.Execute(int(req.WorkerId), j)

	return &workerpb.SubmitJobResponse{Accepted: true}, nil
}
