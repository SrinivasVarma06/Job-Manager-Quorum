package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"quorum/internal/events"
	"quorum/internal/job"
	rpcclient "quorum/internal/rpc/client"
	workerpb "quorum/internal/rpc/proto"
	"quorum/internal/rpc/proxy"
	"quorum/internal/workermanager"
)

// WorkerServer implements the WorkerService gRPC service on the control node.
//
// RPCs it handles:
//   - RegisterWorker: called by a worker node on start-up. The control node
//     dials back to the worker's address, creates a RemoteWorker proxy, and
//     registers it with the WorkerManager so the scheduler can dispatch to it.
//   - Heartbeat: called periodically by each worker node to signal liveness.
//   - SubmitJob: NOT called by the worker; called by the scheduler via the
//     RemoteWorker proxy to push a job to the worker node.
//   - ReportResult: called by the worker node after executing a job. The result
//     is forwarded to the scheduler's result channel, and the worker is re-queued.
//
// Note: WorkerServer is intentionally shared by both the control node and the
// worker node in the current proto layout. A future PR may split into
// ControllerService / ExecutionService when the protocol grows (Phase 8+).
type WorkerServer struct {
	workerpb.UnimplementedWorkerServiceServer
	manager *workermanager.Manager
	results chan<- job.Result
}

func NewWorkerServer(
	manager *workermanager.Manager,
	results chan<- job.Result,
) *WorkerServer {
	return &WorkerServer{
		manager: manager,
		results: results,
	}
}

// RegisterWorker is called by the worker node at startup.
// The control node dials the worker's address, wraps it in a RemoteWorker
// proxy, and registers it so the scheduler can dispatch jobs to it.
func (s *WorkerServer) RegisterWorker(
	ctx context.Context,
	req *workerpb.RegisterWorkerRequest,
) (*workerpb.RegisterWorkerResponse, error) {
	c, err := rpcclient.NewOutbound(int(req.WorkerId), req.Address)
	if err != nil {
		slog.Error("Failed to dial worker", "worker_id", req.WorkerId, "address", req.Address, "error", err)
		return &workerpb.RegisterWorkerResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to dial worker %d at %s: %v", req.WorkerId, req.Address, err),
		}, nil
	}

	remote := proxy.NewRemoteWorker(int(req.WorkerId), c)
	s.manager.Register(remote, req.Address, req.Topics...)
	s.manager.MakeAvailable(remote)

	events.Global().Broadcast(events.Event{
		Type:      events.EventWorkerRegistered,
		Message:   fmt.Sprintf("Worker-%d registered at %s (topics: %v)", req.WorkerId, req.Address, req.Topics),
		Timestamp: time.Now(),
	})

	slog.Info("Worker registered", "worker_id", req.WorkerId, "address", req.Address, "topics", req.Topics)
	return &workerpb.RegisterWorkerResponse{Success: true}, nil
}

// Heartbeat is called periodically by each worker node.
func (s *WorkerServer) Heartbeat(
	ctx context.Context,
	req *workerpb.HeartbeatRequest,
) (*workerpb.HeartbeatResponse, error) {
	s.manager.Heartbeat(int(req.WorkerId))
	slog.Debug("Heartbeat received", "worker_id", req.WorkerId)
	return &workerpb.HeartbeatResponse{Success: true}, nil
}

// SubmitJob is not used on the control node. The control node sends SubmitJob
// to worker nodes via RemoteWorker.Submit. This RPC exists only to satisfy the
// generated interface; receiving it here is a routing error.
func (s *WorkerServer) SubmitJob(
	ctx context.Context,
	req *workerpb.SubmitJobRequest,
) (*workerpb.SubmitJobResponse, error) {
	return nil, errors.New("SubmitJob must not be called on the control node WorkerServer")
}

// ReportResult is called by the worker node after it finishes executing a job.
// The result is forwarded to the scheduler's result channel, and the remote
// worker is re-queued into Available so the scheduler can dispatch the next job.
func (s *WorkerServer) ReportResult(
	ctx context.Context,
	req *workerpb.ReportResultRequest,
) (*workerpb.ReportResultResponse, error) {
	result := job.Result{
		JobID:   int(req.JobId),
		Success: req.Success,
		Attempt: int(req.Attempt),
	}
	if !req.Success {
		result.Error = errors.New(req.Error)
	}

	select {
	case s.results <- result:
	case <-ctx.Done():
		return &workerpb.ReportResultResponse{Acknowledged: false}, ctx.Err()
	}

	// Re-queue the remote worker so the scheduler can dispatch the next job.
	// This mirrors the self-re-queuing behaviour of local Worker.Start.
	if w, ok := s.manager.Get(int(req.WorkerId)); ok {
		s.manager.MakeAvailable(w)
	}

	slog.Debug("Result reported", "job_id", req.JobId, "worker_id", req.WorkerId, "success", req.Success, "attempt", req.Attempt)
	return &workerpb.ReportResultResponse{Acknowledged: true}, nil
}