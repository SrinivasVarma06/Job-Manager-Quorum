package server

import (
	"context"
	"fmt"

	"quorum/internal/job"
	workerpb "quorum/internal/rpc/proto"
	"quorum/internal/runner"
	"quorum/internal/workermanager"
)

type WorkerServer struct {
	workerpb.UnimplementedWorkerServiceServer
	manager *workermanager.Manager
	runner *runner.Runner
}

func NewWorkerServer(
	manager *workermanager.Manager,
	runner *runner.Runner,
) *WorkerServer {
	return &WorkerServer{
		manager: manager,
		runner:  runner,
	}
}

func (s *WorkerServer) RegisterWorker(
	ctx context.Context,
	req *workerpb.RegisterWorkerRequest,
) (*workerpb.RegisterWorkerResponse, error) {
	fmt.Printf(
		"Worker %d registered from %s\n",
		req.WorkerId,
		req.Address,
	)
	return &workerpb.RegisterWorkerResponse{
		Success: true,
	}, nil
}

func (s *WorkerServer) Heartbeat(
	ctx context.Context,
	req *workerpb.HeartbeatRequest,
) (*workerpb.HeartbeatResponse, error) {
	s.manager.Heartbeat(int(req.WorkerId))
	fmt.Printf("Heartbeat received from Worker %d\n", req.WorkerId)
	return &workerpb.HeartbeatResponse{
		Success: true,
	}, nil
}

func (s *WorkerServer) SubmitJob(
	ctx context.Context,
	req *workerpb.SubmitJobRequest,
) (*workerpb.SubmitJobResponse, error) {
	j := job.Job{
		ID:       int(req.Id),
		Type:     req.Type,
		Priority: int(req.Priority),
	}
	s.runner.Execute(int(req.WorkerId), j)
	return &workerpb.SubmitJobResponse{
		Accepted: true,
	}, nil
}