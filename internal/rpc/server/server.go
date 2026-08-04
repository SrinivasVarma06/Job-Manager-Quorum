package server

import (
	"context"
	"fmt"

	"quorum/internal/job"
	workerpb "quorum/internal/rpc/proto"
	"quorum/internal/workermanager"
)

type WorkerServer struct {
	workerpb.UnimplementedWorkerServiceServer

	manager *workermanager.Manager
}

func NewWorkerServer(manager *workermanager.Manager) *WorkerServer {
	return &WorkerServer{
		manager: manager,
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

	select {
	case worker := <-s.manager.Available:
		err := worker.Submit(ctx, j)

		if err != nil {
			return &workerpb.SubmitJobResponse{
				Accepted: false,
				Error:    err.Error(),
			}, nil
		}

		return &workerpb.SubmitJobResponse{
			Accepted: true,
		}, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}