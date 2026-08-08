package client

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"quorum/internal/job"
	workerpb "quorum/internal/rpc/proto"
)

// Client is the gRPC client that a worker node uses to communicate with the
// control node. It handles registration, heartbeats, job submission
// (when acting as a WorkerClient proxy), and result reporting.
type Client struct {
	id             int
	workerAddr     string // address this worker listens on (sent during RegisterWorker)
	topics         []string
	conn           *grpc.ClientConn
	client         workerpb.WorkerServiceClient
}

// New creates a Client for a worker node that needs to dial the control node.
// id is the worker's unique ID.
// workerAddr is the address this worker's gRPC server is listening on;
// it is advertised to the control node during RegisterWorker.
// controllerAddr is the address of the control node's gRPC server.
// topics specifies the job types/topics this worker subscribes to.
func New(id int, workerAddr string, controllerAddr string, topics ...string) (*Client, error) {
	conn, err := grpc.NewClient(
		controllerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		id:         id,
		workerAddr: workerAddr,
		topics:     topics,
		conn:       conn,
		client:     workerpb.NewWorkerServiceClient(conn),
	}, nil
}

// NewOutbound creates a Client used by the control node to dial a remote worker.
// Unlike New, it does not need a workerAddr because the control node never
// calls RegisterWorker or Heartbeat through this connection.
func NewOutbound(id int, workerAddr string) (*Client, error) {
	conn, err := grpc.NewClient(
		workerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		id:         id,
		workerAddr: workerAddr,
		conn:       conn,
		client:     workerpb.NewWorkerServiceClient(conn),
	}, nil
}

func (c *Client) ID() int {
	return c.id
}

// Start registers this worker with the control node and then sends periodic
// heartbeats until ctx is cancelled.
func (c *Client) Start(ctx context.Context) {
	_, err := c.client.RegisterWorker(
		ctx,
		&workerpb.RegisterWorkerRequest{
			WorkerId: int32(c.id),
			Address:  c.workerAddr,
			Topics:   c.topics,
		},
	)
	if err != nil {
		slog.Error("Worker registration failed", "worker_id", c.id, "error", err)
		return
	}
	slog.Info("Worker registered", "worker_id", c.id, "addr", c.workerAddr, "topics", c.topics)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := c.client.Heartbeat(
				ctx,
				&workerpb.HeartbeatRequest{WorkerId: int32(c.id)},
			)
			if err != nil {
				slog.Warn("Heartbeat failed", "worker_id", c.id, "error", err)
			}
		}
	}
}

// Submit dispatches a job to the worker node this client represents.
// Used by the RemoteWorker proxy on the control node side.
func (c *Client) Submit(ctx context.Context, j job.Job) error {
	req := &workerpb.SubmitJobRequest{
		Id:       int64(j.ID),
		Type:     j.Type,
		Priority: int32(j.Priority),
		WorkerId: int32(c.id),
	}

	resp, err := c.client.SubmitJob(ctx, req)
	if err != nil {
		return err
	}

	if !resp.Accepted {
		return fmt.Errorf("%s", resp.Error)
	}

	return nil
}

// ReportResult sends the outcome of a completed job to the control node.
func (c *Client) ReportResult(ctx context.Context, jobID int, success bool, errMsg string) error {
	_, err := c.client.ReportResult(ctx, &workerpb.ReportResultRequest{
		JobId:    int64(jobID),
		Success:  success,
		Error:    errMsg,
		WorkerId: int32(c.id),
	})
	return err
}

func (c *Client) Close() error {
	return c.conn.Close()
}