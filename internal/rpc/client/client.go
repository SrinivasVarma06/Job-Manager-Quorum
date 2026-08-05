package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"
	"quorum/internal/job"
	workerpb "quorum/internal/rpc/proto"
)

type Client struct {
	id     int
	conn   *grpc.ClientConn
	client workerpb.WorkerServiceClient
}

func New(id int, address string) (*Client, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		id:     id,
		conn:   conn,
		client: workerpb.NewWorkerServiceClient(conn),
	}, nil
}

func (c *Client) ID() int {
	return c.id
}

func (c *Client) Start(ctx context.Context) {
	_, err := c.client.RegisterWorker(
		ctx,
		&workerpb.RegisterWorkerRequest{
			WorkerId: int32(c.id),
			Address:  "localhost:50051",
		},
	)
	if err != nil {
		fmt.Println("failed to register worker:", err)
		return
	}
	fmt.Printf("Worker %d registered successfully\n", c.id)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := c.client.Heartbeat(
				ctx,
				&workerpb.HeartbeatRequest{
					WorkerId: int32(c.id),
				},
			)
			if err != nil {
				fmt.Println("heartbeat failed:", err)
			}
		}
	}
}

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

func (c *Client) Close() error {
	return c.conn.Close()
}