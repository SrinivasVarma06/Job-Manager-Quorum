package proxy

import (
	"context"

	"quorum/internal/job"
	rpcclient "quorum/internal/rpc/client"
)

type RemoteWorker struct {
	id     int
	client *rpcclient.Client
}

func NewRemoteWorker(
	id int,
	client *rpcclient.Client,
) *RemoteWorker {

	return &RemoteWorker{
		id:     id,
		client: client,
	}
}

func (r *RemoteWorker) ID() int {
	return r.id
}

func (r *RemoteWorker) Start(ctx context.Context) {
	// Remote worker already runs on another machine.
	// Nothing to start here.
}

func (r *RemoteWorker) Submit(
	ctx context.Context,
	j job.Job,
) error {
	return r.client.Submit(ctx, j)
}