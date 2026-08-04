package server

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	workerpb "quorum/internal/rpc/proto"
)

func StartGRPCServer(
	port int,
	workerServer *WorkerServer,
) error {
	address := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	workerpb.RegisterWorkerServiceServer(
		grpcServer,
		workerServer,
	)
	fmt.Printf("gRPC server listening on %s\n", address)
	return grpcServer.Serve(listener)
}