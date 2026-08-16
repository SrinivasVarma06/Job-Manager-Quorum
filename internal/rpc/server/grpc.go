package server

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	workerpb "quorum/internal/rpc/proto"
)

// StartGRPCServer registers the given WorkerServiceServer implementation and
// begins serving on the specified port. Both WorkerServer (control node) and
// ExecutionServer (worker node) satisfy this interface.
func StartGRPCServer(
	port int,
	srv workerpb.WorkerServiceServer,
) error {
	address := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	workerpb.RegisterWorkerServiceServer(grpcServer, srv)
	fmt.Printf("gRPC server listening on %s\n", address)
	return grpcServer.Serve(listener)
}
