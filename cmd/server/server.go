package main

import (
	"log"
	"log/slog"
	"net/http"
	"os"

	"quorum/internal/config"
	"quorum/internal/engine"
	"quorum/internal/handlers"
	"quorum/internal/middleware"
	rpcserver "quorum/internal/rpc/server"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg := config.Default()
	e, err := engine.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err := e.Restore(); err != nil {
		log.Fatal(err)
	}

	e.Start()
	defer e.Stop()

	// WorkerServer is the control node's gRPC service.
	// It handles worker registration, heartbeats, and result reporting.
	// When a worker calls RegisterWorker, the control node dials back to the
	// worker's address and registers a RemoteWorker in the WorkerManager so
	// the scheduler can dispatch jobs to it.
	grpcWorker := rpcserver.NewWorkerServer(
		e.WorkerManager,
		e.Scheduler.Results,
	)

	go func() {
		if err := rpcserver.StartGRPCServer(e.Config.ControllerGRPCPort, grpcWorker); err != nil {
			log.Fatal(err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"service":"quorum","status":"running"}`))
	})
	mux.HandleFunc("/jobs", handlers.JobsHandler(e))
	mux.HandleFunc("GET /jobs/{id}", handlers.GetJobHandler(e))
	mux.HandleFunc("DELETE /jobs/{id}", handlers.CancelJobHandler(e))
	mux.HandleFunc("/cron", handlers.CronJobsHandler(e))
	mux.HandleFunc("DELETE /cron/{id}", handlers.DeleteCronJobHandler(e))

	clusterHandler := handlers.NewClusterHandler(e)
	mux.HandleFunc("/cluster/status", clusterHandler.Status)
	mux.HandleFunc("/cluster/nodes", clusterHandler.Nodes)
	mux.HandleFunc("/cluster/nodes/", clusterHandler.NodeRoute)

	handler := middleware.RequestID(
		middleware.Logging(mux),
	)
	server := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	log.Println("Quorum control node listening on :8080")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
