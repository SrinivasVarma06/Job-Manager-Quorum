package main

import (
	"embed"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"os"

	"quorum/internal/config"
	"quorum/internal/engine"
	"quorum/internal/events"
	"quorum/internal/handlers"
	"quorum/internal/middleware"
	rpcserver "quorum/internal/rpc/server"
	"quorum/internal/tracing"
)

//go:embed web/*
var webFiles embed.FS

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if shutdown, err := tracing.Init("quorum-server"); err == nil {
		defer shutdown()
	} else {
		slog.Warn("Tracing initialization failed, continuing with fallback tracer", "error", err)
	}

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

	// Serve UI Dashboard static files embedded from web/
	webSubFS, err := fs.Sub(webFiles, "web")
	if err == nil {
		fileServer := http.FileServer(http.FS(webSubFS))
		mux.Handle("/ui/", http.StripPrefix("/ui/", fileServer))
		mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/ui/", http.StatusMovedPermanently)
		})
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/ui/", http.StatusFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"service":"quorum","status":"running"}`))
	})

	// Job Handlers
	mux.HandleFunc("/jobs", handlers.JobsHandler(e))
	mux.HandleFunc("GET /jobs/{id}", handlers.GetJobHandler(e))
	mux.HandleFunc("DELETE /jobs/{id}", handlers.CancelJobHandler(e))
	mux.HandleFunc("/cron", handlers.CronJobsHandler(e))
	mux.HandleFunc("DELETE /cron/{id}", handlers.DeleteCronJobHandler(e))

	// Cluster & Telemetry Handlers
	clusterHandler := handlers.NewClusterHandler(e)
	mux.HandleFunc("/cluster/status", clusterHandler.Status)
	mux.HandleFunc("/cluster/nodes", clusterHandler.Nodes)
	mux.HandleFunc("/cluster/nodes/", clusterHandler.NodeRoute)
	mux.HandleFunc("/cluster/raft", clusterHandler.RaftStatus)
	mux.HandleFunc("/cluster/failover-simulate", clusterHandler.FailoverSimulate)
	mux.HandleFunc("GET /jobs/leases", clusterHandler.Leases)

	// Observability & SSE Handlers
	mux.HandleFunc("/metrics", handlers.MetricsHandler(e))
	mux.HandleFunc("/events", events.Global().HTTPHandler())

	handler := middleware.RequestID(
		middleware.Tracing(
			middleware.Logging(mux),
		),
	)
	server := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	log.Println("Quorum control node listening on :8080 (UI available at http://localhost:8080/ui)")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
