package main

import (
	"embed"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"os"

	"quorum/internal/auth"
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

	// Authorizer middleware configured with the cluster JWT secret
	authz := middleware.NewAuthorizer(e.Config.JWTSecret)

	// Route wrappers enforcing RBAC when AuthEnabled is true
	requireRoles := func(allowedRoles ...string) func(http.Handler) http.Handler {
		if !e.Config.AuthEnabled {
			return func(next http.Handler) http.Handler { return next }
		}
		return authz.RequireRoles(allowedRoles...)
	}
	requireSubmitter := func(h http.Handler) http.Handler {
		return requireRoles(auth.RoleSubmitter, auth.RoleAdmin)(h)
	}
	requireViewer := func(h http.Handler) http.Handler {
		return requireRoles(auth.RoleViewer, auth.RoleSubmitter, auth.RoleAdmin)(h)
	}
	requireAdmin := func(h http.Handler) http.Handler {
		return requireRoles(auth.RoleAdmin)(h)
	}

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

	// Job Handlers (Submitter / Admin for write/cancel, Viewer / Submitter / Admin for read)
	jobsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			requireSubmitter(handlers.SubmitJobHandler(e)).ServeHTTP(w, r)
		case http.MethodGet:
			requireViewer(handlers.ListJobsHandler(e)).ServeHTTP(w, r)
		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.Handle("/jobs", jobsHandler)
	mux.Handle("GET /jobs/{id}", requireViewer(handlers.GetJobHandler(e)))
	mux.Handle("DELETE /jobs/{id}", requireSubmitter(handlers.CancelJobHandler(e)))

	// Cron Handlers (Admin for create/delete, Viewer for list)
	cronHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			requireAdmin(handlers.CreateCronJobHandler(e)).ServeHTTP(w, r)
		case http.MethodGet:
			requireViewer(handlers.ListCronJobsHandler(e)).ServeHTTP(w, r)
		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.Handle("/cron", cronHandler)
	mux.Handle("DELETE /cron/{id}", requireAdmin(handlers.DeleteCronJobHandler(e)))

	// Cluster & Telemetry Handlers
	clusterHandler := handlers.NewClusterHandler(e)
	mux.Handle("/cluster/status", requireViewer(http.HandlerFunc(clusterHandler.Status)))
	mux.Handle("/cluster/nodes", requireViewer(http.HandlerFunc(clusterHandler.Nodes)))
	mux.Handle("/cluster/nodes/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			requireAdmin(http.HandlerFunc(clusterHandler.NodeRoute)).ServeHTTP(w, r)
		} else {
			requireViewer(http.HandlerFunc(clusterHandler.NodeRoute)).ServeHTTP(w, r)
		}
	}))
	mux.Handle("/cluster/raft", requireViewer(http.HandlerFunc(clusterHandler.RaftStatus)))
	mux.Handle("/cluster/failover-simulate", requireAdmin(http.HandlerFunc(clusterHandler.FailoverSimulate)))
	mux.Handle("GET /jobs/leases", requireViewer(http.HandlerFunc(clusterHandler.Leases)))

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
