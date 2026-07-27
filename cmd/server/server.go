package main

import (
	"log"
	"net/http"
	"quorum/internal/engine"
	"quorum/internal/handlers"
	"quorum/internal/middleware"
)

func main() {

	e, err := engine.New()
	if err != nil {
		log.Fatal(err)
	}

	if err := e.Restore(); err != nil {
		log.Fatal(err)
	}

	e.Start()
	defer e.Stop()

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

	handler := middleware.RequestID(
		middleware.Logging(mux),
	)
	server := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	log.Println("Quorum listening on :8080")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
