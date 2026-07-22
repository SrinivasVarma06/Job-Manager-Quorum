package main

import (
	"log"
	"net/http"
	"quorum/internal/engine"
	"quorum/internal/handlers"
	"quorum/internal/middleware"
	"quorum/internal/queue"
	"quorum/internal/job"
	"container/heap"
	"fmt"
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

	pq := &queue.PriorityQueue{}
	heap.Init(pq)

	heap.Push(pq, job.NewJob(1, "low", 1))
	heap.Push(pq, job.NewJob(2, "high", 10))
	heap.Push(pq, job.NewJob(3, "medium", 5))

	for pq.Len() > 0 {
		j := heap.Pop(pq).(job.Job)
		fmt.Println(j.ID, j.Priority)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"service":"quorum","status":"running"}`))
	})
	mux.HandleFunc("/jobs",handlers.JobsHandler(e))
	mux.HandleFunc("GET /jobs/{id}", handlers.GetJobHandler(e))
	mux.HandleFunc("DELETE /jobs/{id}", handlers.CancelJobHandler(e))

	handler := middleware.RequestID(
		middleware.Logging(mux),
	)
	server := &http.Server{
		Addr: ":8080",
		Handler: handler,
	}

	log.Println("Quorum listening on :8080")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}