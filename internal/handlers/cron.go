package handlers

import (
	"encoding/json"
	"net/http"
	"quorum/internal/engine"
	"strings"
)

type CreateCronJobRequest struct {
	ID       string `json:"id"`
	Schedule string `json:"schedule"`
	Type     string `json:"type"`
	Priority int    `json:"priority"`
}

func CronJobsHandler(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			CreateCronJobHandler(e)(w, r)
		case http.MethodGet:
			ListCronJobsHandler(e)(w, r)
		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	}
}

func CreateCronJobHandler(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateCronJobRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		req.ID = strings.TrimSpace(req.ID)
		req.Schedule = strings.TrimSpace(req.Schedule)
		req.Type = strings.TrimSpace(req.Type)

		if req.ID == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}
		if req.Schedule == "" {
			http.Error(w, "schedule is required", http.StatusBadRequest)
			return
		}
		if req.Type == "" {
			http.Error(w, "type is required", http.StatusBadRequest)
			return
		}
		if req.Priority < 0 {
			http.Error(w, "priority must be >= 0", http.StatusBadRequest)
			return
		}

		if err := e.AddCronJob(req.ID, req.Schedule, req.Type, req.Priority); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "created",
			"id":     req.ID,
		})
	}
}

func ListCronJobsHandler(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListCronJobs())
	}
}

func DeleteCronJobHandler(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("id"))
		if id == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}

		e.RemoveCronJob(id)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status": "removed",
			"id":     id,
		})
	}
}
