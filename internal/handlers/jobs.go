package handlers

import (
	"encoding/json"
	"net/http"
	"quorum/internal/engine"
	"quorum/internal/job"
	"strconv"
	"strings"
	"time"
)

// SubmitJobRequest is the JSON body for POST /jobs.
type SubmitJobRequest struct {
	Type           string `json:"type"`
	Priority       int    `json:"priority"`
	RunAt          string `json:"run_at,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

func JobsHandler(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			SubmitJobHandler(e)(w, r)
		case http.MethodGet:
			ListJobsHandler(e)(w, r)

		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	}
}

// SubmitJobHandler handles POST /jobs.
//
// Idempotency behaviour:
//   - First request with a given idempotency_key → 201 Created, new job.
//   - Repeat request with the same key            → 200 OK, existing job (no duplicate created).
//   - Request without a key                       → 201 Created, new job each time.
func SubmitJobHandler(e *engine.Engine) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		var req SubmitJobRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
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

		// Scheduled jobs with an idempotency key are supported but deduplication
		// only covers the initial submission; the key is stored on the job.
		if strings.TrimSpace(req.RunAt) != "" {
			runAt, parseErr := time.Parse(time.RFC3339, req.RunAt)
			if parseErr != nil {
				http.Error(w, "run_at must be RFC3339", http.StatusBadRequest)
				return
			}
			if !runAt.After(time.Now()) {
				http.Error(w, "run_at must be in the future", http.StatusBadRequest)
				return
			}
			created, err := e.SubmitJobAtWithContext(r.Context(), req.Type, req.Priority, runAt)
			if err != nil {
				http.Error(w, "Failed to submit job", http.StatusInternalServerError)
				return
			}
			response := map[string]any{
				"id":     created.ID,
				"status": "scheduled",
				"run_at": created.NextRunAt.Format(time.RFC3339),
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(response)
			return
		}

		// Immediate job — may carry an idempotency key.
		created, wasDuplicate, err := e.SubmitJobIdempotent(
			r.Context(),
			req.IdempotencyKey,
			req.Type,
			req.Priority,
		)
		if err != nil {
			http.Error(w, "Failed to submit job", http.StatusInternalServerError)
			return
		}

		response := map[string]any{
			"id":     created.ID,
			"status": "submitted",
		}
		if created.IdempotencyKey != "" {
			response["idempotency_key"] = created.IdempotencyKey
		}

		w.Header().Set("Content-Type", "application/json")
		if wasDuplicate {
			// Duplicate: return 200 OK with the existing job, not 201.
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusCreated)
		}
		json.NewEncoder(w).Encode(response)
	}
}

func ListJobsHandler(e *engine.Engine) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Jobs())
	}
}

func GetJobHandler(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		id, err := strconv.Atoi(idStr)
		if err != nil {
			http.Error(w, "Invalid job ID", http.StatusBadRequest)
			return
		}
		j, ok := e.Job(id)
		if !ok {
			http.Error(w, "Job not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(j)
	}
}

// jobResponse builds the standard JSON representation of a job for HTTP responses.
func jobResponse(j job.Job) map[string]any {
	m := map[string]any{
		"id":       j.ID,
		"type":     j.Type,
		"priority": j.Priority,
		"status":   j.Status,
	}
	if j.IdempotencyKey != "" {
		m["idempotency_key"] = j.IdempotencyKey
	}
	if !j.NextRunAt.IsZero() {
		m["run_at"] = j.NextRunAt.Format(time.RFC3339)
	}
	return m
}
