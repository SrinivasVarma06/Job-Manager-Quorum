package handlers

import (
	"encoding/json"
	"net/http"
	"quorum/internal/engine"
	"strconv"
)

func CancelJobHandler(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		idStr := r.PathValue("id")

		id, err := strconv.Atoi(idStr)
		if err != nil {
			http.Error(w, "invalid job id", http.StatusBadRequest)
			return
		}

		if err := e.CancelJob(id); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		json.NewEncoder(w).Encode(map[string]string{
			"status": "cancelled",
		})
	}
}
