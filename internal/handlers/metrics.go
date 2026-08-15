package handlers

import (
	"net/http"
	"quorum/internal/engine"
	"quorum/internal/metrics"
)

// MetricsHandler exposes system telemetry in standard Prometheus text format at GET /metrics.
func MetricsHandler(e *engine.Engine) http.HandlerFunc {
	// e is unused for the Prometheus handler — metrics are registered globally
	return func(w http.ResponseWriter, r *http.Request) {
		metrics.Handler().ServeHTTP(w, r)
	}
}
