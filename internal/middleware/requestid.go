package middleware

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

type contextKey string

const RequestIDKey contextKey = "request_id"

// RequestID generates a random hex request ID, sets it as the X-Request-ID
// response header, and stores it in the request context so downstream handlers
// and the logging middleware can include it in structured log lines.
func RequestID(next http.Handler) http.Handler {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Re-use an existing request ID if the upstream proxy provides one.
		id := req.Header.Get("X-Request-ID")
		if id == "" {
			id = fmt.Sprintf("%016x", r.Int63())
		}

		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(req.Context(), RequestIDKey, id)
		next.ServeHTTP(w, req.WithContext(ctx))
	})
}

// RequestIDFromContext retrieves the request ID injected by the RequestID middleware.
func RequestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(RequestIDKey).(string); ok {
		return id
	}
	return ""
}
