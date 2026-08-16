package middleware

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"quorum/internal/tracing"
)

// Tracing starts an "http.request" span for each incoming request and captures
// request metadata such as method, path, user agent, route, and request_id.
func Tracing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tracer := tracing.Tracer()
		ctx, span := tracer.Start(r.Context(), "http.request")

		route := r.Pattern
		if route == "" {
			route = r.URL.Path
		}

		span.SetAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.path", r.URL.Path),
			attribute.String("http.route", route),
			attribute.String("http.user_agent", r.UserAgent()),
		)

		if requestID := RequestIDFromContext(ctx); requestID != "" {
			span.SetAttributes(attribute.String("request.id", requestID))
		}

		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		req := r.WithContext(ctx)

		defer func() {
			status := rw.statusCode
			span.SetAttributes(attribute.Int("http.status_code", status))
			if status >= 500 {
				span.SetStatus(codes.Error, "server_error")
			}
			span.End()
		}()

		next.ServeHTTP(rw, req)
	})
}
