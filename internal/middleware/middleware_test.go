package middleware_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"quorum/internal/middleware"
)

func TestRequestIDGeneratesID(t *testing.T) {
	handler := middleware.RequestID(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			id := middleware.RequestIDFromContext(r.Context())
			if id == "" {
				t.Fatal("expected request id in context")
			}
		}),
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	headerID := rr.Header().Get("X-Request-ID")
	if headerID == "" {
		t.Fatal("expected X-Request-ID header")
	}
}

func TestRequestIDReusesExistingHeader(t *testing.T) {
	handler := middleware.RequestID(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			id := middleware.RequestIDFromContext(r.Context())

			if id != "abc123" {
				t.Fatalf("expected abc123, got %s", id)
			}
		}),
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", "abc123")

	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Header().Get("X-Request-ID") != "abc123" {
		t.Fatal("expected existing request id to be preserved")
	}
}

func TestRequestIDFromContextMissing(t *testing.T) {
	id := middleware.RequestIDFromContext(context.Background())

	if id != "" {
		t.Fatalf("expected empty string, got %s", id)
	}
}
