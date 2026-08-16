package middleware_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"quorum/internal/auth"
	"quorum/internal/middleware"
)

const testSecret = "jwt-secret-test-key-for-middleware"

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims := middleware.UserFromContext(r.Context())
		userID := ""
		role := ""
		if claims != nil {
			userID = claims.UserID
			role = claims.Role
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"user_id": userID,
			"role":    role,
		})
	})
}

func TestAuthMiddleware_ValidToken(t *testing.T) {
	authz := middleware.NewAuthorizer(testSecret)
	handler := authz.Authenticate(okHandler())

	token, err := auth.GenerateToken(testSecret, "alice", auth.RoleSubmitter, time.Hour)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (body: %s)", rr.Code, rr.Body.String())
	}

	var resp map[string]string
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["user_id"] != "alice" || resp["role"] != "submitter" {
		t.Fatalf("unexpected context claims response: %+v", resp)
	}
}

func TestAuthMiddleware_MissingToken(t *testing.T) {
	authz := middleware.NewAuthorizer(testSecret)
	handler := authz.Authenticate(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for missing token, got %d", rr.Code)
	}
}

func TestAuthMiddleware_InvalidToken(t *testing.T) {
	authz := middleware.NewAuthorizer(testSecret)
	handler := authz.Authenticate(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	req.Header.Set("Authorization", "Bearer invalid-garbage-token")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for invalid token, got %d", rr.Code)
	}
}

func TestAuthMiddleware_ExpiredToken(t *testing.T) {
	authz := middleware.NewAuthorizer(testSecret)
	handler := authz.Authenticate(okHandler())

	token, _ := auth.GenerateToken(testSecret, "bob", auth.RoleViewer, -1*time.Minute)

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for expired token, got %d", rr.Code)
	}
}

func TestRBAC_RequireSubmitter_ForbiddenForViewer(t *testing.T) {
	authz := middleware.NewAuthorizer(testSecret)
	handler := authz.RequireSubmitter(okHandler())

	// Viewer tries to access submitter endpoint (e.g. POST /jobs)
	viewerToken, _ := auth.GenerateToken(testSecret, "viewer-1", auth.RoleViewer, time.Hour)

	req := httptest.NewRequest(http.MethodPost, "/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+viewerToken)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for viewer accessing submitter endpoint, got %d", rr.Code)
	}
}

func TestRBAC_RequireSubmitter_AllowedForSubmitterAndAdmin(t *testing.T) {
	authz := middleware.NewAuthorizer(testSecret)
	handler := authz.RequireSubmitter(okHandler())

	// Submitter
	submitterToken, _ := auth.GenerateToken(testSecret, "sub-1", auth.RoleSubmitter, time.Hour)
	req1 := httptest.NewRequest(http.MethodPost, "/jobs", nil)
	req1.Header.Set("Authorization", "Bearer "+submitterToken)
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected 200 for submitter, got %d", rr1.Code)
	}

	// Admin
	adminToken, _ := auth.GenerateToken(testSecret, "admin-1", auth.RoleAdmin, time.Hour)
	req2 := httptest.NewRequest(http.MethodPost, "/jobs", nil)
	req2.Header.Set("Authorization", "Bearer "+adminToken)
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200 for admin, got %d", rr2.Code)
	}
}

func TestRBAC_RequireAdmin_ForbiddenForSubmitter(t *testing.T) {
	authz := middleware.NewAuthorizer(testSecret)
	handler := authz.RequireAdmin(okHandler())

	submitterToken, _ := auth.GenerateToken(testSecret, "sub-1", auth.RoleSubmitter, time.Hour)
	req := httptest.NewRequest(http.MethodDelete, "/cluster/nodes/1", nil)
	req.Header.Set("Authorization", "Bearer "+submitterToken)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for submitter on admin endpoint, got %d", rr.Code)
	}
}
