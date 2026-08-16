package auth_test

import (
	"testing"
	"time"

	"quorum/internal/auth"
)

const testSecret = "test-jwt-secret-key-1234567890"

func TestGenerateAndVerifyToken(t *testing.T) {
	token, err := auth.GenerateToken(testSecret, "user-42", auth.RoleSubmitter, time.Hour)
	if err != nil {
		t.Fatalf("GenerateToken failed: %v", err)
	}

	claims, err := auth.VerifyToken(testSecret, token)
	if err != nil {
		t.Fatalf("VerifyToken failed: %v", err)
	}

	if claims.UserID != "user-42" {
		t.Errorf("expected user_id user-42, got %s", claims.UserID)
	}
	if claims.Role != auth.RoleSubmitter {
		t.Errorf("expected role submitter, got %s", claims.Role)
	}
}

func TestVerifyToken_Expired(t *testing.T) {
	token, err := auth.GenerateToken(testSecret, "user-1", auth.RoleViewer, -1*time.Minute)
	if err != nil {
		t.Fatalf("GenerateToken failed: %v", err)
	}

	_, err = auth.VerifyToken(testSecret, token)
	if err != auth.ErrExpiredToken {
		t.Fatalf("expected ErrExpiredToken, got %v", err)
	}
}

func TestVerifyToken_InvalidSignature(t *testing.T) {
	token, err := auth.GenerateToken("wrong-secret-key", "user-1", auth.RoleAdmin, time.Hour)
	if err != nil {
		t.Fatalf("GenerateToken failed: %v", err)
	}

	_, err = auth.VerifyToken(testSecret, token)
	if err != auth.ErrInvalidToken {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerifyToken_Malformed(t *testing.T) {
	cases := []string{
		"",
		"invalid",
		"header.payload",
		"header.payload.signature.extra",
		"not_base64.not_base64.not_base64",
	}

	for _, token := range cases {
		_, err := auth.VerifyToken(testSecret, token)
		if err == nil {
			t.Errorf("expected error for malformed token %q, got nil", token)
		}
	}
}

func TestVerifyToken_InvalidRole(t *testing.T) {
	_, err := auth.GenerateToken(testSecret, "user-1", "superhero", time.Hour)
	if err == nil {
		t.Fatal("expected error generating token with invalid role, got nil")
	}
}

func TestRoleHierarchy(t *testing.T) {
	// Admin satisfies everything
	if !auth.RoleSatisfies(auth.RoleAdmin, auth.RoleAdmin) {
		t.Error("admin should satisfy admin")
	}
	if !auth.RoleSatisfies(auth.RoleAdmin, auth.RoleSubmitter) {
		t.Error("admin should satisfy submitter")
	}
	if !auth.RoleSatisfies(auth.RoleAdmin, auth.RoleViewer) {
		t.Error("admin should satisfy viewer")
	}

	// Submitter satisfies submitter & viewer (when listed)
	if !auth.RoleSatisfies(auth.RoleSubmitter, auth.RoleSubmitter) {
		t.Error("submitter should satisfy submitter")
	}
	if auth.RoleSatisfies(auth.RoleSubmitter, auth.RoleAdmin) {
		t.Error("submitter should NOT satisfy admin")
	}
	if !auth.RoleSatisfies(auth.RoleSubmitter, auth.RoleSubmitter, auth.RoleAdmin) {
		t.Error("submitter should satisfy (submitter or admin)")
	}

	// Viewer
	if !auth.RoleSatisfies(auth.RoleViewer, auth.RoleViewer) {
		t.Error("viewer should satisfy viewer")
	}
	if auth.RoleSatisfies(auth.RoleViewer, auth.RoleSubmitter) {
		t.Error("viewer should NOT satisfy submitter")
	}
	if auth.RoleSatisfies(auth.RoleViewer, auth.RoleAdmin) {
		t.Error("viewer should NOT satisfy admin")
	}
}
