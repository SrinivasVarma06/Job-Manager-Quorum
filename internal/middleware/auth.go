package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"quorum/internal/auth"
)

type userContextKey string

const (
	UserClaimsKey userContextKey = "user_claims"
)

// UserFromContext returns the authenticated user's claims from the request context, if present.
func UserFromContext(ctx context.Context) *auth.Claims {
	if claims, ok := ctx.Value(UserClaimsKey).(*auth.Claims); ok {
		return claims
	}
	return nil
}

// UserIDFromContext returns the authenticated user's ID, or empty string.
func UserIDFromContext(ctx context.Context) string {
	if claims := UserFromContext(ctx); claims != nil {
		return claims.UserID
	}
	return ""
}

// RoleFromContext returns the authenticated user's role, or empty string.
func RoleFromContext(ctx context.Context) string {
	if claims := UserFromContext(ctx); claims != nil {
		return claims.Role
	}
	return ""
}

// Authorizer provides middleware functions bound to a specific JWT secret.
type Authorizer struct {
	secret string
}

// NewAuthorizer creates a new Authorizer with the given JWT secret.
func NewAuthorizer(secret string) *Authorizer {
	return &Authorizer{secret: secret}
}

// Authenticate verifies the Bearer token in the Authorization header and attaches
// user claims to the request context. Rejects invalid/missing/expired tokens with 401.
func (a *Authorizer) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			writeAuthError(w, http.StatusUnauthorized, "missing Authorization header")
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			writeAuthError(w, http.StatusUnauthorized, "invalid Authorization header format, expected 'Bearer <token>'")
			return
		}

		tokenStr := strings.TrimSpace(parts[1])
		claims, err := auth.VerifyToken(a.secret, tokenStr)
		if err != nil {
			switch err {
			case auth.ErrExpiredToken:
				writeAuthError(w, http.StatusUnauthorized, "token has expired")
			default:
				writeAuthError(w, http.StatusUnauthorized, "invalid or malformed token")
			}
			return
		}

		ctx := context.WithValue(r.Context(), UserClaimsKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireRoles returns a middleware that validates the JWT and ensures the user's
// role matches at least one of the required roles (or is admin).
func (a *Authorizer) RequireRoles(allowedRoles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return a.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims := UserFromContext(r.Context())
			if claims == nil || !auth.RoleSatisfies(claims.Role, allowedRoles...) {
				writeAuthError(w, http.StatusForbidden, "insufficient permissions for this resource")
				return
			}
			next.ServeHTTP(w, r)
		}))
	}
}

// RequireAdmin enforces that only users with the 'admin' role can access the endpoint.
func (a *Authorizer) RequireAdmin(next http.Handler) http.Handler {
	return a.RequireRoles(auth.RoleAdmin)(next)
}

// RequireSubmitter enforces that users with 'submitter' or 'admin' roles can access.
func (a *Authorizer) RequireSubmitter(next http.Handler) http.Handler {
	return a.RequireRoles(auth.RoleSubmitter, auth.RoleAdmin)(next)
}

// RequireViewer allows 'viewer', 'submitter', or 'admin' roles.
func (a *Authorizer) RequireViewer(next http.Handler) http.Handler {
	return a.RequireRoles(auth.RoleViewer, auth.RoleSubmitter, auth.RoleAdmin)(next)
}

func writeAuthError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":   http.StatusText(statusCode),
		"message": message,
	})
}
