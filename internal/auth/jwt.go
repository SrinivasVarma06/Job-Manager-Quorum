package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	RoleAdmin     = "admin"
	RoleSubmitter = "submitter"
	RoleViewer    = "viewer"
)

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("token has expired")
	ErrInvalidRole  = errors.New("invalid role")
	ErrEmptySecret  = errors.New("secret cannot be empty")
)

// Claims represents the JWT payload containing user identity, role, and expiry.
type Claims struct {
	UserID    string `json:"user_id"`
	Role      string `json:"role"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
}

type header struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// IsValidRole checks if the given role string is one of the recognized roles.
func IsValidRole(role string) bool {
	switch role {
	case RoleAdmin, RoleSubmitter, RoleViewer:
		return true
	default:
		return false
	}
}

// RoleSatisfies returns true if the user's role satisfies at least one of the allowed roles.
// Hierarchy: admin satisfies all roles.
func RoleSatisfies(userRole string, allowedRoles ...string) bool {
	if userRole == RoleAdmin {
		return true
	}
	for _, allowed := range allowedRoles {
		if userRole == allowed {
			return true
		}
	}
	return false
}

// GenerateToken creates a signed HS256 JWT string with user_id, role, and expiration.
func GenerateToken(secret, userID, role string, ttl time.Duration) (string, error) {
	if secret == "" {
		return "", ErrEmptySecret
	}
	if !IsValidRole(role) {
		return "", fmt.Errorf("%w: %q", ErrInvalidRole, role)
	}

	h := header{
		Alg: "HS256",
		Typ: "JWT",
	}

	headerJSON, err := json.Marshal(h)
	if err != nil {
		return "", err
	}

	now := time.Now()
	claims := Claims{
		UserID:    userID,
		Role:      role,
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(ttl).Unix(),
	}

	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)

	signingInput := headerB64 + "." + claimsB64
	sig := signHS256(signingInput, []byte(secret))
	sigB64 := base64.RawURLEncoding.EncodeToString(sig)

	return signingInput + "." + sigB64, nil
}

// VerifyToken decodes, validates the signature, and checks the expiration of an HS256 JWT.
func VerifyToken(secret, tokenStr string) (*Claims, error) {
	if secret == "" {
		return nil, ErrEmptySecret
	}

	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidToken
	}

	headerB64, claimsB64, sigB64 := parts[0], parts[1], parts[2]

	expectedSig := signHS256(headerB64+"."+claimsB64, []byte(secret))
	actualSig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return nil, ErrInvalidToken
	}

	if !hmac.Equal(expectedSig, actualSig) {
		return nil, ErrInvalidToken
	}

	claimsBytes, err := base64.RawURLEncoding.DecodeString(claimsB64)
	if err != nil {
		return nil, ErrInvalidToken
	}

	var claims Claims
	if err := json.Unmarshal(claimsBytes, &claims); err != nil {
		return nil, ErrInvalidToken
	}

	if claims.ExpiresAt > 0 && time.Now().Unix() > claims.ExpiresAt {
		return nil, ErrExpiredToken
	}

	if !IsValidRole(claims.Role) {
		return nil, ErrInvalidRole
	}

	return &claims, nil
}

func signHS256(message string, key []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(message))
	return mac.Sum(nil)
}
