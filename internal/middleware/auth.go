package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/umurinzi/backend/internal/domain"
)

// ── Context keys ──────────────────────────────────────────────────────────────
// Exported so every package in the module reads auth context values using the
// same key type and value — prevents the "different type, always nil" bug.

// ContextKey is the exported type used for all auth-related context values.
// Using a named type prevents collisions with keys from other packages.
type ContextKey string

const (
	// CtxUserID is the context key for the authenticated user's UUID.
	CtxUserID ContextKey = "user_id"
	// CtxUserRole is the context key for the authenticated user's role string.
	CtxUserRole ContextKey = "role"
)

type Claims struct {
	UserID string      `json:"uid"`
	Role   domain.Role `json:"role"`
	jwt.RegisteredClaims
}

// JWTMiddleware validates the Authorization: Bearer <token> header and injects
// the parsed claims into the request context under the exported CtxUserID and
// CtxUserRole keys. Requests without a valid token receive a 401 and are not
// forwarded to downstream handlers.
func JWTMiddleware(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw := r.Header.Get("Authorization")
			if raw == "" || !strings.HasPrefix(raw, "Bearer ") {
				writeUnauthorized(w, "missing or malformed authorization header")
				return
			}
			tokenStr := strings.TrimPrefix(raw, "Bearer ")

			claims := &Claims{}
			token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
				if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
				}
				return []byte(secret), nil
			})
			if err != nil || !token.Valid {
				writeUnauthorized(w, "invalid or expired token")
				return
			}
			ctx := context.WithValue(r.Context(), CtxUserID, claims.UserID)
			ctx = context.WithValue(ctx, CtxUserRole, string(claims.Role))

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ── RequestLogger ─────────────────────────────────────────────────────────────

// RequestLogger logs every inbound HTTP request with method, path, status, and latency.
func RequestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)
			log.Info("http request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rw.status),
				slog.Duration("latency", time.Since(start)),
				slog.String("remote_addr", r.RemoteAddr),
			)
		})
	}
}

// ── Recovery ──────────────────────────────────────────────────────────────────

// Recovery catches panics in downstream handlers, logs a structured error, and
// returns HTTP 500. This prevents a single handler panic from crashing the process.
func Recovery(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("handler panic recovered",
						slog.Any("panic", rec),
						slog.String("method", r.Method),
						slog.String("path", r.URL.Path),
					)
					http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func writeUnauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// statusRecorder wraps http.ResponseWriter to capture the written status code.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
