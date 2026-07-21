package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"matching-service/internal/common/reqctx"
	"net/http"
)

const requestIDHeader = "X-Request-Id"

// RequestID reads X-Request-Id from the incoming request if present,
// otherwise generates one, echoes it back on the response, and stores it on
// the request context for handlers/decorators to attach to their logs.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			id = generateID()
		}

		w.Header().Set(requestIDHeader, id)
		ctx := reqctx.WithRequestID(r.Context(), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func generateID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b)
}
