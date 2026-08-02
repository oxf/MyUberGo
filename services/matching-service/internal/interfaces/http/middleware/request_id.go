package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"matching-service/internal/common/reqctx"
	"net/http"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const requestIDHeader = "X-Request-Id"

var traceContextPropagator = propagation.TraceContext{}

// RequestID reads X-Request-Id from the incoming request if present,
// otherwise prefers the trace ID from an incoming W3C traceparent header so
// X-Request-Id and the request's eventual trace_id are the same value, and
// only falls back to a random ID if neither is present. Echoes the chosen
// ID back on the response and stores it on the request context for
// handlers/decorators to attach to their logs.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			extracted := traceContextPropagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			if sc := trace.SpanContextFromContext(extracted); sc.IsValid() {
				id = sc.TraceID().String()
			}
		}
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
