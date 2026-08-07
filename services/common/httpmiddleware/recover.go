package httpmiddleware

import (
	"fmt"
	"net/http"

	"github.com/oxf/MyUber/common/reqctx"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Recover catches a panic in an inner handler, logs it, and returns a 500.
// Deliberately the innermost middleware, wrapped directly around mux by
// obshttp.Handler, so a live otelhttp span is in r.Context() here to attach
// the request ID and record the panic before recovering.
func Recover(logger *logrus.Entry) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			span := trace.SpanFromContext(r.Context())
			if id, ok := reqctx.RequestID(r.Context()); ok {
				span.SetAttributes(attribute.String("request_id", id))
			}
			defer func() {
				if rec := recover(); rec != nil {
					logger.WithField("panic", rec).Error("recovered from panic in HTTP handler")
					span.RecordError(fmt.Errorf("panic: %v", rec))
					span.SetStatus(codes.Error, "panic")
					http.Error(w, "internal server error", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
