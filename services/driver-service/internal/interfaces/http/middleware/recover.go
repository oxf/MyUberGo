package middleware

import (
	"net/http"

	"github.com/sirupsen/logrus"
)

// Recover catches a panic in an inner handler, logs it, and returns a 500
// instead of letting the panic unwind past net/http (which would otherwise
// only abort the one connection, but with no structured error response or
// log entry marking what happened).
func Recover(logger *logrus.Entry) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.WithField("panic", rec).Error("recovered from panic in HTTP handler")
					http.Error(w, "internal server error", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
