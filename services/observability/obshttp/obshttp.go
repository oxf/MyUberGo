// Package obshttp wraps an http.Handler with OTel server instrumentation.
package obshttp

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Handler wraps next with otelhttp instrumentation (span names use the ServeMux
// pattern, e.g. "GET /driver/{id}"); /health/ and excludePrefixes are filtered out.
func Handler(next http.Handler, service string, excludePrefixes ...string) http.Handler {
	return otelhttp.NewHandler(next, service,
		otelhttp.WithFilter(func(r *http.Request) bool {
			if strings.HasPrefix(r.URL.Path, "/health/") {
				return false
			}
			for _, prefix := range excludePrefixes {
				if strings.HasPrefix(r.URL.Path, prefix) {
					return false
				}
			}
			return true
		}),
	)
}
