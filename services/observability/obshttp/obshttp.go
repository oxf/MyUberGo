// Package obshttp wraps an http.Handler with OTel server instrumentation.
package obshttp

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Handler wraps next with otelhttp server instrumentation. Span names start
// as "service operation" and are re-evaluated after routing once Go 1.22
// ServeMux sets r.Pattern (see otelhttp's handler.go), so every service's
// `mux.HandleFunc("GET /driver/{id}", ...)`-style patterns already in this
// repo produce bounded span names like "GET /driver/{id}" — never one
// distinct name per driver ID.
//
// Health-check polling (docker-compose HEALTHCHECK hits /health/ready every
// 10s on all 5 services) is filtered out so it doesn't spam every trace
// backend with a span every few seconds forever.
func Handler(next http.Handler, service string) http.Handler {
	return otelhttp.NewHandler(next, service,
		otelhttp.WithFilter(func(r *http.Request) bool {
			return !strings.HasPrefix(r.URL.Path, "/health/")
		}),
	)
}
