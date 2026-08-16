// Package httpclient is the shared *http.Client for internal service-to-service HTTP calls,
// giving a bounded timeout + otelhttp instrumentation. No retry/circuit-breaker: fallback is caller-specific.
package httpclient

import (
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const (
	// DefaultTimeout bounds the whole request (dial + write + read), used
	// when New is called with timeout <= 0.
	DefaultTimeout             = 2 * time.Second
	defaultMaxIdleConns        = 100
	defaultMaxIdleConnsPerHost = 10
	defaultIdleConnTimeout     = 90 * time.Second
)

// New builds an *http.Client with a request timeout and an
// otelhttp-instrumented, connection-pooled transport.
func New(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	base := &http.Transport{
		MaxIdleConns:        defaultMaxIdleConns,
		MaxIdleConnsPerHost: defaultMaxIdleConnsPerHost,
		IdleConnTimeout:     defaultIdleConnTimeout,
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: otelhttp.NewTransport(base),
	}
}
