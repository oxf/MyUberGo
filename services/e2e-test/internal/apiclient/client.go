package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// bearerHeader builds the Authorization header Kong's jwt plugin requires on
// every route except auth-service's public signup/login/refresh (see
// gateway/kong.yml). Kong derives X-User-Id/X-User-Email/X-User-Role from
// this token's claims and overwrites any of those headers a caller sets
// directly, so per-user identity now flows through the token, not headers.
func bearerHeader(accessToken string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + accessToken}
}

// APIError is returned for any non-2xx response so actors can log the
// status and body the service actually sent back.
type APIError struct {
	Status int
	Body   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("http %d: %s", e.Status, e.Body)
}

type baseClient struct {
	baseURL string
	http    *http.Client
}

// sharedTransport is used by every apiclient (Auth/Driver/Ride all target
// Kong on the same host - Matching targets a different host directly, but
// shares this transport too since there's no downside). Without an explicit
// Transport, http.Client falls back to http.DefaultTransport, whose
// MaxIdleConnsPerHost defaults to 2 - under this simulator's bursty
// concurrent-actor startup, that forces constant TCP connection churn to
// Kong instead of reuse. Headroom here is well above the default actor
// counts (5 clients/3 drivers) and scales with E2E_CLIENTS/E2E_DRIVERS.
var sharedTransport = &http.Transport{
	MaxIdleConns:        100,
	MaxIdleConnsPerHost: 50,
	IdleConnTimeout:     90 * time.Second,
}

func newBaseClient(baseURL string) baseClient {
	return baseClient{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 10 * time.Second, Transport: sharedTransport},
	}
}

// doJSON sends in (if non-nil) as a JSON body and decodes the response into
// out (if non-nil). Any non-2xx status returns an *APIError.
func (c baseClient) doJSON(
	ctx context.Context,
	method string,
	path string,
	headers map[string]string,
	in any,
	out any,
) error {

	var body io.Reader
	if in != nil {
		payload, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &APIError{Status: resp.StatusCode, Body: string(bytes.TrimSpace(raw))}
	}

	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("decode %s %s response: %w", method, path, err)
		}
	}

	return nil
}
