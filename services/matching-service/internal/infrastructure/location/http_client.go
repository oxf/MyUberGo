// Package location adapts services.LocationClient over HTTP — matching-service's first outbound
// (non-Kafka) call to another service. A caller must fall back to the rating-only pool on any error here.
package location

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"matching-service/internal/application/services"

	contracts "github.com/oxf/MyUber/contracts/http"
)

// HTTPClient is the services.LocationClient adapter over location-service's
// GET /internal/drivers/nearby.
type HTTPClient struct {
	baseURL string
	http    *http.Client
}

// NewHTTPClient takes an already-configured *http.Client (see common/httpclient) rather than
// building one itself, so the timeout/transport policy lives in one shared place.
func NewHTTPClient(baseURL string, httpClient *http.Client) *HTTPClient {
	return &HTTPClient{baseURL: baseURL, http: httpClient}
}

func (c *HTTPClient) Nearby(ctx context.Context, lat, lon, radiusKm float64, limit int) ([]services.NearbyDriver, error) {
	q := url.Values{}
	q.Set("lat", strconv.FormatFloat(lat, 'f', -1, 64))
	q.Set("lon", strconv.FormatFloat(lon, 'f', -1, 64))
	q.Set("radiusKm", strconv.FormatFloat(radiusKm, 'f', -1, 64))
	q.Set("limit", strconv.Itoa(limit))

	reqURL := c.baseURL + "/internal/drivers/nearby?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("location-service: nearby returned status %d", resp.StatusCode)
	}

	var out contracts.NearbyDriversResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	candidates := make([]services.NearbyDriver, 0, len(out.Candidates))
	for _, c := range out.Candidates {
		candidates = append(candidates, services.NearbyDriver{DriverID: c.DriverId, DistanceM: c.DistanceM})
	}
	return candidates, nil
}
