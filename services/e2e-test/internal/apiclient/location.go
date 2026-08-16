package apiclient

import (
	"context"
	"net/http"

	contracts "github.com/oxf/MyUber/contracts/http"
)

// LocationClient calls location-service through Kong. GET /internal/drivers/nearby is
// network-isolated (matching-service only), so only the batch-ingest endpoint exists here.
type LocationClient struct {
	baseClient
}

func NewLocationClient(baseURL string) *LocationClient {
	return &LocationClient{baseClient: newBaseClient(baseURL)}
}

func (c *LocationClient) IngestBatch(ctx context.Context, accessToken string, in contracts.LocationBatchRequest) (contracts.LocationBatchResponse, error) {
	var out contracts.LocationBatchResponse
	err := c.doJSON(ctx, http.MethodPost, "/batch", bearerHeader(accessToken), in, &out)
	return out, err
}
