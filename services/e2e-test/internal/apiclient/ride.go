package apiclient

import (
	"context"
	"fmt"
	"net/http"

	contracts "github.com/oxf/MyUber/contracts/http"
)

// RideClient calls ride-service. ride-service does not validate JWTs — it
// trusts the X-User-Id header (the target design puts validation in the
// gateway), so RequestRide takes the raw user ID.
type RideClient struct {
	baseClient
}

func NewRideClient(baseURL string) *RideClient {
	return &RideClient{newBaseClient(baseURL)}
}

func (c *RideClient) RequestRide(ctx context.Context, userID string, req contracts.CreateRideRequest) (contracts.CreateRideResponse, error) {
	var resp contracts.CreateRideResponse
	headers := map[string]string{"X-User-Id": userID}
	err := c.doJSON(ctx, http.MethodPost, "/request-ride", headers, req, &resp)
	return resp, err
}

func (c *RideClient) GetRide(ctx context.Context, id string) (contracts.RideDto, error) {
	var resp contracts.RideDto
	err := c.doJSON(ctx, http.MethodGet, "/ride/"+id, nil, nil, &resp)
	return resp, err
}

func (c *RideClient) ListRides(ctx context.Context, page, pageSize int) ([]contracts.RideDto, error) {
	var resp []contracts.RideDto
	path := fmt.Sprintf("/ride?page=%d&pageSize=%d", page, pageSize)
	err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &resp)
	return resp, err
}
