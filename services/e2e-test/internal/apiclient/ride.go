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

func (c *RideClient) ListRides(ctx context.Context, page, pageSize int) (contracts.PagedResponse[contracts.RideDto], error) {
	var resp contracts.PagedResponse[contracts.RideDto]
	path := fmt.Sprintf("/ride?page=%d&pageSize=%d", page, pageSize)
	err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &resp)
	return resp, err
}

// CancelRide takes the caller's user ID explicitly (rather than reusing the
// ride's own client) so callers can deep-verify the ownership check by
// cancelling as a different user.
func (c *RideClient) CancelRide(ctx context.Context, userID, rideID string, req contracts.CancelRideRequest) (contracts.CancelRideResponse, error) {
	var resp contracts.CancelRideResponse
	headers := map[string]string{"X-User-Id": userID}
	err := c.doJSON(ctx, http.MethodDelete, "/ride/"+rideID, headers, req, &resp)
	return resp, err
}

// StartRide and CompleteRide are driver-initiated: ride-service has no
// driver-authenticated HTTP mechanism (no X-User-Id equivalent), so the
// driverId is asserted in the body and validated by the handler against
// the ride's stored driver_id.
func (c *RideClient) StartRide(ctx context.Context, rideID, driverID string) (contracts.StartRideResponse, error) {
	var resp contracts.StartRideResponse
	err := c.doJSON(ctx, http.MethodPost, "/ride/"+rideID+"/start", nil, contracts.StartRideRequest{DriverId: driverID}, &resp)
	return resp, err
}

func (c *RideClient) CompleteRide(ctx context.Context, rideID, driverID string) (contracts.CompleteRideResponse, error) {
	var resp contracts.CompleteRideResponse
	err := c.doJSON(ctx, http.MethodPost, "/ride/"+rideID+"/complete", nil, contracts.CompleteRideRequest{DriverId: driverID}, &resp)
	return resp, err
}
