package apiclient

import (
	"context"
	"fmt"
	"net/http"

	contracts "github.com/oxf/MyUber/contracts/http"
)

// RideClient calls ride-service through Kong. ride-service itself still
// doesn't validate JWTs — it trusts the X-User-Id header — but Kong now sits
// in front (see gateway/kong.yml): every route requires a valid bearer token,
// and Kong derives X-User-Id from that token's claims itself, overwriting
// whatever the caller sends. So identity here comes from the accessToken,
// not from a caller-supplied user ID.
type RideClient struct {
	baseClient
}

func NewRideClient(baseURL string) *RideClient {
	return &RideClient{newBaseClient(baseURL)}
}

func (c *RideClient) RequestRide(ctx context.Context, accessToken string, req contracts.CreateRideRequest) (contracts.CreateRideResponse, error) {
	var resp contracts.CreateRideResponse
	err := c.doJSON(ctx, http.MethodPost, "/request-ride", bearerHeader(accessToken), req, &resp)
	return resp, err
}

func (c *RideClient) GetRide(ctx context.Context, accessToken, id string) (contracts.RideDto, error) {
	var resp contracts.RideDto
	err := c.doJSON(ctx, http.MethodGet, "/ride/"+id, bearerHeader(accessToken), nil, &resp)
	return resp, err
}

func (c *RideClient) ListRides(ctx context.Context, accessToken string, page, pageSize int) (contracts.PagedResponse[contracts.RideDto], error) {
	var resp contracts.PagedResponse[contracts.RideDto]
	path := fmt.Sprintf("/ride?page=%d&pageSize=%d", page, pageSize)
	err := c.doJSON(ctx, http.MethodGet, path, bearerHeader(accessToken), nil, &resp)
	return resp, err
}

// CancelRide takes the caller's own access token (rather than reusing the
// ride's own client) so callers can deep-verify the ownership check by
// cancelling as a different, genuinely authenticated user — Kong won't let
// a spoofed X-User-Id through anymore, so "someone else" has to be a real
// second account with its own token.
func (c *RideClient) CancelRide(ctx context.Context, accessToken, rideID string, req contracts.CancelRideRequest) (contracts.CancelRideResponse, error) {
	var resp contracts.CancelRideResponse
	err := c.doJSON(ctx, http.MethodDelete, "/ride/"+rideID, bearerHeader(accessToken), req, &resp)
	return resp, err
}

// StartRide and CompleteRide are driver-initiated: ride-service has no
// driver-authenticated HTTP mechanism (no X-User-Id equivalent), so the
// driverId is asserted in the body and validated by the handler against
// the ride's stored driver_id. Kong still requires some valid bearer token
// to pass the gate — accessToken just needs to belong to any authenticated
// user, here the driver's own.
func (c *RideClient) StartRide(ctx context.Context, accessToken, rideID, driverID string) (contracts.StartRideResponse, error) {
	var resp contracts.StartRideResponse
	err := c.doJSON(ctx, http.MethodPost, "/ride/"+rideID+"/start", bearerHeader(accessToken), contracts.StartRideRequest{DriverId: driverID}, &resp)
	return resp, err
}

func (c *RideClient) CompleteRide(ctx context.Context, accessToken, rideID, driverID string) (contracts.CompleteRideResponse, error) {
	var resp contracts.CompleteRideResponse
	err := c.doJSON(ctx, http.MethodPost, "/ride/"+rideID+"/complete", bearerHeader(accessToken), contracts.CompleteRideRequest{DriverId: driverID}, &resp)
	return resp, err
}
