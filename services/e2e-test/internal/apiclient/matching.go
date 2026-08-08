package apiclient

import (
	"context"
	"net/http"

	contracts "github.com/oxf/MyUber/contracts/http"
)

// MatchingClient calls matching-service through Kong. matching-service now
// checks the caller's X-User-Id (Kong-derived from the bearer token, not
// caller-supplied) against the driverId's cached owner — see
// matching-service's AcceptRideHandler/GetDriverOfferHandler.
type MatchingClient struct {
	baseClient
}

func NewMatchingClient(baseURL string) *MatchingClient {
	return &MatchingClient{baseClient: newBaseClient(baseURL)}
}

// GetDriverOffer returns the driver's current offer; a 404 *APIError means
// "no offer right now" and is an expected outcome, not a failure.
func (c *MatchingClient) GetDriverOffer(ctx context.Context, accessToken, driverID string) (*contracts.DriverOfferDto, error) {
	var out contracts.DriverOfferDto
	if err := c.doJSON(ctx, http.MethodGet, "/drivers/"+driverID+"/offer", bearerHeader(accessToken), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *MatchingClient) AcceptRide(ctx context.Context, accessToken, rideID string, in contracts.AcceptRideRequest) (*contracts.AcceptRideResponse, error) {
	var out contracts.AcceptRideResponse
	if err := c.doJSON(ctx, http.MethodPost, "/rides/"+rideID+"/accept", bearerHeader(accessToken), in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
