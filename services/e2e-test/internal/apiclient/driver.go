package apiclient

import (
	"context"
	"fmt"
	"net/http"

	contracts "github.com/oxf/MyUber/contracts/http"
)

// DriverClient calls driver-service through Kong (see gateway/kong.yml) —
// every route requires a valid bearer token now. All GET endpoints return
// the camelCase contracts DTOs (list endpoints wrap them in PagedResponse).
type DriverClient struct {
	baseClient
}

func NewDriverClient(baseURL string) *DriverClient {
	return &DriverClient{newBaseClient(baseURL)}
}

func (c *DriverClient) CreateDriver(ctx context.Context, accessToken string, req contracts.CreateDriverDto) (contracts.CreateDriverResponse, error) {
	var resp contracts.CreateDriverResponse
	err := c.doJSON(ctx, http.MethodPost, "/driver", bearerHeader(accessToken), req, &resp)
	return resp, err
}

func (c *DriverClient) UpdateDriver(ctx context.Context, accessToken, id string, req contracts.UpdateDriverDto) error {
	return c.doJSON(ctx, http.MethodPut, "/driver/"+id, bearerHeader(accessToken), req, nil)
}

func (c *DriverClient) GetDriver(ctx context.Context, accessToken, id string) (contracts.DriverDto, error) {
	var resp contracts.DriverDto
	err := c.doJSON(ctx, http.MethodGet, "/driver/"+id, bearerHeader(accessToken), nil, &resp)
	return resp, err
}

func (c *DriverClient) CreateShift(ctx context.Context, accessToken string, req contracts.CreateShiftRequest) (contracts.CreateShiftResponse, error) {
	var resp contracts.CreateShiftResponse
	err := c.doJSON(ctx, http.MethodPost, "/driver-shift/create", bearerHeader(accessToken), req, &resp)
	return resp, err
}

func (c *DriverClient) UpdateShift(ctx context.Context, accessToken, id string, req contracts.UpdateShiftRequest) error {
	return c.doJSON(ctx, http.MethodPut, "/driver-shift/"+id, bearerHeader(accessToken), req, nil)
}

func (c *DriverClient) GetShift(ctx context.Context, accessToken, id string) (contracts.ShiftDto, error) {
	var resp contracts.ShiftDto
	err := c.doJSON(ctx, http.MethodGet, "/driver-shift/"+id, bearerHeader(accessToken), nil, &resp)
	return resp, err
}

// ListShifts hits GET /driver-shift, an Admin-only route now (see
// gateway/kong.yml) — accessToken must be an admin's, not the driver's own.
func (c *DriverClient) ListShifts(ctx context.Context, accessToken string, page, pageSize int) (contracts.PagedResponse[contracts.ShiftDto], error) {
	var resp contracts.PagedResponse[contracts.ShiftDto]
	path := fmt.Sprintf("/driver-shift?page=%d&pageSize=%d", page, pageSize)
	err := c.doJSON(ctx, http.MethodGet, path, bearerHeader(accessToken), nil, &resp)
	return resp, err
}
