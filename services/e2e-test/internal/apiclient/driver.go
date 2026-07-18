package apiclient

import (
	"context"
	"fmt"
	"net/http"

	contracts "github.com/oxf/MyUber/contracts/http"
)

// DriverClient calls driver-service. GET endpoints encode the service's
// domain entities (PascalCase keys like "DriverID"), not the contracts DTOs;
// decoding into the DTOs below still works because encoding/json matches
// field names case-insensitively.
type DriverClient struct {
	baseClient
}

func NewDriverClient(baseURL string) *DriverClient {
	return &DriverClient{newBaseClient(baseURL)}
}

func (c *DriverClient) CreateProfile(ctx context.Context, req contracts.CreateDriverProfileDto) (contracts.CreateDriverProfileResponse, error) {
	var resp contracts.CreateDriverProfileResponse
	err := c.doJSON(ctx, http.MethodPost, "/driver-profile", nil, req, &resp)
	return resp, err
}

func (c *DriverClient) UpdateProfile(ctx context.Context, id string, req contracts.UpdateDriverProfileDto) error {
	return c.doJSON(ctx, http.MethodPut, "/driver-profile/"+id, nil, req, nil)
}

func (c *DriverClient) GetProfile(ctx context.Context, id string) (contracts.DriverProfileDto, error) {
	var resp contracts.DriverProfileDto
	err := c.doJSON(ctx, http.MethodGet, "/driver-profile/"+id, nil, nil, &resp)
	return resp, err
}

func (c *DriverClient) CreateShift(ctx context.Context, req contracts.CreateShiftRequest) (contracts.CreateShiftResponse, error) {
	var resp contracts.CreateShiftResponse
	err := c.doJSON(ctx, http.MethodPost, "/driver-shift/create", nil, req, &resp)
	return resp, err
}

func (c *DriverClient) UpdateShift(ctx context.Context, id string, req contracts.UpdateShiftRequest) error {
	return c.doJSON(ctx, http.MethodPut, "/driver-shift/"+id, nil, req, nil)
}

func (c *DriverClient) GetShift(ctx context.Context, id string) (contracts.ShiftDto, error) {
	var resp contracts.ShiftDto
	err := c.doJSON(ctx, http.MethodGet, "/driver-shift/"+id, nil, nil, &resp)
	return resp, err
}

func (c *DriverClient) ListShifts(ctx context.Context, page, pageSize int) ([]contracts.ShiftDto, error) {
	var resp []contracts.ShiftDto
	path := fmt.Sprintf("/driver-shift?page=%d&pageSize=%d", page, pageSize)
	err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &resp)
	return resp, err
}
