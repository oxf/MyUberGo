package apiclient

import (
	"context"
	"fmt"
	"net/http"

	contracts "github.com/oxf/MyUber/contracts/http"
)

type AuthClient struct {
	baseClient
}

func NewAuthClient(baseURL string) *AuthClient {
	return &AuthClient{newBaseClient(baseURL)}
}

func (c *AuthClient) Signup(ctx context.Context, req contracts.SignupRequest) (contracts.SignupResponse, error) {
	var resp contracts.SignupResponse
	err := c.doJSON(ctx, http.MethodPost, "/signup", nil, req, &resp)
	return resp, err
}

func (c *AuthClient) Login(ctx context.Context, req contracts.LoginRequest) (contracts.LoginResponse, error) {
	var resp contracts.LoginResponse
	err := c.doJSON(ctx, http.MethodPost, "/login", nil, req, &resp)
	return resp, err
}

func (c *AuthClient) Refresh(ctx context.Context, req contracts.RefreshRequest) (contracts.RefreshResponse, error) {
	var resp contracts.RefreshResponse
	err := c.doJSON(ctx, http.MethodPost, "/refresh", nil, req, &resp)
	return resp, err
}

func (c *AuthClient) ListUsers(ctx context.Context, page, pageSize int) (contracts.PagedResponse[contracts.UserDto], error) {
	var resp contracts.PagedResponse[contracts.UserDto]
	path := fmt.Sprintf("/users?page=%d&pageSize=%d", page, pageSize)
	err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &resp)
	return resp, err
}
