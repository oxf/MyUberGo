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

// ListUsers hits /users, an Admin-only route behind Kong (see
// gateway/kong.yml) — accessToken must be an admin's, not an ordinary
// client/driver's.
func (c *AuthClient) ListUsers(ctx context.Context, accessToken string, page, pageSize int) (contracts.PagedResponse[contracts.UserDto], error) {
	var resp contracts.PagedResponse[contracts.UserDto]
	path := fmt.Sprintf("/users?page=%d&pageSize=%d", page, pageSize)
	err := c.doJSON(ctx, http.MethodGet, path, bearerHeader(accessToken), nil, &resp)
	return resp, err
}

// Me hits /me, the caller's own profile derived from the bearer token's own
// claims (see gateway/kong.yml's inject_user_headers post-function).
func (c *AuthClient) Me(ctx context.Context, accessToken string) (contracts.UserDto, error) {
	var resp contracts.UserDto
	err := c.doJSON(ctx, http.MethodGet, "/me", bearerHeader(accessToken), nil, &resp)
	return resp, err
}

// Logout revokes one refresh token, scoped by the bearer token's own
// X-User-Id claim.
func (c *AuthClient) Logout(ctx context.Context, accessToken string, req contracts.LogoutRequest) error {
	return c.doJSON(ctx, http.MethodPost, "/logout", bearerHeader(accessToken), req, nil)
}
