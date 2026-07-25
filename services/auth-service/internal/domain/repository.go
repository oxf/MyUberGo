package domain

import "context"

type UserRepository interface {
	CreateUser(ctx context.Context, u *User) (string, error)
	GetByEmail(ctx context.Context, email string) (*User, error)
	GetByID(ctx context.Context, id string) (*User, error)
	GetUserList(ctx context.Context, req PageRequest) ([]*User, error)
	CountUsers(ctx context.Context) (int, error)
}

type RefreshTokenRepository interface {
	Store(ctx context.Context, t *RefreshToken) error
	// Exists reports whether token belongs to userID and is still valid
	// (expires_at > now()).
	Exists(ctx context.Context, userID, token string) (bool, error)
	Delete(ctx context.Context, userID, token string) error
}

type ClientRepository interface {
	Create(ctx context.Context, c *Client) (string, error)
	// GetByUserID returns ErrNotFound if the user has no client row (e.g.
	// the user is a Driver or Admin).
	GetByUserID(ctx context.Context, userID string) (*Client, error)
}

type AdminRepository interface {
	Create(ctx context.Context, a *Admin) (string, error)
}
