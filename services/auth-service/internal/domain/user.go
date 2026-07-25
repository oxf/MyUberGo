package domain

import (
	"errors"

	contracts "github.com/oxf/MyUber/contracts/http"
)

type User struct {
	ID           string
	Email        string
	PasswordHash string
	Name         string
	Phone        string
	Role         string
	CreatedAt    string // RFC3339
	UpdatedAt    string // RFC3339
	DeletedAt    *string
	// ClientID is only populated by GetUserByID (backing GET /me), never by
	// CreateUser/GetUserList — it's a convenience lookup, not stored on the
	// user row itself.
	ClientID *string
}

// NewUser validates signup input and builds a User ready for persistence.
// PasswordHash is expected to already be hashed by the caller (via the
// PasswordHasher port) — this factory only validates shape, not secrecy.
func NewUser(email, passwordHash, name, phone string, role contracts.UserRole) (*User, error) {
	if email == "" {
		return nil, errors.New("email is required")
	}
	if passwordHash == "" {
		return nil, errors.New("password is required")
	}
	if role != contracts.RoleClient && role != contracts.RoleDriver {
		return nil, errors.New("role must be Client or Driver")
	}
	return &User{
		Email:        email,
		PasswordHash: passwordHash,
		Name:         name,
		Phone:        phone,
		Role:         string(role),
	}, nil
}
