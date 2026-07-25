package domain

import (
	"testing"

	contracts "github.com/oxf/MyUber/contracts/http"
)

func TestNewUser_Valid(t *testing.T) {
	u, err := NewUser("a@b.com", "hashed", "Alice", "+123", contracts.RoleClient)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.Email != "a@b.com" || u.PasswordHash != "hashed" || u.Role != string(contracts.RoleClient) {
		t.Fatalf("unexpected user: %+v", u)
	}
}

func TestNewUser_RejectsEmptyEmail(t *testing.T) {
	if _, err := NewUser("", "hashed", "Alice", "+123", contracts.RoleClient); err == nil {
		t.Fatal("expected error for empty email")
	}
}

func TestNewUser_RejectsEmptyPasswordHash(t *testing.T) {
	if _, err := NewUser("a@b.com", "", "Alice", "+123", contracts.RoleClient); err == nil {
		t.Fatal("expected error for empty password hash")
	}
}

func TestNewUser_RejectsInvalidRole(t *testing.T) {
	if _, err := NewUser("a@b.com", "hashed", "Alice", "+123", contracts.UserRole("Admin")); err == nil {
		t.Fatal("expected error for invalid role")
	}
}
