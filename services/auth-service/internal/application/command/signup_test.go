package command

import (
	"context"
	"testing"

	"auth-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/http"
)

type fakeUserRepo struct {
	domain.UserRepository
	created *domain.User
}

func (f *fakeUserRepo) CreateUser(ctx context.Context, u *domain.User) (string, error) {
	f.created = u
	return "user-1", nil
}

type fakeHasher struct{}

func (fakeHasher) Hash(plain string) (string, error) { return "hashed:" + plain, nil }
func (fakeHasher) Compare(hash, plain string) error  { return nil }

func TestSignup_HashesPasswordAndCreatesUser(t *testing.T) {
	repo := &fakeUserRepo{}
	h := &SignupHandler{repo: repo, hasher: fakeHasher{}}

	result, err := h.Handle(context.Background(), Signup{
		Email: "a@b.com", Password: "plain-pw", Name: "Alice", Phone: "+123", Role: contracts.RoleClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.UserID != "user-1" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if repo.created == nil || repo.created.PasswordHash != "hashed:plain-pw" {
		t.Fatalf("user not created with hashed password: %+v", repo.created)
	}
}
