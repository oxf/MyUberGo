package command

import (
	"context"
	"testing"

	"auth-service/internal/domain"
	"auth-service/internal/infrastructure/metrics"

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

type fakeClientRepo struct {
	domain.ClientRepository
	created *domain.Client
}

func (f *fakeClientRepo) Create(ctx context.Context, c *domain.Client) (string, error) {
	f.created = c
	return "client-1", nil
}

type fakeHasher struct{}

func (fakeHasher) Hash(plain string) (string, error) { return "hashed:" + plain, nil }
func (fakeHasher) Compare(hash, plain string) error  { return nil }

type fakeTransactionManager struct{}

func (fakeTransactionManager) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func TestSignup_HashesPasswordAndCreatesUser(t *testing.T) {
	repo := &fakeUserRepo{}
	clientRepo := &fakeClientRepo{}
	h := &SignupHandler{repo: repo, clientRepo: clientRepo, hasher: fakeHasher{}, transaction: fakeTransactionManager{}, metrics: metrics.NewNoopMetricsClient()}

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
	if clientRepo.created == nil || clientRepo.created.UserID != "user-1" {
		t.Fatalf("client row not created for user: %+v", clientRepo.created)
	}
}

func TestSignup_DriverGetsNoClientRow(t *testing.T) {
	repo := &fakeUserRepo{}
	clientRepo := &fakeClientRepo{}
	h := &SignupHandler{repo: repo, clientRepo: clientRepo, hasher: fakeHasher{}, transaction: fakeTransactionManager{}, metrics: metrics.NewNoopMetricsClient()}

	if _, err := h.Handle(context.Background(), Signup{
		Email: "d@b.com", Password: "plain-pw", Name: "Dave", Phone: "+123", Role: contracts.RoleDriver,
	}); err != nil {
		t.Fatal(err)
	}
	if clientRepo.created != nil {
		t.Fatalf("expected no client row for a Driver signup, got: %+v", clientRepo.created)
	}
}
