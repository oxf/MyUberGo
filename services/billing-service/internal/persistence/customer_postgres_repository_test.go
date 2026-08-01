package persistence

import (
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"errors"
	"testing"
)

func TestCreate_DuplicateClientProvider_ReturnsErrCustomerExists(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresCustomerRepository(testDB)
	clientID := seedClient(t, testDB)

	first := &domain.Customer{ClientID: clientID, Provider: domain.ProviderStub, ProviderCustomerID: "cus_first"}
	if _, err := repo.Create(ctx, first); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	second := &domain.Customer{ClientID: clientID, Provider: domain.ProviderStub, ProviderCustomerID: "cus_second"}
	_, err := repo.Create(ctx, second)
	if !errors.Is(err, domain.ErrCustomerExists) {
		t.Fatalf("expected ErrCustomerExists, got %v", err)
	}

	// A different provider for the same client must succeed — the unique
	// constraint is scoped to (client_id, provider), not client_id alone.
	third := &domain.Customer{ClientID: clientID, Provider: domain.ProviderStripe, ProviderCustomerID: "cus_stripe"}
	if _, err := repo.Create(ctx, third); err != nil {
		t.Fatalf("Create with a different provider should succeed, got %v", err)
	}
}

func TestGetByClientID_FoundAndNotFound(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresCustomerRepository(testDB)
	clientID := seedClient(t, testDB)

	if _, err := repo.GetByClientID(ctx, clientID, domain.ProviderStub); !errors.Is(err, commonerrors.ErrNotFound) {
		t.Fatalf("expected ErrNotFound before any customer exists, got %v", err)
	}

	created := &domain.Customer{ClientID: clientID, Provider: domain.ProviderStub, ProviderCustomerID: "cus_123"}
	id, err := repo.Create(ctx, created)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := repo.GetByClientID(ctx, clientID, domain.ProviderStub)
	if err != nil {
		t.Fatalf("GetByClientID: %v", err)
	}
	if got.ID != id || got.ClientID != clientID || got.Provider != domain.ProviderStub || got.ProviderCustomerID != "cus_123" {
		t.Fatalf("unexpected customer: %+v", got)
	}
}
