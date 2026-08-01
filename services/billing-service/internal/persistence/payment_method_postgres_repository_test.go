package persistence

import (
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"errors"
	"testing"
)

func newActiveMethod(clientID, providerMethodID string, isDefault bool) *domain.PaymentMethod {
	return &domain.PaymentMethod{
		ClientID:                clientID,
		Provider:                domain.ProviderStub,
		ProviderPaymentMethodID: providerMethodID,
		Brand:                   "visa",
		Last4:                   "4242",
		ExpMonth:                12,
		ExpYear:                 2030,
		IsDefault:               isDefault,
		Status:                  domain.PaymentMethodStatusActive,
	}
}

func TestCreate_DedupeIndexScopedByClient(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentMethodRepository(testDB)
	client1 := seedClient(t, testDB)
	client2 := seedClient(t, testDB)

	if _, err := repo.Create(ctx, newActiveMethod(client1, "pm_shared", false)); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	// Same (client, provider, provider_payment_method_id) for the same
	// client conflicts.
	_, err := repo.Create(ctx, newActiveMethod(client1, "pm_shared", false))
	if !errors.Is(err, domain.ErrPaymentMethodExists) {
		t.Fatalf("expected ErrPaymentMethodExists for same client, got %v", err)
	}

	// The same provider_payment_method_id for a DIFFERENT client must
	// succeed — the dedupe index is scoped by client_id, not global.
	if _, err := repo.Create(ctx, newActiveMethod(client2, "pm_shared", false)); err != nil {
		t.Fatalf("Create for a different client should succeed, got %v", err)
	}
}

func TestCreate_OneDefaultPerClient_UniqueIndexEnforced(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentMethodRepository(testDB)
	clientID := seedClient(t, testDB)

	if _, err := repo.Create(ctx, newActiveMethod(clientID, "pm_default_1", true)); err != nil {
		t.Fatalf("first default Create: %v", err)
	}

	// A second active default for the same client without clearing the
	// first must hit idx_payment_method_one_default's constraint directly.
	_, err := repo.Create(ctx, newActiveMethod(clientID, "pm_default_2", true))
	if err == nil {
		t.Fatal("expected the DB's one-default-per-client index to reject a second active default, got nil error")
	}
}

func TestClearDefault_OnlyAffectsActiveDefault(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentMethodRepository(testDB)
	clientID := seedClient(t, testDB)

	id, err := repo.Create(ctx, newActiveMethod(clientID, "pm_clear", true))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := repo.ClearDefault(ctx, clientID); err != nil {
		t.Fatalf("ClearDefault: %v", err)
	}

	got, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.IsDefault {
		t.Fatal("expected IsDefault=false after ClearDefault")
	}

	// Now a new default can be created without conflicting.
	if _, err := repo.Create(ctx, newActiveMethod(clientID, "pm_new_default", true)); err != nil {
		t.Fatalf("Create new default after ClearDefault: %v", err)
	}
}

func TestGetActiveByProviderID_ReReadsAfterDedupeConflict(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentMethodRepository(testDB)
	clientID := seedClient(t, testDB)

	id, err := repo.Create(ctx, newActiveMethod(clientID, "pm_retry", false))
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}

	// A retried/double-submitted attach hits ErrPaymentMethodExists ...
	_, err = repo.Create(ctx, newActiveMethod(clientID, "pm_retry", false))
	if !errors.Is(err, domain.ErrPaymentMethodExists) {
		t.Fatalf("expected ErrPaymentMethodExists, got %v", err)
	}

	// ... and the caller's re-read-by-provider-id must return the original row.
	got, err := repo.GetActiveByProviderID(ctx, clientID, domain.ProviderStub, "pm_retry")
	if err != nil {
		t.Fatalf("GetActiveByProviderID: %v", err)
	}
	if got.ID != id {
		t.Fatalf("expected re-read id %s, got %s", id, got.ID)
	}
}

func TestListByClientID(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentMethodRepository(testDB)
	clientID := seedClient(t, testDB)
	otherClientID := seedClient(t, testDB)

	if _, err := repo.Create(ctx, newActiveMethod(clientID, "pm_list_1", false)); err != nil {
		t.Fatalf("Create 1: %v", err)
	}
	if _, err := repo.Create(ctx, newActiveMethod(clientID, "pm_list_2", false)); err != nil {
		t.Fatalf("Create 2: %v", err)
	}
	if _, err := repo.Create(ctx, newActiveMethod(otherClientID, "pm_list_other", false)); err != nil {
		t.Fatalf("Create other client: %v", err)
	}

	list, err := repo.ListByClientID(ctx, clientID)
	if err != nil {
		t.Fatalf("ListByClientID: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 methods scoped to clientID, got %d", len(list))
	}
}

func TestGetByID_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentMethodRepository(testDB)

	_, err := repo.GetByID(ctx, "00000000-0000-0000-0000-000000000099")
	if !errors.Is(err, commonerrors.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGetDefaultActive(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentMethodRepository(testDB)
	clientID := seedClient(t, testDB)

	if _, err := repo.GetDefaultActive(ctx, clientID); !errors.Is(err, commonerrors.ErrNotFound) {
		t.Fatalf("expected ErrNotFound before any default exists, got %v", err)
	}

	id, err := repo.Create(ctx, newActiveMethod(clientID, "pm_getdefault", true))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := repo.GetDefaultActive(ctx, clientID)
	if err != nil {
		t.Fatalf("GetDefaultActive: %v", err)
	}
	if got.ID != id {
		t.Fatalf("expected default id %s, got %s", id, got.ID)
	}
}

func TestMarkRemoved_ClearsDefaultAndNotFoundOnMissing(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentMethodRepository(testDB)
	clientID := seedClient(t, testDB)

	id, err := repo.Create(ctx, newActiveMethod(clientID, "pm_remove", true))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := repo.MarkRemoved(ctx, id); err != nil {
		t.Fatalf("MarkRemoved: %v", err)
	}

	got, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID after MarkRemoved: %v", err)
	}
	if got.Status != domain.PaymentMethodStatusRemoved || got.IsDefault {
		t.Fatalf("expected status=removed, is_default=false, got status=%s is_default=%v", got.Status, got.IsDefault)
	}

	// The dedupe/one-default indexes are both WHERE status='active', so a
	// new default with the same provider_payment_method_id must now succeed.
	if _, err := repo.Create(ctx, newActiveMethod(clientID, "pm_remove", true)); err != nil {
		t.Fatalf("Create after removal of the conflicting row should succeed, got %v", err)
	}

	if err := repo.MarkRemoved(ctx, "00000000-0000-0000-0000-000000000099"); !errors.Is(err, commonerrors.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for a missing id, got %v", err)
	}
}
