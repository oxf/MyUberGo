package persistence

import (
	"context"
	"errors"
	commonerrors "ride-service/internal/common/errors"
	"testing"
)

func TestGetByName_SeededStandardTariff(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresTariffRepository(testDB)

	got, err := repo.GetByName(ctx, "Standard")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if got.Name != "Standard" || got.Currency != "EUR" || got.BaseFareMinor != 300 || got.PricePerKmMinor != 100 || got.PricePerMinMinor != 20 {
		t.Fatalf("unexpected tariff: %+v", got)
	}
}

func TestGetByName_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresTariffRepository(testDB)

	_, err := repo.GetByName(ctx, "DoesNotExist")
	if !errors.Is(err, commonerrors.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
