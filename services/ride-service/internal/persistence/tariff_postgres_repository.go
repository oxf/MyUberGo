package persistence

import (
	"context"
	"database/sql"
	commonerrors "ride-service/internal/common/errors"
	"ride-service/internal/domain"
)

type PostgresTariffRepository struct {
	db *sql.DB
}

func NewPostgresTariffRepository(db *sql.DB) *PostgresTariffRepository {
	return &PostgresTariffRepository{db: db}
}

func (r *PostgresTariffRepository) GetByName(ctx context.Context, name string) (*domain.Tariff, error) {
	executor := Executor(ctx, r.db)
	row := executor.QueryRowContext(ctx, `
		SELECT id, name, base_fare_minor, price_per_km_minor, price_per_min_minor, currency
		FROM ride.tariff
		WHERE name = $1
	`, name)

	var t domain.Tariff
	if err := row.Scan(&t.ID, &t.Name, &t.BaseFareMinor, &t.PricePerKmMinor, &t.PricePerMinMinor, &t.Currency); err != nil {
		if err == sql.ErrNoRows {
			return nil, commonerrors.ErrNotFound
		}
		return nil, err
	}
	return &t, nil
}
