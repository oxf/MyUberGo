package persistence

import (
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"database/sql"
)

type PostgresCustomerRepository struct {
	db *sql.DB
}

func NewPostgresCustomerRepository(db *sql.DB) *PostgresCustomerRepository {
	return &PostgresCustomerRepository{db: db}
}

func (r *PostgresCustomerRepository) GetByClientID(ctx context.Context, clientID, provider string) (*domain.Customer, error) {
	executor := Executor(ctx, r.db)
	row := executor.QueryRowContext(ctx, `
		SELECT id, client_id, provider, provider_customer_id
		FROM billing.customer
		WHERE client_id = $1 AND provider = $2
	`, clientID, provider)

	var c domain.Customer
	if err := row.Scan(&c.ID, &c.ClientID, &c.Provider, &c.ProviderCustomerID); err != nil {
		if err == sql.ErrNoRows {
			return nil, commonerrors.ErrNotFound
		}
		return nil, err
	}
	return &c, nil
}

func (r *PostgresCustomerRepository) Create(ctx context.Context, c *domain.Customer) (string, error) {
	executor := Executor(ctx, r.db)
	var id string
	err := executor.QueryRowContext(ctx, `
		INSERT INTO billing.customer (client_id, provider, provider_customer_id)
		VALUES ($1, $2, $3)
		RETURNING id
	`, c.ClientID, c.Provider, c.ProviderCustomerID).Scan(&id)
	return id, err
}
