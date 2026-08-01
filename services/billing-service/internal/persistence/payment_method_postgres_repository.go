package persistence

import (
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"database/sql"
	"time"
)

type PostgresPaymentMethodRepository struct {
	db *sql.DB
}

func NewPostgresPaymentMethodRepository(db *sql.DB) *PostgresPaymentMethodRepository {
	return &PostgresPaymentMethodRepository{db: db}
}

func (r *PostgresPaymentMethodRepository) Create(ctx context.Context, m *domain.PaymentMethod) (string, error) {
	executor := Executor(ctx, r.db)
	var id string
	err := executor.QueryRowContext(ctx, `
		INSERT INTO billing.payment_method
			(client_id, provider, provider_payment_method_id, brand, last4, exp_month, exp_year, is_default, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		RETURNING id
	`,
		m.ClientID, m.Provider, m.ProviderPaymentMethodID, m.Brand, m.Last4,
		m.ExpMonth, m.ExpYear, m.IsDefault, m.Status,
	).Scan(&id)
	if err != nil {
		if isUniqueViolation(err) {
			return "", domain.ErrPaymentMethodExists
		}
		return "", err
	}
	return id, nil
}

func (r *PostgresPaymentMethodRepository) ClearDefault(ctx context.Context, clientID string) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		UPDATE billing.payment_method
		SET is_default = FALSE, updated_at = NOW()
		WHERE client_id = $1 AND is_default = TRUE AND status = 'active'
	`, clientID)
	return err
}

func (r *PostgresPaymentMethodRepository) ListByClientID(ctx context.Context, clientID string) ([]*domain.PaymentMethod, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, client_id, provider, provider_payment_method_id, brand, last4, exp_month, exp_year, is_default, status, created_at
		FROM billing.payment_method
		WHERE client_id = $1
		ORDER BY created_at DESC
	`, clientID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.PaymentMethod
	for rows.Next() {
		m, err := scanPaymentMethod(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

func (r *PostgresPaymentMethodRepository) GetByID(ctx context.Context, id string) (*domain.PaymentMethod, error) {
	executor := Executor(ctx, r.db)
	row := executor.QueryRowContext(ctx, `
		SELECT id, client_id, provider, provider_payment_method_id, brand, last4, exp_month, exp_year, is_default, status, created_at
		FROM billing.payment_method
		WHERE id = $1
	`, id)
	m, err := scanPaymentMethod(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return m, err
}

func (r *PostgresPaymentMethodRepository) GetDefaultActive(ctx context.Context, clientID string) (*domain.PaymentMethod, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, client_id, provider, provider_payment_method_id, brand, last4, exp_month, exp_year, is_default, status, created_at
		FROM billing.payment_method
		WHERE client_id = $1 AND is_default = TRUE AND status = 'active'
	`, clientID)
	m, err := scanPaymentMethod(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return m, err
}

// GetActiveByProviderID re-reads the row after an ErrPaymentMethodExists
// violation, so a retried attach returns the existing id instead of
// erroring — matches idx_payment_method_dedupe's scope (client_id,
// provider, provider_payment_method_id, WHERE status='active').
func (r *PostgresPaymentMethodRepository) GetActiveByProviderID(ctx context.Context, clientID, provider, providerPaymentMethodID string) (*domain.PaymentMethod, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, client_id, provider, provider_payment_method_id, brand, last4, exp_month, exp_year, is_default, status, created_at
		FROM billing.payment_method
		WHERE client_id = $1 AND provider = $2 AND provider_payment_method_id = $3 AND status = 'active'
	`, clientID, provider, providerPaymentMethodID)
	m, err := scanPaymentMethod(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return m, err
}

func (r *PostgresPaymentMethodRepository) MarkRemoved(ctx context.Context, id string) error {
	executor := Executor(ctx, r.db)
	res, err := executor.ExecContext(ctx, `
		UPDATE billing.payment_method SET status = 'removed', is_default = FALSE, updated_at = NOW()
		WHERE id = $1
	`, id)
	if err != nil {
		return err
	}
	if rows, err := res.RowsAffected(); err != nil {
		return err
	} else if rows == 0 {
		return commonerrors.ErrNotFound
	}
	return nil
}

func scanPaymentMethod(row interface{ Scan(dest ...any) error }) (*domain.PaymentMethod, error) {
	var m domain.PaymentMethod
	var createdAt time.Time
	if err := row.Scan(
		&m.ID, &m.ClientID, &m.Provider, &m.ProviderPaymentMethodID, &m.Brand, &m.Last4,
		&m.ExpMonth, &m.ExpYear, &m.IsDefault, &m.Status, &createdAt,
	); err != nil {
		return nil, err
	}
	m.CreatedAt = createdAt.Format(time.RFC3339)
	return &m, nil
}
