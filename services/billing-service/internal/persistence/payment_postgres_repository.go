package persistence

import (
	"billing-service/internal/domain"
	"context"
	"database/sql"
)

type PostgresPaymentRepository struct {
	db *sql.DB
}

func NewPostgresPaymentRepository(db *sql.DB) *PostgresPaymentRepository {
	return &PostgresPaymentRepository{db: db}
}

func (r *PostgresPaymentRepository) Create(ctx context.Context, p *domain.Payment) (string, error) {
	executor := Executor(ctx, r.db)
	var id string
	err := executor.QueryRowContext(ctx, `
		INSERT INTO billing.payment
			(invoice_id, attempt_no, provider, payment_method_id, amount_minor, currency, status, idempotency_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING id
	`,
		p.InvoiceID, p.AttemptNo, p.Provider, p.PaymentMethodID, p.AmountMinor, p.Currency,
		p.Status, p.IdempotencyKey,
	).Scan(&id)
	return id, err
}

func (r *PostgresPaymentRepository) MarkSucceeded(ctx context.Context, id string, providerPaymentIntentID string) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		UPDATE billing.payment
		SET status = 'succeeded', provider_payment_intent_id = $2, updated_at = NOW()
		WHERE id = $1
	`, id, providerPaymentIntentID)
	return err
}

func (r *PostgresPaymentRepository) MarkFailed(ctx context.Context, id, failureCode, failureMessage string) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		UPDATE billing.payment
		SET status = 'failed', failure_code = $2, failure_message = $3, updated_at = NOW()
		WHERE id = $1
	`, id, failureCode, failureMessage)
	return err
}
