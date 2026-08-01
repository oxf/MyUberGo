package persistence

import (
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"database/sql"
	"time"
)

type PostgresPaymentRepository struct {
	db *sql.DB
}

func NewPostgresPaymentRepository(db *sql.DB) *PostgresPaymentRepository {
	return &PostgresPaymentRepository{db: db}
}

const paymentSelectCols = `
	SELECT id, invoice_id, attempt_no, provider, provider_payment_intent_id, payment_method_id,
	       amount_minor, currency, status, failure_code, failure_message, idempotency_key, claimed_until
	FROM billing.payment
`

func (r *PostgresPaymentRepository) Create(ctx context.Context, p *domain.Payment) (string, error) {
	executor := Executor(ctx, r.db)
	var id string
	err := executor.QueryRowContext(ctx, `
		INSERT INTO billing.payment
			(invoice_id, attempt_no, provider, payment_method_id, amount_minor, currency, status, idempotency_key, claimed_until)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		RETURNING id
	`,
		p.InvoiceID, p.AttemptNo, p.Provider, p.PaymentMethodID, p.AmountMinor, p.Currency,
		p.Status, p.IdempotencyKey, p.ClaimedUntil,
	).Scan(&id)
	return id, err
}

func (r *PostgresPaymentRepository) GetNonTerminalByInvoiceID(ctx context.Context, invoiceID string) (*domain.Payment, error) {
	executor := Executor(ctx, r.db)
	row := executor.QueryRowContext(ctx, paymentSelectCols+`
		WHERE invoice_id = $1 AND status IN ('pending', 'processing')
		ORDER BY attempt_no DESC
		LIMIT 1
	`, invoiceID)

	p, err := scanPayment(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return p, err
}

func (r *PostgresPaymentRepository) GetByProviderIntentID(ctx context.Context, providerIntentID string) (*domain.Payment, error) {
	executor := Executor(ctx, r.db)
	row := executor.QueryRowContext(ctx, paymentSelectCols+` WHERE provider_payment_intent_id = $1`, providerIntentID)

	p, err := scanPayment(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return p, err
}

func (r *PostgresPaymentRepository) SetClaimedUntil(ctx context.Context, id string, claimedUntil string) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		UPDATE billing.payment SET claimed_until = $2, updated_at = NOW()
		WHERE id = $1
	`, id, claimedUntil)
	return err
}

// MarkProcessing is guarded to only affect a row still pending — a payment
// already processing/terminal is left alone (relevant once a webhook and
// ChargeWorker's own resume can both observe the same in-flight attempt).
func (r *PostgresPaymentRepository) MarkProcessing(ctx context.Context, id string, providerPaymentIntentID string) (bool, error) {
	executor := Executor(ctx, r.db)
	res, err := executor.ExecContext(ctx, `
		UPDATE billing.payment
		SET status = 'processing', provider_payment_intent_id = $2, updated_at = NOW()
		WHERE id = $1 AND status = 'pending'
	`, id, providerPaymentIntentID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// MarkSucceeded is guarded to only affect a row still pending/processing —
// see domain.PaymentRepository for why this matters once a webhook can race
// ChargeWorker's own resolution of the same payment.
func (r *PostgresPaymentRepository) MarkSucceeded(ctx context.Context, id string, providerPaymentIntentID string) (bool, error) {
	executor := Executor(ctx, r.db)
	res, err := executor.ExecContext(ctx, `
		UPDATE billing.payment
		SET status = 'succeeded', provider_payment_intent_id = $2, updated_at = NOW()
		WHERE id = $1 AND status IN ('pending', 'processing')
	`, id, providerPaymentIntentID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (r *PostgresPaymentRepository) MarkFailed(ctx context.Context, id, failureCode, failureMessage string) (bool, error) {
	executor := Executor(ctx, r.db)
	res, err := executor.ExecContext(ctx, `
		UPDATE billing.payment
		SET status = 'failed', failure_code = $2, failure_message = $3, updated_at = NOW()
		WHERE id = $1 AND status IN ('pending', 'processing')
	`, id, failureCode, failureMessage)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func scanPayment(row interface{ Scan(dest ...any) error }) (*domain.Payment, error) {
	var p domain.Payment
	var claimedUntilT sql.NullTime
	if err := row.Scan(
		&p.ID, &p.InvoiceID, &p.AttemptNo, &p.Provider, &p.ProviderPaymentIntentID, &p.PaymentMethodID,
		&p.AmountMinor, &p.Currency, &p.Status, &p.FailureCode, &p.FailureMessage, &p.IdempotencyKey,
		&claimedUntilT,
	); err != nil {
		return nil, err
	}
	if claimedUntilT.Valid {
		s := claimedUntilT.Time.Format(time.RFC3339)
		p.ClaimedUntil = &s
	}
	return &p, nil
}
