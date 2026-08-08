package persistence

import (
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"database/sql"
	"fmt"
	"time"
)

type PostgresInvoiceRepository struct {
	db *sql.DB
}

func NewPostgresInvoiceRepository(db *sql.DB) *PostgresInvoiceRepository {
	return &PostgresInvoiceRepository{db: db}
}

// Create inserts the invoice and its lines in the caller's ambient
// transaction. A (ride_id, type) unique-violation — a redelivered
// ride.completed/ride.cancelled event — surfaces as ErrDuplicateInvoice
// rather than a raw driver error, so callers can treat it as a no-op
// success instead of pre-checking with a racy SELECT.
func (r *PostgresInvoiceRepository) Create(ctx context.Context, inv *domain.Invoice) (string, error) {
	executor := Executor(ctx, r.db)

	var id string
	err := executor.QueryRowContext(ctx, `
		INSERT INTO billing.invoice
			(ride_id, client_id, driver_id, type, status, amount_minor, currency, next_attempt_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING id
	`,
		inv.RideID, inv.ClientID, inv.DriverID, inv.Type, inv.Status,
		inv.AmountMinor, inv.Currency, inv.NextAttemptAt,
	).Scan(&id)
	if err != nil {
		if isUniqueViolation(err) {
			return "", domain.ErrDuplicateInvoice
		}
		return "", err
	}

	for _, line := range inv.Lines {
		if _, err := executor.ExecContext(ctx, `
			INSERT INTO billing.invoice_line (invoice_id, kind, amount_minor, currency, description)
			VALUES ($1,$2,$3,$4,$5)
		`, id, line.Kind, line.AmountMinor, line.Currency, line.Description); err != nil {
			return "", err
		}
	}

	return id, nil
}

// attempt_count is a correlated subquery, not a stored column (dropped from
// billing.invoice) — every invoice-read method shares this
// one SQL expression for it, including GetDueForCharge, so ChargeWorker's
// claim step gets the current count for free on the same row it already
// locked, with no separate repository call needed.
const invoiceSelectCols = `
	SELECT i.id, i.ride_id, i.client_id, i.driver_id, i.type, i.status, i.amount_minor, i.currency,
	       (SELECT COUNT(*) FROM billing.payment p WHERE p.invoice_id = i.id) AS attempt_count,
	       i.next_attempt_at, i.created_at, i.paid_at
	FROM billing.invoice i
`

func (r *PostgresInvoiceRepository) GetByID(ctx context.Context, id string) (*domain.Invoice, error) {
	row := r.db.QueryRowContext(ctx, invoiceSelectCols+` WHERE i.id = $1`, id)
	inv, err := scanInvoice(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := r.attachLines(ctx, inv); err != nil {
		return nil, err
	}
	return inv, nil
}

func (r *PostgresInvoiceRepository) GetByRideID(ctx context.Context, rideID, invoiceType string) (*domain.Invoice, error) {
	row := r.db.QueryRowContext(ctx, invoiceSelectCols+` WHERE i.ride_id = $1 AND i.type = $2`, rideID, invoiceType)
	inv, err := scanInvoice(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := r.attachLines(ctx, inv); err != nil {
		return nil, err
	}
	return inv, nil
}

func (r *PostgresInvoiceRepository) attachLines(ctx context.Context, inv *domain.Invoice) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT kind, amount_minor, currency, description FROM billing.invoice_line WHERE invoice_id = $1
	`, inv.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var l domain.InvoiceLine
		if err := rows.Scan(&l.Kind, &l.AmountMinor, &l.Currency, &l.Description); err != nil {
			return err
		}
		inv.Lines = append(inv.Lines, l)
	}
	return rows.Err()
}

func (r *PostgresInvoiceRepository) GetList(ctx context.Context, req domain.PageRequest) ([]*domain.Invoice, error) {
	col, ok := domain.InvoiceSortColumns[req.SortBy]
	if !ok {
		col = "created_at"
	}
	dir := req.SortDir
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}

	query := fmt.Sprintf(invoiceSelectCols+` ORDER BY %s %s LIMIT $1 OFFSET $2`, col, dir)
	rows, err := r.db.QueryContext(ctx, query, req.PageSize, (req.Page-1)*req.PageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.Invoice
	for rows.Next() {
		inv, err := scanInvoice(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, inv)
	}
	return result, rows.Err()
}

func (r *PostgresInvoiceRepository) CountInvoices(ctx context.Context) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM billing.invoice`).Scan(&n)
	return n, err
}

func (r *PostgresInvoiceRepository) CountOpenByClientID(ctx context.Context, clientID string) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM billing.invoice WHERE client_id = $1 AND status = 'open'
	`, clientID).Scan(&n)
	return n, err
}

// GetDueForCharge is the ChargeWorker sweep query: open invoices whose
// retry deadline has elapsed, locked so concurrent worker ticks (or a
// future multi-instance deployment) can't double-attempt the same invoice.
func (r *PostgresInvoiceRepository) GetDueForCharge(ctx context.Context, limit int) ([]*domain.Invoice, error) {
	executor := Executor(ctx, r.db)
	rows, err := executor.QueryContext(ctx, invoiceSelectCols+`
		WHERE i.status = 'open' AND i.next_attempt_at IS NOT NULL AND i.next_attempt_at <= NOW()
		ORDER BY i.next_attempt_at
		LIMIT $1
		FOR UPDATE OF i SKIP LOCKED
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.Invoice
	for rows.Next() {
		inv, err := scanInvoice(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, inv)
	}
	return result, rows.Err()
}

// MarkPaid is guarded to WHERE status='open' and reports whether the guard
// won — see domain.InvoiceRepository for why (a webhook and ChargeWorker
// racing to resolve the same invoice must not both post T2).
func (r *PostgresInvoiceRepository) MarkPaid(ctx context.Context, id, paidAt string) (bool, error) {
	executor := Executor(ctx, r.db)
	res, err := executor.ExecContext(ctx, `
		UPDATE billing.invoice SET status = 'paid', paid_at = $2, next_attempt_at = NULL, updated_at = NOW()
		WHERE id = $1 AND status = 'open'
	`, id, paidAt)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (r *PostgresInvoiceRepository) SetNextAttemptAt(ctx context.Context, id string, nextAttemptAt *string) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		UPDATE billing.invoice SET next_attempt_at = $2, updated_at = NOW()
		WHERE id = $1
	`, id, nextAttemptAt)
	return err
}

func (r *PostgresInvoiceRepository) MarkUncollectible(ctx context.Context, id string) (bool, error) {
	executor := Executor(ctx, r.db)
	res, err := executor.ExecContext(ctx, `
		UPDATE billing.invoice SET status = 'uncollectible', next_attempt_at = NULL, updated_at = NOW()
		WHERE id = $1 AND status = 'open'
	`, id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func scanInvoice(row interface{ Scan(dest ...any) error }) (*domain.Invoice, error) {
	var inv domain.Invoice
	var driverID sql.NullString
	var createdAt time.Time
	var nextAttemptAtT, paidAtT sql.NullTime

	if err := row.Scan(
		&inv.ID, &inv.RideID, &inv.ClientID, &driverID, &inv.Type, &inv.Status,
		&inv.AmountMinor, &inv.Currency, &inv.AttemptCount, &nextAttemptAtT, &createdAt, &paidAtT,
	); err != nil {
		return nil, err
	}

	if driverID.Valid {
		inv.DriverID = &driverID.String
	}
	if nextAttemptAtT.Valid {
		s := nextAttemptAtT.Time.Format(time.RFC3339)
		inv.NextAttemptAt = &s
	}
	if paidAtT.Valid {
		s := paidAtT.Time.Format(time.RFC3339)
		inv.PaidAt = &s
	}
	inv.CreatedAt = createdAt.Format(time.RFC3339)

	return &inv, nil
}
