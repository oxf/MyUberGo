package persistence

import (
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"database/sql"
	"time"
)

type PostgresPspEventRepository struct {
	db *sql.DB
}

func NewPostgresPspEventRepository(db *sql.DB) *PostgresPspEventRepository {
	return &PostgresPspEventRepository{db: db}
}

// Insert returns ErrDuplicatePspEvent on the id (Stripe's own event id)
// primary-key violation, rather than a raw driver error — the caller then
// reads the existing row to decide whether this is a fully-processed
// no-op or a previously-interrupted delivery worth retrying.
func (r *PostgresPspEventRepository) Insert(ctx context.Context, e *domain.PspEvent) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		INSERT INTO billing.psp_event (id, type, api_version, payload)
		VALUES ($1,$2,$3,$4)
	`, e.ID, e.Type, e.APIVersion, e.Payload)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.ErrDuplicatePspEvent
		}
		return err
	}
	return nil
}

func (r *PostgresPspEventRepository) GetByID(ctx context.Context, id string) (*domain.PspEvent, error) {
	executor := Executor(ctx, r.db)
	row := executor.QueryRowContext(ctx, `
		SELECT id, type, api_version, payload, received_at, processed_at, process_error
		FROM billing.psp_event
		WHERE id = $1
	`, id)

	var e domain.PspEvent
	var receivedAt time.Time
	var processedAtT sql.NullTime
	var processError sql.NullString
	if err := row.Scan(&e.ID, &e.Type, &e.APIVersion, &e.Payload, &receivedAt, &processedAtT, &processError); err != nil {
		if err == sql.ErrNoRows {
			return nil, commonerrors.ErrNotFound
		}
		return nil, err
	}
	e.ReceivedAt = receivedAt.Format(time.RFC3339)
	if processedAtT.Valid {
		s := processedAtT.Time.Format(time.RFC3339)
		e.ProcessedAt = &s
	}
	if processError.Valid {
		e.ProcessError = &processError.String
	}
	return &e, nil
}

func (r *PostgresPspEventRepository) MarkProcessed(ctx context.Context, id string) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		UPDATE billing.psp_event SET processed_at = NOW(), process_error = NULL WHERE id = $1
	`, id)
	return err
}
