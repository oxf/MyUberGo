package persistence

import (
	"billing-service/internal/domain"
	"context"
	"database/sql"

	"github.com/oxf/MyUber/observability/obsoutbox"
)

type PostgresOutboxRepository struct {
	db *sql.DB
}

func NewPostgresOutboxRepository(db *sql.DB) *PostgresOutboxRepository {
	return &PostgresOutboxRepository{db: db}
}

func (r *PostgresOutboxRepository) Insert(
	ctx context.Context,
	message *domain.OutboxMessage,
) error {

	executor := Executor(ctx, r.db)

	// Must stay `any`, not []byte: a nil []byte boxed into an interface{} isn't a nil interface,
	// so database/sql's NULL-detection misses it and the jsonb column's cast fails instead of storing NULL.
	var traceContext any
	if tc := obsoutbox.MarshalTraceContext(ctx); tc != nil {
		traceContext = tc
	}

	_, err := executor.ExecContext(
		ctx,
		`
		INSERT INTO billing.outbox_message
		(topic,event_type,payload,processed,retries,trace_context)
		VALUES ($1,$2,$3,$4,$5,$6)
		`,
		message.Topic,
		message.EventType,
		message.Payload,
		message.Processed,
		message.Retries,
		traceContext,
	)

	return err
}

func (r *PostgresOutboxRepository) GetUnprocessedBatch(
	ctx context.Context,
	limit int,
) ([]*domain.OutboxMessage, error) {

	executor := Executor(ctx, r.db)

	rows, err := executor.QueryContext(
		ctx,
		`
		SELECT id, topic, event_type, payload, processed, retries, trace_context
		FROM billing.outbox_message
		WHERE processed = false
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
		`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []*domain.OutboxMessage

	for rows.Next() {
		var m domain.OutboxMessage

		if err := rows.Scan(&m.ID, &m.Topic, &m.EventType, &m.Payload, &m.Processed, &m.Retries, &m.TraceContext); err != nil {
			return nil, err
		}

		messages = append(messages, &m)
	}

	return messages, rows.Err()
}

func (r *PostgresOutboxRepository) MarkProcessed(
	ctx context.Context,
	id string,
) error {

	executor := Executor(ctx, r.db)

	_, err := executor.ExecContext(
		ctx,
		`UPDATE billing.outbox_message SET processed = true WHERE id = $1`,
		id,
	)

	return err
}

func (r *PostgresOutboxRepository) IncrementRetries(
	ctx context.Context,
	id string,
) error {

	executor := Executor(ctx, r.db)

	_, err := executor.ExecContext(
		ctx,
		`UPDATE billing.outbox_message SET retries = retries + 1 WHERE id = $1`,
		id,
	)

	return err
}

func (r *PostgresOutboxRepository) CountByRetries(
	ctx context.Context,
	maxRetries int,
) (pending int64, parked int64, err error) {

	executor := Executor(ctx, r.db)

	err = executor.QueryRowContext(
		ctx,
		`
		SELECT
			count(*) FILTER (WHERE retries < $1),
			count(*) FILTER (WHERE retries >= $1)
		FROM billing.outbox_message
		WHERE processed = false
		`,
		maxRetries,
	).Scan(&pending, &parked)

	return pending, parked, err
}
