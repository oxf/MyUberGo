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

	// Captured here, not by callers: every outbox insert automatically
	// carries forward whatever trace is active on ctx (the command
	// handler's span), so no command handler needs to know this exists.
	//
	// traceContext is declared `any`, not []byte: a nil []byte boxed into
	// an interface{} arg is NOT the same as a nil interface, so
	// database/sql's NULL-detection would miss it and lib/pq would send an
	// empty (not NULL) value — which then fails the jsonb column's
	// implicit text->json cast with "invalid input syntax for type json"
	// instead of storing NULL. Leaving this `any` at its zero value (a true
	// nil interface) when there's no trace context avoids that.
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
