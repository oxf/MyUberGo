package persistence

import (
	"context"
	"database/sql"
	"driver-service/internal/domain"
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

	_, err := executor.ExecContext(
		ctx,
		`
		INSERT INTO driver.outbox_message
		(topic,event_type,payload,processed,retries)
		VALUES ($1,$2,$3,$4,$5)
		`,
		message.Topic,
		message.EventType,
		message.Payload,
		message.Processed,
		message.Retries,
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
		SELECT id, topic, event_type, payload, processed, retries
		FROM driver.outbox_message
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

		if err := rows.Scan(&m.ID, &m.Topic, &m.EventType, &m.Payload, &m.Processed, &m.Retries); err != nil {
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
		`UPDATE driver.outbox_message SET processed = true WHERE id = $1`,
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
		`UPDATE driver.outbox_message SET retries = retries + 1 WHERE id = $1`,
		id,
	)

	return err
}
