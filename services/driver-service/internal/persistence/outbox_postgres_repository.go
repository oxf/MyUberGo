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
