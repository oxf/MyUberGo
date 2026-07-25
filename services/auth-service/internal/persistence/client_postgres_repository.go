package persistence

import (
	"context"
	"database/sql"
	"time"

	commonerrors "auth-service/internal/common/errors"
	"auth-service/internal/domain"
)

type PostgresClientRepository struct {
	db *sql.DB
}

func NewPostgresClientRepository(db *sql.DB) *PostgresClientRepository {
	return &PostgresClientRepository{db: db}
}

func (r *PostgresClientRepository) Create(ctx context.Context, c *domain.Client) (string, error) {
	var id string
	err := Executor(ctx, r.db).QueryRowContext(ctx, `
		INSERT INTO auth.client (user_id)
		VALUES ($1)
		RETURNING id
	`, c.UserID).Scan(&id)
	return id, err
}

func (r *PostgresClientRepository) GetByUserID(ctx context.Context, userID string) (*domain.Client, error) {
	row := Executor(ctx, r.db).QueryRowContext(ctx, `
		SELECT id, user_id, rating, total_rides_completed, created_at
		FROM auth.client
		WHERE user_id = $1
	`, userID)

	var c domain.Client
	var createdAt time.Time
	if err := row.Scan(&c.ID, &c.UserID, &c.Rating, &c.TotalRidesCompleted, &createdAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, commonerrors.ErrNotFound
		}
		return nil, err
	}
	c.CreatedAt = createdAt.Format(time.RFC3339)
	return &c, nil
}
