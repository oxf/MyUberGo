package persistence

import (
	"context"
	"database/sql"

	"auth-service/internal/domain"
)

type PostgresAdminRepository struct {
	db *sql.DB
}

func NewPostgresAdminRepository(db *sql.DB) *PostgresAdminRepository {
	return &PostgresAdminRepository{db: db}
}

func (r *PostgresAdminRepository) Create(ctx context.Context, a *domain.Admin) (string, error) {
	var id string
	err := Executor(ctx, r.db).QueryRowContext(ctx, `
		INSERT INTO auth.admin (user_id)
		VALUES ($1)
		RETURNING id
	`, a.UserID).Scan(&id)
	return id, err
}
