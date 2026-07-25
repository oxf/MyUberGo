package persistence

import (
	"context"
	"database/sql"

	"auth-service/internal/domain"
)

type PostgresRefreshTokenRepository struct {
	db *sql.DB
}

func NewPostgresRefreshTokenRepository(db *sql.DB) *PostgresRefreshTokenRepository {
	return &PostgresRefreshTokenRepository{db: db}
}

func (r *PostgresRefreshTokenRepository) Store(ctx context.Context, t *domain.RefreshToken) error {
	_, err := Executor(ctx, r.db).ExecContext(ctx, `
		INSERT INTO auth.refresh_token (user_id, token, expires_at)
		VALUES ($1, $2, $3)
	`, t.UserID, t.Token, t.ExpiresAt)
	return err
}

func (r *PostgresRefreshTokenRepository) Exists(ctx context.Context, userID, token string) (bool, error) {
	var exists bool
	err := Executor(ctx, r.db).QueryRowContext(ctx, `
		SELECT exists(
			SELECT 1 FROM auth.refresh_token
			WHERE token = $1 AND user_id = $2 AND expires_at > now()
		)
	`, token, userID).Scan(&exists)
	return exists, err
}

func (r *PostgresRefreshTokenRepository) Delete(ctx context.Context, userID, token string) error {
	_, err := Executor(ctx, r.db).ExecContext(ctx, `
		DELETE FROM auth.refresh_token
		WHERE user_id = $1 AND token = $2
	`, userID, token)
	return err
}
