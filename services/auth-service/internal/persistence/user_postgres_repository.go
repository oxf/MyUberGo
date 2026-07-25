package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	commonerrors "auth-service/internal/common/errors"
	"auth-service/internal/domain"

	"github.com/lib/pq"
)

type PostgresUserRepository struct {
	db *sql.DB
}

func NewPostgresUserRepository(db *sql.DB) *PostgresUserRepository {
	return &PostgresUserRepository{db: db}
}

func (r *PostgresUserRepository) CreateUser(ctx context.Context, u *domain.User) (string, error) {
	var id string
	err := Executor(ctx, r.db).QueryRowContext(ctx, `
		INSERT INTO auth."user" (email, password_hash, name, phone, role)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, u.Email, u.PasswordHash, u.Name, u.Phone, u.Role).Scan(&id)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			return "", commonerrors.ErrConflict
		}
	}
	return id, err
}

// GetByEmail is the only lookup that also returns password_hash — it backs
// login, which needs it to verify the submitted password. Every other read
// path never selects password_hash.
func (r *PostgresUserRepository) GetByEmail(ctx context.Context, email string) (*domain.User, error) {
	row := Executor(ctx, r.db).QueryRowContext(ctx, `
		SELECT id, email, password_hash, name, phone, role, created_at, updated_at
		FROM auth."user"
		WHERE email = $1 AND deleted_at IS NULL
	`, email)

	user, err := scanUserWithHash(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return user, err
}

func (r *PostgresUserRepository) GetByID(ctx context.Context, id string) (*domain.User, error) {
	row := Executor(ctx, r.db).QueryRowContext(ctx, `
		SELECT id, email, name, phone, role, created_at, updated_at
		FROM auth."user"
		WHERE id = $1 AND deleted_at IS NULL
	`, id)

	user, err := scanUser(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return user, err
}

func (r *PostgresUserRepository) GetUserList(ctx context.Context, req domain.PageRequest) ([]*domain.User, error) {
	col, ok := domain.UserSortColumns[req.SortBy]
	if !ok {
		col = "created_at" // handler validates; this is a defensive fallback
	}
	dir := req.SortDir
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}

	query := fmt.Sprintf(`
		SELECT id, email, name, phone, role, created_at, updated_at
		FROM auth."user"
		WHERE deleted_at IS NULL
		ORDER BY %s %s
		LIMIT $1 OFFSET $2
	`, col, dir)

	rows, err := Executor(ctx, r.db).QueryContext(ctx, query, req.PageSize, (req.Page-1)*req.PageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.User
	for rows.Next() {
		user, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, user)
	}
	return result, rows.Err()
}

func (r *PostgresUserRepository) CountUsers(ctx context.Context) (int, error) {
	var n int
	err := Executor(ctx, r.db).QueryRowContext(ctx, `SELECT COUNT(*) FROM auth."user" WHERE deleted_at IS NULL`).Scan(&n)
	return n, err
}

// scanUser reads id/email/name/phone/role/created_at/updated_at — the
// projection used everywhere except GetByEmail, which additionally needs
// password_hash to verify a login attempt.
func scanUser(row interface{ Scan(dest ...any) error }) (*domain.User, error) {
	var u domain.User
	var createdAt, updatedAt time.Time

	if err := row.Scan(&u.ID, &u.Email, &u.Name, &u.Phone, &u.Role, &createdAt, &updatedAt); err != nil {
		return nil, err
	}
	u.CreatedAt = createdAt.Format(time.RFC3339)
	u.UpdatedAt = updatedAt.Format(time.RFC3339)
	return &u, nil
}

func scanUserWithHash(row interface{ Scan(dest ...any) error }) (*domain.User, error) {
	var u domain.User
	var createdAt, updatedAt time.Time

	if err := row.Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Name, &u.Phone, &u.Role, &createdAt, &updatedAt); err != nil {
		return nil, err
	}
	u.CreatedAt = createdAt.Format(time.RFC3339)
	u.UpdatedAt = updatedAt.Format(time.RFC3339)
	return &u, nil
}
