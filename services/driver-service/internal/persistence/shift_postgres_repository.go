package persistence

import (
	"context"
	"database/sql"
	"driver-service/internal/domain"
)

type PostgresShiftRepository struct {
	db *sql.DB
}

func NewPostgresShiftRepository(db *sql.DB) *PostgresShiftRepository {
	return &PostgresShiftRepository{db: db}
}

func (r *PostgresShiftRepository) CreateShift(ctx context.Context, s *domain.Shift) (string, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var id string
	err = tx.QueryRowContext(ctx, `
        INSERT INTO driver.shift (driver_id) VALUES ($1) RETURNING id
    `, s.DriverID).Scan(&id)

	if err != nil {
		tx.Rollback()
		return "", err
	}

	err = tx.Commit()
	if err != nil {
		return "", err
	}
	return id, err
}

func (r *PostgresShiftRepository) UpdateShift(ctx context.Context, s *domain.Shift) (string, error) {
	var id string
	err := r.db.QueryRowContext(ctx, `
        INSERT INTO driver.shift (driver_id) VALUES ($1) RETURNING id
    `, s.DriverID).Scan(&id)
	return id, err
}

func (r *PostgresShiftRepository) HasActiveShift(ctx context.Context, driverID string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `
        SELECT EXISTS (SELECT 1 FROM driver.shift WHERE driver_id = $1 AND ended_at IS NULL)
    `, driverID).Scan(&exists)
	return exists, err
}

func (r *PostgresShiftRepository) EndShift(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `
        UPDATE driver.shift SET ended_at = NOW() WHERE id = $1 AND ended_at IS NULL
    `, id)
	return err
}

func (r *PostgresShiftRepository) GetShiftList(ctx context.Context, page, pageSize int) ([]*domain.Shift, error) {
	offset := page * pageSize
	rows, err := r.db.QueryContext(ctx, `
        SELECT id, driver_id, started_at, ended_at, total_rides, total_earnings
        FROM driver.shift
        ORDER BY started_at DESC
        LIMIT $1 OFFSET $2
    `, pageSize, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.Shift
	for rows.Next() {
		var d domain.Shift
		if err := rows.Scan(&d.ID,
			&d.DriverID,
			&d.StartedAt,
			&d.EndedAt,
			&d.TotalRides,
			&d.TotalEarnings); err != nil {
			return nil, err
		}
		result = append(result, &d)
	}
	return result, nil
}

func (r *PostgresShiftRepository) GetShiftByID(ctx context.Context, id string) (*domain.Shift, error) {
	var d domain.Shift

	err := r.db.QueryRowContext(ctx, `
		SELECT id, driver_id, started_at, ended_at, total_rides, total_earnings
		FROM driver.shift
		WHERE driver_id = $1
		ORDER BY started_at DESC
	`, id).Scan(
		&d.ID,
		&d.DriverID,
		&d.StartedAt,
		&d.EndedAt,
		&d.TotalRides,
		&d.TotalEarnings,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &d, err
}
