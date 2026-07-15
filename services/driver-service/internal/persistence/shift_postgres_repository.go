package persistence

import (
	"context"
	"database/sql"
	commonerrors "driver-service/internal/common/errors"
	"driver-service/internal/domain"
)

type PostgresShiftRepository struct {
	db *sql.DB
}

func NewPostgresShiftRepository(db *sql.DB) *PostgresShiftRepository {
	return &PostgresShiftRepository{db: db}
}

func (r *PostgresShiftRepository) CreateShift(ctx context.Context, s *domain.Shift) (string, error) {

	executor := Executor(ctx, r.db)

	var id string
	err := executor.QueryRowContext(
		ctx,
		`INSERT INTO driver.shift (driver_id) VALUES ($1) RETURNING id`,
		s.DriverID).Scan(&id)

	return id, err
}

func (r *PostgresShiftRepository) UpdateShift(
	ctx context.Context,
	s *domain.Shift,
) error {

	executor := Executor(ctx, r.db)

	res, err := executor.ExecContext(
		ctx,
		`
		UPDATE driver.shift
		SET total_rides=$1,
		    total_earnings=$2
		WHERE id=$3
		`,
		s.TotalRides,
		s.TotalEarnings,
		s.ID,
	)
	if err != nil {
		return err
	}

	if rows, err := res.RowsAffected(); err != nil {
		return err
	} else if rows == 0 {
		return commonerrors.ErrNotFound
	}

	return nil
}

func (r *PostgresShiftRepository) HasActiveShift(ctx context.Context, driverID string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `
        SELECT EXISTS (SELECT 1 FROM driver.shift WHERE driver_id = $1 AND ended_at IS NULL)
    `, driverID).Scan(&exists)
	return exists, err
}

func (r *PostgresShiftRepository) EndShift(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `
        UPDATE driver.shift SET ended_at = NOW() WHERE id = $1 AND ended_at IS NULL
    `, id)
	if err != nil {
		return err
	}

	if rows, err := res.RowsAffected(); err != nil {
		return err
	} else if rows == 0 {
		return commonerrors.ErrNotFound
	}

	return nil
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
		WHERE id = $1
	`, id).Scan(
		&d.ID,
		&d.DriverID,
		&d.StartedAt,
		&d.EndedAt,
		&d.TotalRides,
		&d.TotalEarnings,
	)

	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return &d, err
}
