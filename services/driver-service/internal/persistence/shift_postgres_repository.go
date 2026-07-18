package persistence

import (
	"context"
	"database/sql"
	commonerrors "driver-service/internal/common/errors"
	"driver-service/internal/domain"
	"fmt"
	"time"
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

func (r *PostgresShiftRepository) GetShiftList(ctx context.Context, req domain.PageRequest) ([]*domain.Shift, error) {
	col, ok := domain.ShiftSortColumns[req.SortBy]
	if !ok {
		col = "started_at" // handler validates; this is a defensive fallback
	}
	dir := req.SortDir
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}

	query := fmt.Sprintf(`
        SELECT id, driver_id, started_at, ended_at, total_rides, total_earnings
        FROM driver.shift
        ORDER BY %s %s
        LIMIT $1 OFFSET $2
    `, col, dir)

	rows, err := r.db.QueryContext(ctx, query, req.PageSize, (req.Page-1)*req.PageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.Shift
	for rows.Next() {
		s, err := scanShift(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

func (r *PostgresShiftRepository) CountShifts(ctx context.Context) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM driver.shift`).Scan(&n)
	return n, err
}

func (r *PostgresShiftRepository) GetShiftByID(ctx context.Context, id string) (*domain.Shift, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, driver_id, started_at, ended_at, total_rides, total_earnings
		FROM driver.shift
		WHERE id = $1
	`, id)

	s, err := scanShift(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return s, err
}

// scanShift reads one shift row, normalizing timestamps to RFC3339 (matching
// driver-profile CreatedAt) instead of the driver's raw time encoding.
func scanShift(row interface{ Scan(dest ...any) error }) (*domain.Shift, error) {
	var d domain.Shift
	var startedAt time.Time
	var endedAt sql.NullTime

	if err := row.Scan(&d.ID, &d.DriverID, &startedAt, &endedAt, &d.TotalRides, &d.TotalEarnings); err != nil {
		return nil, err
	}

	d.StartedAt = startedAt.Format(time.RFC3339)
	if endedAt.Valid {
		s := endedAt.Time.Format(time.RFC3339)
		d.EndedAt = &s
	}
	return &d, nil
}
