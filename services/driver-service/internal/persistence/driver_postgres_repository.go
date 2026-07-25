package persistence

import (
	"context"
	"database/sql"
	commonerrors "driver-service/internal/common/errors"
	"driver-service/internal/domain"
	"fmt"
	"time"
)

type PostgresDriverRepository struct {
	db *sql.DB
}

func NewPostgresDriverRepository(db *sql.DB) *PostgresDriverRepository {
	return &PostgresDriverRepository{db: db}
}

func (r *PostgresDriverRepository) CreateDriver(ctx context.Context, d *domain.Driver) (string, error) {
	var id string
	err := r.db.QueryRowContext(ctx, `
        INSERT INTO driver.driver (user_id, vehicle_type, license_plate)
        VALUES ($1,$2,$3)
        RETURNING id
    `, d.UserID, d.VehicleType, d.LicencePlate).Scan(&id)
	return id, err
}

func (r *PostgresDriverRepository) UpdateDriver(ctx context.Context, id, vehicleType, licencePlate string) error {
	res, err := r.db.ExecContext(ctx, `
        UPDATE driver.driver
        SET
            vehicle_type  = COALESCE(NULLIF($1,''), vehicle_type),
            license_plate = COALESCE(NULLIF($2,''), license_plate)
        WHERE id = $3
    `, vehicleType, licencePlate, id)
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

// UpdateDriverStatus flips a driver's status, guarded by the expected
// current status so it's idempotent against Kafka's at-most-once
// redelivery: a duplicate/late/out-of-order delivery matches zero rows and
// is a no-op rather than an error, same pattern as ride-service's
// MarkRideMatched.
func (r *PostgresDriverRepository) UpdateDriverStatus(ctx context.Context, id, fromStatus, toStatus string) (bool, error) {
	executor := Executor(ctx, r.db)
	res, err := executor.ExecContext(ctx, `
        UPDATE driver.driver
        SET status = $1
        WHERE id = $2 AND status = $3
    `, toStatus, id, fromStatus)
	if err != nil {
		return false, err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	return rows > 0, nil
}

// IncrementRidesCompleted bumps total_rides_completed by one.
func (r *PostgresDriverRepository) IncrementRidesCompleted(ctx context.Context, id string) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
        UPDATE driver.driver
        SET total_rides_completed = total_rides_completed + 1
        WHERE id = $1
    `, id)
	return err
}

func (r *PostgresDriverRepository) GetDriverList(ctx context.Context, req domain.PageRequest) ([]*domain.Driver, error) {
	col, ok := domain.DriverSortColumns[req.SortBy]
	if !ok {
		col = "created_at" // handler validates; this is a defensive fallback
	}
	dir := req.SortDir
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}

	query := fmt.Sprintf(`
        SELECT id, user_id, rating, vehicle_type, license_plate, status, total_rides_completed, created_at
        FROM driver.driver
        ORDER BY %s %s
        LIMIT $1 OFFSET $2
    `, col, dir)

	rows, err := r.db.QueryContext(ctx, query, req.PageSize, (req.Page-1)*req.PageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.Driver
	for rows.Next() {
		var d domain.Driver
		var createdAt time.Time
		if err := rows.Scan(&d.ID, &d.UserID, &d.Rating,
			&d.VehicleType, &d.LicencePlate, &d.Status, &d.TotalRidesCompleted, &createdAt); err != nil {
			return nil, err
		}
		d.CreatedAt = createdAt.Format(time.RFC3339)
		result = append(result, &d)
	}
	return result, rows.Err()
}

func (r *PostgresDriverRepository) CountDrivers(ctx context.Context) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM driver.driver`).Scan(&n)
	return n, err
}

func (r *PostgresDriverRepository) GetDriverByID(ctx context.Context, id string) (*domain.Driver, error) {
	var d domain.Driver
	var createdAt time.Time
	err := r.db.QueryRowContext(ctx, `
        SELECT id, user_id, rating, vehicle_type, license_plate, status, total_rides_completed, created_at
        FROM driver.driver WHERE id = $1
    `, id).Scan(&d.ID, &d.UserID, &d.Rating,
		&d.VehicleType, &d.LicencePlate, &d.Status, &d.TotalRidesCompleted, &createdAt)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	d.CreatedAt = createdAt.Format(time.RFC3339)
	return &d, nil
}
