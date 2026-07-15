package persistence

import (
	"context"
	"database/sql"
	commonerrors "driver-service/internal/common/errors"
	"driver-service/internal/domain"
	"time"
)

type PostgresDriverProfileRepository struct {
	db *sql.DB
}

func NewPostgresDriverProfileRepository(db *sql.DB) *PostgresDriverProfileRepository {
	return &PostgresDriverProfileRepository{db: db}
}

func (r *PostgresDriverProfileRepository) CreateDriverProfile(ctx context.Context, p *domain.DriverProfile) (string, error) {
	var id string
	err := r.db.QueryRowContext(ctx, `
        INSERT INTO driver.driver_profile (user_id, name, phone, vehicle_type, license_plate)
        VALUES ($1,$2,$3,$4,$5)
        RETURNING id
    `, p.UserID, p.DriverName, p.Phone, p.VehicleType, p.LicencePlate).Scan(&id)
	return id, err
}

func (r *PostgresDriverProfileRepository) UpdateDriverProfile(ctx context.Context, id, name, phone, vehicleType, licencePlate string) error {
	res, err := r.db.ExecContext(ctx, `
        UPDATE driver.driver_profile
        SET
            name          = COALESCE(NULLIF($1,''), name),
            phone         = COALESCE(NULLIF($2,''), phone),
            vehicle_type  = COALESCE(NULLIF($3,''), vehicle_type),
            license_plate = COALESCE(NULLIF($4,''), license_plate)
        WHERE id = $5
    `, name, phone, vehicleType, licencePlate, id)
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

func (r *PostgresDriverProfileRepository) GetDriverProfileList(ctx context.Context, page, pageSize int) ([]*domain.DriverProfile, error) {
	offset := page * pageSize
	rows, err := r.db.QueryContext(ctx, `
        SELECT id, user_id, name, phone, rating, vehicle_type, license_plate, status, total_rides_completed, created_at
        FROM driver.driver_profile
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
    `, pageSize, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.DriverProfile
	for rows.Next() {
		var d domain.DriverProfile
		var createdAt time.Time
		if err := rows.Scan(&d.ID, &d.UserID, &d.DriverName, &d.Phone, &d.Rating,
			&d.VehicleType, &d.LicencePlate, &d.Status, &d.TotalRidesCompleted, &createdAt); err != nil {
			return nil, err
		}
		d.CreatedAt = createdAt.Format(time.RFC3339)
		result = append(result, &d)
	}
	return result, nil
}

func (r *PostgresDriverProfileRepository) GetDriverProfileByID(ctx context.Context, id string) (*domain.DriverProfile, error) {
	var d domain.DriverProfile
	var createdAt time.Time
	err := r.db.QueryRowContext(ctx, `
        SELECT id, user_id, name, phone, rating, vehicle_type, license_plate, status, total_rides_completed, created_at
        FROM driver.driver_profile WHERE id = $1
    `, id).Scan(&d.ID, &d.UserID, &d.DriverName, &d.Phone, &d.Rating,
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
