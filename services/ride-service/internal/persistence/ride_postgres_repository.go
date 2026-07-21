package persistence

import (
	"context"
	"database/sql"
	"fmt"
	commonerrors "ride-service/internal/common/errors"
	"ride-service/internal/domain"
	"time"
)

type PostgresRideRepository struct {
	db *sql.DB
}

func NewPostgresRideRepository(db *sql.DB) *PostgresRideRepository {
	return &PostgresRideRepository{db: db}
}

func (r *PostgresRideRepository) CreateRide(ctx context.Context, ride *domain.Ride) (string, error) {

	executor := Executor(ctx, r.db)

	var id string
	err := executor.QueryRowContext(
		ctx,
		`
		INSERT INTO ride.ride
			(client_id,status,pickup_lat,pickup_lng,pickup_address,dest_lat,dest_lng,dest_address,estimated_price,estimated_distance_km)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		RETURNING id
		`,
		ride.ClientID,
		ride.Status,
		ride.PickupLat,
		ride.PickupLng,
		ride.PickupAddress,
		ride.DestLat,
		ride.DestLng,
		ride.DestAddress,
		ride.EstimatedPrice,
		ride.EstimatedDistanceKm,
	).Scan(&id)

	return id, err
}

func (r *PostgresRideRepository) GetRideList(ctx context.Context, req domain.PageRequest) ([]*domain.Ride, error) {
	col, ok := domain.RideSortColumns[req.SortBy]
	if !ok {
		col = "created_at" // handler validates; this is a defensive fallback
	}
	dir := req.SortDir
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}

	query := fmt.Sprintf(`
		SELECT id, client_id, driver_id, status, pickup_lat, pickup_lng, pickup_address,
		       dest_lat, dest_lng, dest_address, estimated_price, estimated_distance_km, created_at
		FROM ride.ride
		ORDER BY %s %s
		LIMIT $1 OFFSET $2
	`, col, dir)

	rows, err := r.db.QueryContext(ctx, query, req.PageSize, (req.Page-1)*req.PageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.Ride
	for rows.Next() {
		ride, err := scanRide(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, ride)
	}
	return result, rows.Err()
}

func (r *PostgresRideRepository) CountRides(ctx context.Context) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ride.ride`).Scan(&n)
	return n, err
}

func (r *PostgresRideRepository) GetRideByID(ctx context.Context, id string) (*domain.Ride, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, client_id, driver_id, status, pickup_lat, pickup_lng, pickup_address,
		       dest_lat, dest_lng, dest_address, estimated_price, estimated_distance_km, created_at
		FROM ride.ride
		WHERE id = $1
	`, id)

	ride, err := scanRide(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return ride, err
}

// scanRide reads one ride row, normalizing created_at to RFC3339 and the
// nullable driver_id column to a *string (nil until a ride is matched).
func scanRide(row interface{ Scan(dest ...any) error }) (*domain.Ride, error) {
	var d domain.Ride
	var driverID sql.NullString
	var createdAt time.Time

	if err := row.Scan(
		&d.ID, &d.ClientID, &driverID, &d.Status,
		&d.PickupLat, &d.PickupLng, &d.PickupAddress,
		&d.DestLat, &d.DestLng, &d.DestAddress,
		&d.EstimatedPrice, &d.EstimatedDistanceKm, &createdAt,
	); err != nil {
		return nil, err
	}

	if driverID.Valid {
		d.DriverID = &driverID.String
	}
	d.CreatedAt = createdAt.Format(time.RFC3339)

	return &d, nil
}
