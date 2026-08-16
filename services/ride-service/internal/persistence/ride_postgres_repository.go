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
			(client_id,status,pickup_lat,pickup_lng,pickup_address,dest_lat,dest_lng,dest_address,estimated_price_minor,currency,estimated_distance_km)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
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
		ride.EstimatedPriceMinor,
		ride.Currency,
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
		       dest_lat, dest_lng, dest_address, estimated_price_minor, currency, estimated_distance_km, bill_id, created_at
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
		       dest_lat, dest_lng, dest_address, estimated_price_minor, currency, estimated_distance_km, bill_id, created_at
		FROM ride.ride
		WHERE id = $1
	`, id)

	ride, err := scanRide(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return ride, err
}

// MarkRideMatched flips a ride to Matched and assigns its driver. The
// "AND status = 'Requested'" guard makes this idempotent against Kafka's
// at-most-once redelivery of ride.accepted: a duplicate/late delivery
// matches zero rows and is a no-op rather than an error.
func (r *PostgresRideRepository) MarkRideMatched(ctx context.Context, rideID, driverID string, matchedAt time.Time) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		UPDATE ride.ride
		SET driver_id = $1, status = 'Matched', matched_at = $2
		WHERE id = $3 AND status = 'Requested'
	`, driverID, matchedAt, rideID)
	return err
}

// FailRide flips a ride to Failed once matching-service gives up after exhausting its
// retries. The "AND status = 'Requested'" guard makes this idempotent against Kafka's
// at-least-once redelivery of ride.matching_failed, same pattern as MarkRideMatched.
func (r *PostgresRideRepository) FailRide(ctx context.Context, rideID string) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		UPDATE ride.ride
		SET status = 'Failed'
		WHERE id = $1 AND status = 'Requested'
	`, rideID)
	return err
}

// MarkRideBilled sets bill_id once billing-service publishes
// payment.completed. The "AND bill_id IS NULL" guard makes this idempotent
// against Kafka's at-least-once redelivery, same pattern as MarkRideMatched.
func (r *PostgresRideRepository) MarkRideBilled(ctx context.Context, rideID, billID string) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		UPDATE ride.ride
		SET bill_id = $1
		WHERE id = $2 AND bill_id IS NULL
	`, billID, rideID)
	return err
}

// GetRideForUpdate locks the ride row within the current transaction so its
// status/ownership can be checked and then mutated atomically.
func (r *PostgresRideRepository) GetRideForUpdate(ctx context.Context, id string) (*domain.Ride, error) {
	executor := Executor(ctx, r.db)
	row := executor.QueryRowContext(ctx, `
		SELECT id, client_id, driver_id, status, pickup_lat, pickup_lng, pickup_address,
		       dest_lat, dest_lng, dest_address, estimated_price_minor, currency, estimated_distance_km, bill_id, created_at
		FROM ride.ride
		WHERE id = $1
		FOR UPDATE
	`, id)

	ride, err := scanRide(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return ride, err
}

// CancelRide flips a ride to Cancelled. Callers are expected to have already
// validated ownership/current status via GetRideForUpdate in the same
// transaction, so this is an unconditional write.
func (r *PostgresRideRepository) CancelRide(ctx context.Context, id, reason string) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		UPDATE ride.ride
		SET status = 'Cancelled', cancelled_at = NOW(), cancellation_reason = $2
		WHERE id = $1
	`, id, reason)
	return err
}

// MarkRideStarted flips a ride to InProgress. Callers are expected to have
// already validated ownership/current status via GetRideForUpdate in the
// same transaction, so this is an unconditional write (same pattern as
// CancelRide).
func (r *PostgresRideRepository) MarkRideStarted(ctx context.Context, id string, startedAt time.Time) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		UPDATE ride.ride
		SET status = 'InProgress', started_at = $2
		WHERE id = $1
	`, id, startedAt)
	return err
}

// CompleteRide flips a ride to Completed. Same unconditional-write pattern
// as MarkRideStarted/CancelRide.
func (r *PostgresRideRepository) CompleteRide(ctx context.Context, id string, finishedAt time.Time) error {
	executor := Executor(ctx, r.db)
	_, err := executor.ExecContext(ctx, `
		UPDATE ride.ride
		SET status = 'Completed', finished_at = $2
		WHERE id = $1
	`, id, finishedAt)
	return err
}

// scanRide reads one ride row, normalizing created_at to RFC3339 and the
// nullable driver_id column to a *string (nil until a ride is matched).
func scanRide(row interface{ Scan(dest ...any) error }) (*domain.Ride, error) {
	var d domain.Ride
	var driverID sql.NullString
	var billID sql.NullString
	var createdAt time.Time

	if err := row.Scan(
		&d.ID, &d.ClientID, &driverID, &d.Status,
		&d.PickupLat, &d.PickupLng, &d.PickupAddress,
		&d.DestLat, &d.DestLng, &d.DestAddress,
		&d.EstimatedPriceMinor, &d.Currency, &d.EstimatedDistanceKm, &billID, &createdAt,
	); err != nil {
		return nil, err
	}

	if driverID.Valid {
		d.DriverID = &driverID.String
	}
	if billID.Valid {
		d.BillID = &billID.String
	}
	d.CreatedAt = createdAt.Format(time.RFC3339)

	return &d, nil
}
