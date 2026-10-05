package persistence

import (
	"context"
	"database/sql"
	"errors"

	cmnerrors "location-service/internal/common/errors"
	"location-service/internal/domain"
)

type PostgresRideSummaryRepository struct {
	db *sql.DB
}

func NewPostgresRideSummaryRepository(db *sql.DB) *PostgresRideSummaryRepository {
	return &PostgresRideSummaryRepository{db: db}
}

// Insert is idempotent against ride.completed/ride.cancelled redelivery —
// ON CONFLICT (ride_id) DO NOTHING, the same "insert, don't pre-check"
// idiom as billing.invoice's UNIQUE(ride_id, type) guard (a pre-check would
// race a concurrent redelivery). inserted=false on a conflict — the caller
// uses this to skip re-publishing ride.summary.ready on redelivery.
func (r *PostgresRideSummaryRepository) Insert(ctx context.Context, summary domain.RideSummary) (bool, error) {
	executor := Executor(ctx, r.db)

	res, err := executor.ExecContext(ctx, `
		INSERT INTO location.ride_summary
		(ride_id, client_id, driver_id, started_at, ended_at, start_lat, start_lon, end_lat, end_lon,
		 polyline, distance_m, duration_s, point_count, source)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (ride_id) DO NOTHING
		`,
		summary.RideID, summary.ClientID, summary.DriverID, summary.StartedAt, summary.EndedAt,
		summary.Start.Lat, summary.Start.Lon, summary.End.Lat, summary.End.Lon,
		summary.Polyline, summary.DistanceM, summary.DurationS, summary.PointCount, string(summary.Source),
	)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (r *PostgresRideSummaryRepository) GetByRideID(ctx context.Context, rideID string) (domain.RideSummary, error) {
	executor := Executor(ctx, r.db)

	var s domain.RideSummary
	var source string
	err := executor.QueryRowContext(ctx, `
		SELECT ride_id, client_id, driver_id, started_at, ended_at, start_lat, start_lon, end_lat, end_lon,
		       polyline, distance_m, duration_s, point_count, source
		FROM location.ride_summary
		WHERE ride_id = $1
		`, rideID,
	).Scan(
		&s.RideID, &s.ClientID, &s.DriverID, &s.StartedAt, &s.EndedAt,
		&s.Start.Lat, &s.Start.Lon, &s.End.Lat, &s.End.Lon,
		&s.Polyline, &s.DistanceM, &s.DurationS, &s.PointCount, &source,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RideSummary{}, cmnerrors.ErrNotFound
	}
	if err != nil {
		return domain.RideSummary{}, err
	}
	s.Source = domain.SummarySource(source)
	return s, nil
}
