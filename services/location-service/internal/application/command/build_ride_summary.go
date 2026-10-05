package command

import (
	"context"
	"encoding/json"
	"time"

	"location-service/internal/application/services"
	"location-service/internal/common/decorator"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/metrics"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

// BuildRideSummary builds and persists a ride's route summary from its
// archived Redis Stream track, then publishes ride.summary.ready via the
// outbox. Run once per ride at ride end (LOCATION_SPEC.md §8.1), before
// CloseTrackingWindow — CloseWindow deletes the participants hash this
// command reads OpenedAt/ClientID/DriverID from.
type BuildRideSummary struct {
	RideID  string
	EndedAt time.Time
}

type BuildRideSummaryHandler struct {
	tracking    domain.TrackingRepository
	tracks      domain.RideTrackRepository
	summaries   domain.RideSummaryRepository
	outbox      domain.OutboxRepository
	transaction services.TransactionManager
	// mapMatcher is nilable: the Slice-4 Geoapify adapter doesn't exist
	// yet, so every summary today takes the Simplified fallback path.
	mapMatcher domain.MapMatchingProvider
	epsilonM   float64
	logger     *logrus.Entry
	metrics    decorator.MetricsClient
}

func NewBuildRideSummaryHandler(
	tracking domain.TrackingRepository,
	tracks domain.RideTrackRepository,
	summaries domain.RideSummaryRepository,
	outboxRepo domain.OutboxRepository,
	transaction services.TransactionManager,
	mapMatcher domain.MapMatchingProvider,
	epsilonM float64,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[BuildRideSummary] {
	if tracking == nil || tracks == nil || summaries == nil || outboxRepo == nil || transaction == nil {
		panic("nil repo")
	}
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}
	handler := &BuildRideSummaryHandler{
		tracking: tracking, tracks: tracks, summaries: summaries, outbox: outboxRepo,
		transaction: transaction, mapMatcher: mapMatcher, epsilonM: epsilonM,
		logger: logger, metrics: metricsClient,
	}
	return decorator.ApplyCommandDecoratorsNoResult[BuildRideSummary](handler, logger, metricsClient)
}

func (h *BuildRideSummaryHandler) Handle(ctx context.Context, cmd BuildRideSummary) error {
	participants, ok, err := h.tracking.Participants(ctx, cmd.RideID)
	if err != nil {
		return err
	}
	if !ok {
		// No open tracking window — nothing to summarize. Defensive no-op
		// rather than an error, so this handler stays safe to call
		// unconditionally from either consumer.
		return nil
	}

	entries, err := h.tracks.Range(ctx, cmd.RideID, "")
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		h.logger.WithField("ride_id", cmd.RideID).Info("no track entries recorded for ride, skipping summary")
		return nil
	}

	positions := driverPositions(entries)

	summary, err := h.buildSummary(ctx, cmd.RideID, participants, cmd.EndedAt, positions)
	if err != nil {
		return err
	}

	return h.persist(ctx, summary)
}

// driverPositions prefers the driver's own track — the ride's actual
// route — over the rider's phone, which samples independently and would
// introduce noise into the polyline. Falls back to whatever was recorded if
// the driver never pinged during this ride.
func driverPositions(entries []domain.TrackEntry) []domain.Position {
	positions := make([]domain.Position, 0, len(entries))
	for _, e := range entries {
		if e.Subject == domain.SubjectDriver {
			positions = append(positions, e.Position)
		}
	}
	if len(positions) > 0 {
		return positions
	}
	for _, e := range entries {
		positions = append(positions, e.Position)
	}
	return positions
}

func (h *BuildRideSummaryHandler) buildSummary(
	ctx context.Context, rideID string, participants domain.Participants, endedAt time.Time, positions []domain.Position,
) (domain.RideSummary, error) {
	if h.mapMatcher != nil {
		coords := make([]domain.Coordinate, len(positions))
		for i, p := range positions {
			coords[i] = p.Coordinate
		}
		polyline, distanceM, err := h.mapMatcher.MatchRoute(ctx, coords)
		if err == nil {
			return domain.RideSummary{
				RideID: rideID, ClientID: participants.ClientID, DriverID: participants.DriverID,
				StartedAt: participants.OpenedAt, EndedAt: endedAt,
				Start: positions[0].Coordinate, End: positions[len(positions)-1].Coordinate,
				Polyline: polyline, DistanceM: distanceM,
				DurationS:  int(endedAt.Sub(participants.OpenedAt).Seconds()),
				PointCount: len(positions), Source: domain.SourceMapMatched,
			}, nil
		}
		h.logger.WithError(err).WithField("ride_id", rideID).Warn("map matching failed, falling back to simplified summary")
	}

	return domain.BuildSummaryFallback(rideID, participants.ClientID, participants.DriverID, participants.OpenedAt, endedAt, positions, h.epsilonM)
}

// persist inserts the summary and its outbox row in one transaction.
// ON CONFLICT (ride_id) DO NOTHING never aborts the transaction (unlike a
// raw unique-violation), so — unlike billing's CreateInvoiceFromRideHandler
// — no post-WithinTransaction error translation is needed here: inserted
// tells us directly whether this call actually created the row, which gates
// the outbox insert so a redelivery never republishes ride.summary.ready.
func (h *BuildRideSummaryHandler) persist(ctx context.Context, summary domain.RideSummary) error {
	var inserted bool
	err := h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		var err error
		inserted, err = h.summaries.Insert(ctx, summary)
		if err != nil || !inserted {
			return err
		}

		event := contractsKafka.RideSummaryReadyEvent{
			RideID: summary.RideID, ClientID: summary.ClientID, DriverID: summary.DriverID,
			DistanceM: summary.DistanceM, DurationS: summary.DurationS, Polyline: summary.Polyline,
			StartedAt: summary.StartedAt.UTC().Format(time.RFC3339),
			EndedAt:   summary.EndedAt.UTC().Format(time.RFC3339),
			Source:    string(summary.Source),
		}
		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}
		return h.outbox.Insert(ctx, &domain.OutboxMessage{
			Topic: "ride.summary.ready", EventType: "RideSummaryReady", Payload: payload,
		})
	})
	if err != nil {
		return err
	}

	if inserted {
		h.metrics.IncCounter(ctx, "myubergo.location.summaries_built", attribute.String("source", string(summary.Source)))
	}
	return nil
}
