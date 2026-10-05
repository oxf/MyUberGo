package workers

import (
	"context"
	"time"

	"location-service/internal/common/decorator"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/metrics"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// archiveTracer: a distinct package-level tracer var from staleness_worker's
// tracer — two vars named tracer in the same package won't compile.
var archiveTracer = otel.Tracer("location-service/archive-worker")

// ArchiveWorker drains each active ride's loc:ride:{id}:track Redis Stream
// into the long-retention history store on a ticker. This tier is an
// audit/verification input, never a system of record (LOCATION_SPEC.md
// §2.4/§6.2): a failed archive is logged and retried next tick, never
// escalated — it must never affect ingest or a ride's own lifecycle.
type ArchiveWorker struct {
	tracking domain.TrackingRepository
	tracks   domain.RideTrackRepository
	history  domain.LocationHistoryRepository
	interval time.Duration
	batch    int
	logger   *logrus.Entry
	metrics  decorator.MetricsClient
}

func NewArchiveWorker(
	tracking domain.TrackingRepository,
	tracks domain.RideTrackRepository,
	history domain.LocationHistoryRepository,
	interval time.Duration,
	batch int,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) *ArchiveWorker {
	if tracking == nil || tracks == nil || history == nil {
		panic("nil repo")
	}
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}
	return &ArchiveWorker{
		tracking: tracking, tracks: tracks, history: history,
		interval: interval, batch: batch, logger: logger, metrics: metricsClient,
	}
}

func (w *ArchiveWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.logger.Info("archive worker started")

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("archive worker stopped")
			return
		case <-ticker.C:
			w.sweep(ctx)
		}
	}
}

func (w *ArchiveWorker) sweep(ctx context.Context) {
	ctx, span := archiveTracer.Start(ctx, "archive sweep")
	defer span.End()

	rideIDs, err := w.tracking.ActiveRideIDs(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		w.logger.WithError(err).Error("failed to list active rides")
		return
	}
	span.SetAttributes(attribute.Int("location.active_rides", len(rideIDs)))

	for _, rideID := range rideIDs {
		if err := w.archiveRide(ctx, rideID); err != nil {
			w.logger.WithError(err).WithField("ride_id", rideID).Warn("failed to archive ride track")
		}
	}
}

// archiveRide is best-effort and never surfaces past this worker: a ride
// whose window closed between listing and archiving (ok=false) is simply
// skipped, and a batch write failure is logged by sweep and retried whole
// next tick (LastArchivedID is only advanced on success).
func (w *ArchiveWorker) archiveRide(ctx context.Context, rideID string) error {
	participants, ok, err := w.tracking.Participants(ctx, rideID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	afterID, err := w.tracks.LastArchivedID(ctx, rideID)
	if err != nil {
		return err
	}

	entries, err := w.tracks.Range(ctx, rideID, afterID)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	if w.batch > 0 && len(entries) > w.batch {
		// Catch up over several ticks rather than one unbounded write.
		entries = entries[:w.batch]
	}

	history := make([]domain.HistoryEntry, 0, len(entries))
	for _, e := range entries {
		subjectID := participants.DriverID
		if e.Subject == domain.SubjectClient {
			subjectID = participants.ClientID
		}
		history = append(history, domain.HistoryEntry{
			SubjectID:   subjectID,
			SubjectType: e.Subject,
			RideID:      rideID,
			Position:    e.Position,
		})
	}

	if err := w.history.PutBatch(ctx, history); err != nil {
		return err
	}
	if err := w.tracks.SetLastArchivedID(ctx, rideID, entries[len(entries)-1].StreamID); err != nil {
		return err
	}

	// IncCounter once per archived entry, not once per ride — same
	// reasoning as StalenessWorker's eviction counter.
	for range history {
		w.metrics.IncCounter(ctx, "myubergo.location.archive_entries")
	}
	return nil
}
