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

// Gives each sweep its own span so the StaleDriverIDs/Evict round trips join
// one trace instead of each becoming a disconnected root.
var tracer = otel.Tracer("location-service/staleness-worker")

// StalenessWorker evicts drivers who've stopped pinging from the geo index.
// Redis GEO has no per-member TTL, so without this a force-killed app's driver stays forever.
type StalenessWorker struct {
	drivers   domain.DriverLocationRepository
	staleness time.Duration
	interval  time.Duration
	logger    *logrus.Entry
	metrics   decorator.MetricsClient
}

func NewStalenessWorker(
	drivers domain.DriverLocationRepository,
	staleness time.Duration,
	interval time.Duration,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) *StalenessWorker {
	if drivers == nil {
		panic("nil repo")
	}
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}
	return &StalenessWorker{drivers: drivers, staleness: staleness, interval: interval, logger: logger, metrics: metricsClient}
}

func (w *StalenessWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.logger.Info("staleness worker started")

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("staleness worker stopped")
			return
		case <-ticker.C:
			w.sweep(ctx)
		}
	}
}

func (w *StalenessWorker) sweep(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "staleness sweep")
	defer span.End()

	cutoff := time.Now().Add(-w.staleness)
	staleIDs, err := w.drivers.StaleDriverIDs(ctx, cutoff)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		w.logger.WithError(err).Error("failed to list stale drivers")
		return
	}
	span.SetAttributes(attribute.Int("location.stale_count", len(staleIDs)))
	if len(staleIDs) == 0 {
		return
	}

	if err := w.drivers.Evict(ctx, staleIDs); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		w.logger.WithError(err).Error("failed to evict stale drivers")
		return
	}

	// IncCounter once per evicted driver, not once per sweep — a sweep-level
	// increment would understate this by up to len(staleIDs)×.
	for range staleIDs {
		w.metrics.IncCounter(ctx, "myubergo.location.staleness_evictions")
	}
	w.logger.WithField("count", len(staleIDs)).Info("evicted stale drivers from geo index")
}
