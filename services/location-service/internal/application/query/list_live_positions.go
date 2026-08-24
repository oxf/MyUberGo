package query

import (
	"context"

	"location-service/internal/common/decorator"
	"location-service/internal/domain"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

// ListLivePositions answers the admin fleet-wide live-positions map — no
// authorization inside the handler, the sole gate is Kong's require_admin
// plugin, the same trust model FindNearbyDrivers uses for its own
// internal-only trust boundary.
type ListLivePositions struct{}

// LiveDriverPosition is one driver's current position.
type LiveDriverPosition struct {
	DriverID string
	Position domain.Position
}

// LiveClientPosition is one client's current position — only present while
// their ride's tracking window is open (LOCATION_SPEC.md §17 decision 5),
// hence the accompanying RideID.
type LiveClientPosition struct {
	ClientID string
	RideID   string
	Position domain.Position
}

type LivePositionsResult struct {
	Drivers []LiveDriverPosition
	Clients []LiveClientPosition
}

type ListLivePositionsHandler struct {
	drivers  domain.DriverLocationRepository
	clients  domain.ClientLocationRepository
	tracking domain.TrackingRepository
	metrics  decorator.MetricsClient
}

func NewListLivePositionsHandler(
	drivers domain.DriverLocationRepository,
	clients domain.ClientLocationRepository,
	tracking domain.TrackingRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[ListLivePositions, LivePositionsResult] {
	if drivers == nil || clients == nil || tracking == nil {
		panic("nil repo")
	}
	handler := &ListLivePositionsHandler{drivers: drivers, clients: clients, tracking: tracking, metrics: metricsClient}
	return decorator.ApplyQueryDecorators[ListLivePositions, LivePositionsResult](handler, logger, metricsClient)
}

func (h *ListLivePositionsHandler) Handle(ctx context.Context, _ ListLivePositions) (LivePositionsResult, error) {
	driverPositions, err := h.drivers.AllPositions(ctx)
	if err != nil {
		return LivePositionsResult{}, err
	}
	drivers := make([]LiveDriverPosition, 0, len(driverPositions))
	for _, dp := range driverPositions {
		drivers = append(drivers, LiveDriverPosition{DriverID: dp.DriverID, Position: dp.Position})
	}

	rideIDs, err := h.tracking.ActiveRideIDs(ctx)
	if err != nil {
		return LivePositionsResult{}, err
	}
	clients := make([]LiveClientPosition, 0, len(rideIDs))
	for _, rideID := range rideIDs {
		participants, ok, err := h.tracking.Participants(ctx, rideID)
		if err != nil {
			return LivePositionsResult{}, err
		}
		if !ok {
			continue // stale index entry — window closed since ActiveRideIDs was read
		}
		pos, err := h.clients.LastPosition(ctx, participants.ClientID)
		if err != nil {
			return LivePositionsResult{}, err
		}
		if pos == nil {
			continue // client paired but hasn't pinged yet
		}
		clients = append(clients, LiveClientPosition{ClientID: participants.ClientID, RideID: rideID, Position: *pos})
	}

	if h.metrics != nil {
		h.metrics.RecordValue(ctx, "myubergo.location.live_positions_returned", float64(len(drivers)), attribute.String("subject", "driver"))
		h.metrics.RecordValue(ctx, "myubergo.location.live_positions_returned", float64(len(clients)), attribute.String("subject", "client"))
	}
	return LivePositionsResult{Drivers: drivers, Clients: clients}, nil
}
