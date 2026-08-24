package query

import (
	"context"

	"location-service/internal/common/decorator"
	cmnerrors "location-service/internal/common/errors"
	"location-service/internal/domain"

	"github.com/sirupsen/logrus"
)

// GetCounterpartyPosition answers the non-WS one-shot fallback. Authorization
// is the tracking window itself (§9) — window closed means 403, regardless of history.
type GetCounterpartyPosition struct {
	RideID string
	// CallerUserID/CallerClientID are the Kong-injected identities; the
	// handler tries whichever is non-empty against the ride's participants.
	CallerUserID   string
	CallerClientID string
}

type CounterpartyPositionResult struct {
	Subject  domain.SubjectType
	Position domain.Position
}

type GetCounterpartyPositionHandler struct {
	tracking domain.TrackingRepository
	owner    domain.OwnerRepository
	drivers  domain.DriverLocationRepository
	clients  domain.ClientLocationRepository
}

func NewGetCounterpartyPositionHandler(
	tracking domain.TrackingRepository,
	owner domain.OwnerRepository,
	drivers domain.DriverLocationRepository,
	clients domain.ClientLocationRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[GetCounterpartyPosition, CounterpartyPositionResult] {
	if tracking == nil || owner == nil || drivers == nil || clients == nil {
		panic("nil repo")
	}
	handler := &GetCounterpartyPositionHandler{tracking: tracking, owner: owner, drivers: drivers, clients: clients}
	return decorator.ApplyQueryDecorators[GetCounterpartyPosition, CounterpartyPositionResult](handler, logger, metricsClient)
}

func (h *GetCounterpartyPositionHandler) Handle(ctx context.Context, q GetCounterpartyPosition) (CounterpartyPositionResult, error) {
	participants, ok, err := h.tracking.Participants(ctx, q.RideID)
	if err != nil {
		return CounterpartyPositionResult{}, err
	}
	if !ok {
		return CounterpartyPositionResult{}, cmnerrors.ErrForbidden
	}

	// Client caller: X-Client-Id is the client's own stable id, checked
	// directly — no owner-mapping indirection the way driver identity needs.
	if q.CallerClientID != "" && q.CallerClientID == participants.ClientID {
		pos, err := h.drivers.LastPosition(ctx, participants.DriverID)
		if err != nil {
			return CounterpartyPositionResult{}, err
		}
		if pos == nil {
			return CounterpartyPositionResult{}, cmnerrors.ErrNotFound
		}
		return CounterpartyPositionResult{Subject: domain.SubjectDriver, Position: *pos}, nil
	}

	// Driver caller: X-User-Id resolves to a driverId via the cached owner
	// mapping, same as AcceptRide's ownership check in matching-service.
	if q.CallerUserID != "" {
		driverID, err := h.owner.DriverIDForUser(ctx, q.CallerUserID)
		if err != nil {
			return CounterpartyPositionResult{}, err
		}
		if driverID != "" && driverID == participants.DriverID {
			pos, err := h.clients.LastPosition(ctx, participants.ClientID)
			if err != nil {
				return CounterpartyPositionResult{}, err
			}
			if pos == nil {
				return CounterpartyPositionResult{}, cmnerrors.ErrNotFound
			}
			return CounterpartyPositionResult{Subject: domain.SubjectClient, Position: *pos}, nil
		}
	}

	return CounterpartyPositionResult{}, cmnerrors.ErrForbidden
}
