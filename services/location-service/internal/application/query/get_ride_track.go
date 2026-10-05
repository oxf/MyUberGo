package query

import (
	"context"

	"location-service/internal/common/decorator"
	cmnerrors "location-service/internal/common/errors"
	"location-service/internal/domain"

	"github.com/sirupsen/logrus"
)

// GetRideTrack answers the historical-track read (LOCATION_SPEC.md §7.1).
// Unlike GetCounterpartyPosition, this deliberately does NOT use the
// tracking window as its authorization scope — the window is closed by the
// time anyone reads a past ride's summary. Authorized instead against the
// persisted summary's own client_id/driver_id, the same pattern
// matching-service uses for driver ownership and billing-service uses for
// invoice ownership.
type GetRideTrack struct {
	RideID         string
	CallerUserID   string
	CallerClientID string
}

type GetRideTrackHandler struct {
	summaries domain.RideSummaryRepository
	owner     domain.OwnerRepository
}

func NewGetRideTrackHandler(
	summaries domain.RideSummaryRepository,
	owner domain.OwnerRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[GetRideTrack, domain.RideSummary] {
	if summaries == nil || owner == nil {
		panic("nil repo")
	}
	handler := &GetRideTrackHandler{summaries: summaries, owner: owner}
	return decorator.ApplyQueryDecorators[GetRideTrack, domain.RideSummary](handler, logger, metricsClient)
}

func (h *GetRideTrackHandler) Handle(ctx context.Context, q GetRideTrack) (domain.RideSummary, error) {
	summary, err := h.summaries.GetByRideID(ctx, q.RideID)
	if err != nil {
		return domain.RideSummary{}, err
	}

	if q.CallerClientID != "" && q.CallerClientID == summary.ClientID {
		return summary, nil
	}

	if q.CallerUserID != "" {
		driverID, err := h.owner.DriverIDForUser(ctx, q.CallerUserID)
		if err != nil {
			return domain.RideSummary{}, err
		}
		if driverID != "" && driverID == summary.DriverID {
			return summary, nil
		}
	}

	return domain.RideSummary{}, cmnerrors.ErrForbidden
}
