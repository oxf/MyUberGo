package handler

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	app "location-service/internal/application"
	"location-service/internal/application/command"
	"location-service/internal/application/query"
	cmnerrors "location-service/internal/common/errors"

	"github.com/oxf/MyUber/common/httpresponse"
	"github.com/oxf/MyUber/common/kongheaders"
	contracts "github.com/oxf/MyUber/contracts/http"
	"github.com/sirupsen/logrus"
)

type LocationHandler struct {
	app    app.Application
	logger *logrus.Entry
}

func NewLocationHandler(app app.Application, logger *logrus.Entry) *LocationHandler {
	return &LocationHandler{app: app, logger: logger}
}

// IngestBatch handles POST /batch (client path /api/location/batch via Kong).
// No driverId in the body — identity comes from the Kong-injected X-User-Id.
func (h *LocationHandler) IngestBatch(w http.ResponseWriter, r *http.Request) {
	userID, ok := kongheaders.RequireUserID(w, r)
	if !ok {
		return
	}

	req, err := httpresponse.Decode[contracts.LocationBatchRequest](r)
	if err != nil {
		httpresponse.WriteError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if len(req.Pings) == 0 {
		httpresponse.WriteError(w, "pings must not be empty", http.StatusBadRequest)
		return
	}

	pings := make([]command.PingInput, 0, len(req.Pings))
	for _, p := range req.Pings {
		deviceTs, err := time.Parse(time.RFC3339, p.DeviceTs)
		if err != nil {
			httpresponse.WriteError(w, "invalid deviceTs, must be RFC3339", http.StatusBadRequest)
			return
		}
		pings = append(pings, command.PingInput{
			Lat: p.Lat, Lon: p.Lon, AccuracyM: p.AccuracyM,
			HeadingDeg: p.HeadingDeg, SpeedMps: p.SpeedMps, DeviceTs: deviceTs,
		})
	}

	result, err := h.app.Commands.IngestPings.Handle(r.Context(), command.IngestPings{UserID: userID, Pings: pings})
	switch {
	case errors.Is(err, cmnerrors.ErrForbidden):
		httpresponse.WriteError(w, "no driver associated with this account", http.StatusForbidden)
		return
	case err != nil:
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.LocationBatchResponse{
		Accepted: result.Accepted,
		Rejected: result.Rejected,
	})
}

// NearbyDrivers handles GET /internal/drivers/nearby — network-isolated, no
// Kong route, no user auth. Called by matching-service only.
func (h *LocationHandler) NearbyDrivers(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	lat, errLat := strconv.ParseFloat(q.Get("lat"), 64)
	lon, errLon := strconv.ParseFloat(q.Get("lon"), 64)
	radiusKm, errRadius := strconv.ParseFloat(q.Get("radiusKm"), 64)
	limit, errLimit := strconv.Atoi(q.Get("limit"))
	if errLat != nil || errLon != nil || errRadius != nil || errLimit != nil {
		httpresponse.WriteError(w, "lat, lon, radiusKm, and limit are required", http.StatusBadRequest)
		return
	}

	candidates, err := h.app.Queries.FindNearbyDrivers.Handle(r.Context(), query.FindNearbyDrivers{
		Lat: lat, Lon: lon, RadiusKm: radiusKm, Limit: limit,
	})
	switch {
	case errors.Is(err, cmnerrors.ErrInvalidInput):
		httpresponse.WriteError(w, "invalid lat/lon/radiusKm/limit", http.StatusBadRequest)
		return
	case err != nil:
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	dtos := make([]contracts.NearbyDriverDto, 0, len(candidates))
	for _, c := range candidates {
		dtos = append(dtos, contracts.NearbyDriverDto{DriverId: c.DriverID, DistanceM: c.DistanceM})
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.NearbyDriversResponse{Candidates: dtos})
}
