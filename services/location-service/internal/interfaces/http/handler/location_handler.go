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

// GetCounterparty handles GET /api/location/rides/{rideId}/counterparty, the
// non-WS one-shot fallback. Full path: see gateway/kong.yml's strip_path:false note.
func (h *LocationHandler) GetCounterparty(w http.ResponseWriter, r *http.Request) {
	userID, ok := kongheaders.RequireUserID(w, r)
	if !ok {
		return
	}
	clientID, _ := kongheaders.ClientID(r)

	rideID := r.PathValue("rideId")
	if rideID == "" {
		httpresponse.WriteError(w, "rideId is required", http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.GetCounterpartyPosition.Handle(r.Context(), query.GetCounterpartyPosition{
		RideID:         rideID,
		CallerUserID:   userID,
		CallerClientID: clientID,
	})
	switch {
	case errors.Is(err, cmnerrors.ErrForbidden):
		httpresponse.WriteError(w, "not a participant of this ride's open tracking window", http.StatusForbidden)
		return
	case errors.Is(err, cmnerrors.ErrNotFound):
		httpresponse.WriteError(w, "counterparty has not reported a position yet", http.StatusNotFound)
		return
	case err != nil:
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.CounterpartyPositionResponse{
		Subject:    string(result.Subject),
		Lat:        result.Position.Coordinate.Lat,
		Lon:        result.Position.Coordinate.Lon,
		AccuracyM:  result.Position.AccuracyM,
		HeadingDeg: result.Position.HeadingDeg,
		SpeedMps:   result.Position.SpeedMps,
		DeviceTs:   result.Position.DeviceTs.UTC().Format(time.RFC3339),
		ServerTs:   result.Position.ServerTs.UTC().Format(time.RFC3339),
	})
}

// ListLivePositions handles GET /api/location/positions — the admin
// fleet-wide live-positions map snapshot. Admin-only at Kong (require_admin,
// see gateway/kong.yml); no caller-identity-dependent logic here, so unlike
// IngestBatch/GetCounterparty there's no kongheaders.RequireUserID call.
func (h *LocationHandler) ListLivePositions(w http.ResponseWriter, r *http.Request) {
	result, err := h.app.Queries.ListLivePositions.Handle(r.Context(), query.ListLivePositions{})
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	drivers := make([]contracts.LiveDriverPositionDto, 0, len(result.Drivers))
	for _, d := range result.Drivers {
		drivers = append(drivers, contracts.LiveDriverPositionDto{
			DriverId:   d.DriverID,
			Lat:        d.Position.Coordinate.Lat,
			Lon:        d.Position.Coordinate.Lon,
			HeadingDeg: d.Position.HeadingDeg,
			SpeedMps:   d.Position.SpeedMps,
			ServerTs:   d.Position.ServerTs.UTC().Format(time.RFC3339),
		})
	}
	clients := make([]contracts.LiveClientPositionDto, 0, len(result.Clients))
	for _, c := range result.Clients {
		clients = append(clients, contracts.LiveClientPositionDto{
			ClientId: c.ClientID,
			RideId:   c.RideID,
			Lat:      c.Position.Coordinate.Lat,
			Lon:      c.Position.Coordinate.Lon,
			ServerTs: c.Position.ServerTs.UTC().Format(time.RFC3339),
		})
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.LivePositionsResponse{Drivers: drivers, Clients: clients})
}
