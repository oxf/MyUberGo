package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	app "location-service/internal/application"
	"location-service/internal/application/query"
	cmnerrors "location-service/internal/common/errors"
	"location-service/internal/domain"

	"github.com/oxf/MyUber/common/kongheaders"
	"github.com/sirupsen/logrus"
)

type fakeCounterpartyQuery struct {
	result query.CounterpartyPositionResult
	err    error
}

func (f *fakeCounterpartyQuery) Handle(ctx context.Context, q query.GetCounterpartyPosition) (query.CounterpartyPositionResult, error) {
	return f.result, f.err
}

func newTestLocationHandler(q *fakeCounterpartyQuery) *LocationHandler {
	application := app.Application{
		Queries: app.Queries{GetCounterpartyPosition: q},
	}
	logger := logrus.NewEntry(logrus.New())
	return NewLocationHandler(application, logger)
}

type fakeLivePositionsQuery struct {
	result query.LivePositionsResult
	err    error
}

func (f *fakeLivePositionsQuery) Handle(ctx context.Context, q query.ListLivePositions) (query.LivePositionsResult, error) {
	return f.result, f.err
}

func newTestLocationHandlerForLivePositions(q *fakeLivePositionsQuery) *LocationHandler {
	application := app.Application{
		Queries: app.Queries{ListLivePositions: q},
	}
	logger := logrus.NewEntry(logrus.New())
	return NewLocationHandler(application, logger)
}

// mux is needed (not a bare handler call) so r.PathValue("rideId") resolves —
// that's populated by ServeMux's own pattern matching, not by the handler.
func newCounterpartyRouter(h *LocationHandler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/location/rides/{rideId}/counterparty", h.GetCounterparty)
	return mux
}

func TestGetCounterparty_MissingUserIDIsBadRequest(t *testing.T) {
	h := newTestLocationHandler(&fakeCounterpartyQuery{})
	req := httptest.NewRequest(http.MethodGet, "/api/location/rides/ride-1/counterparty", nil)
	rr := httptest.NewRecorder()

	newCounterpartyRouter(h).ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400", rr.Code)
	}
}

func TestGetCounterparty_ForbiddenMapsTo403(t *testing.T) {
	h := newTestLocationHandler(&fakeCounterpartyQuery{err: cmnerrors.ErrForbidden})
	req := httptest.NewRequest(http.MethodGet, "/api/location/rides/ride-1/counterparty", nil)
	req.Header.Set(kongheaders.HeaderUserID, "user-1")
	rr := httptest.NewRecorder()

	newCounterpartyRouter(h).ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("got status %d, want 403", rr.Code)
	}
}

func TestGetCounterparty_NotFoundMapsTo404(t *testing.T) {
	h := newTestLocationHandler(&fakeCounterpartyQuery{err: cmnerrors.ErrNotFound})
	req := httptest.NewRequest(http.MethodGet, "/api/location/rides/ride-1/counterparty", nil)
	req.Header.Set(kongheaders.HeaderUserID, "user-1")
	rr := httptest.NewRecorder()

	newCounterpartyRouter(h).ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("got status %d, want 404", rr.Code)
	}
}

func TestGetCounterparty_SuccessReturnsPosition(t *testing.T) {
	pos := domain.Position{Coordinate: domain.Coordinate{Lat: 34.7, Lon: 33.0}}
	h := newTestLocationHandler(&fakeCounterpartyQuery{
		result: query.CounterpartyPositionResult{Subject: domain.SubjectDriver, Position: pos},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/location/rides/ride-1/counterparty", nil)
	req.Header.Set(kongheaders.HeaderUserID, "user-1")
	req.Header.Set(kongheaders.HeaderClientID, "client-1")
	rr := httptest.NewRecorder()

	newCounterpartyRouter(h).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rr.Code, rr.Body.String())
	}

	var body struct {
		Subject string  `json:"subject"`
		Lat     float64 `json:"lat"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Subject != "driver" || body.Lat != 34.7 {
		t.Fatalf("got %+v, want subject=driver lat=34.7", body)
	}
}

func TestListLivePositions_ReturnsDriversAndClients(t *testing.T) {
	h := newTestLocationHandlerForLivePositions(&fakeLivePositionsQuery{
		result: query.LivePositionsResult{
			Drivers: []query.LiveDriverPosition{
				{DriverID: "driver-1", Position: domain.Position{Coordinate: domain.Coordinate{Lat: 1, Lon: 2}}},
			},
			Clients: []query.LiveClientPosition{
				{ClientID: "client-1", RideID: "ride-1", Position: domain.Position{Coordinate: domain.Coordinate{Lat: 3, Lon: 4}}},
			},
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/location/positions", nil)
	rr := httptest.NewRecorder()

	h.ListLivePositions(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200, body=%s", rr.Code, rr.Body.String())
	}

	var body struct {
		Drivers []struct {
			DriverId string  `json:"driverId"`
			Lat      float64 `json:"lat"`
		} `json:"drivers"`
		Clients []struct {
			ClientId string  `json:"clientId"`
			RideId   string  `json:"rideId"`
			Lat      float64 `json:"lat"`
		} `json:"clients"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Drivers) != 1 || body.Drivers[0].DriverId != "driver-1" || body.Drivers[0].Lat != 1 {
		t.Fatalf("got drivers %+v, want one driver-1 at lat=1", body.Drivers)
	}
	if len(body.Clients) != 1 || body.Clients[0].ClientId != "client-1" || body.Clients[0].RideId != "ride-1" || body.Clients[0].Lat != 3 {
		t.Fatalf("got clients %+v, want one client-1/ride-1 at lat=3", body.Clients)
	}
}

func TestListLivePositions_QueryErrorMapsTo500(t *testing.T) {
	h := newTestLocationHandlerForLivePositions(&fakeLivePositionsQuery{err: errors.New("boom")})
	req := httptest.NewRequest(http.MethodGet, "/api/location/positions", nil)
	rr := httptest.NewRecorder()

	h.ListLivePositions(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("got status %d, want 500", rr.Code)
	}
}
