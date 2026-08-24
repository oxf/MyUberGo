package query

import (
	"context"
	"testing"
	"time"

	"location-service/internal/domain"
)

type fakeDriversForLivePositions struct {
	domain.DriverLocationRepository
	positions []domain.DriverPosition
}

func (f *fakeDriversForLivePositions) AllPositions(ctx context.Context) ([]domain.DriverPosition, error) {
	return f.positions, nil
}

type fakeTrackingForLivePositions struct {
	domain.TrackingRepository
	activeRideIDs []string
	participants  map[string]domain.Participants // rideID -> participants; missing means ok=false
}

func (f *fakeTrackingForLivePositions) ActiveRideIDs(ctx context.Context) ([]string, error) {
	return f.activeRideIDs, nil
}

func (f *fakeTrackingForLivePositions) Participants(ctx context.Context, rideID string) (domain.Participants, bool, error) {
	p, ok := f.participants[rideID]
	return p, ok, nil
}

type fakeClientsForLivePositions struct {
	domain.ClientLocationRepository
	positions map[string]*domain.Position // clientID -> position
}

func (f *fakeClientsForLivePositions) LastPosition(ctx context.Context, clientID string) (*domain.Position, error) {
	return f.positions[clientID], nil
}

func newTestListLivePositionsHandler(
	driverPositions []domain.DriverPosition,
	activeRideIDs []string,
	participants map[string]domain.Participants,
	clientPositions map[string]*domain.Position,
) *ListLivePositionsHandler {
	return &ListLivePositionsHandler{
		drivers:  &fakeDriversForLivePositions{positions: driverPositions},
		tracking: &fakeTrackingForLivePositions{activeRideIDs: activeRideIDs, participants: participants},
		clients:  &fakeClientsForLivePositions{positions: clientPositions},
	}
}

func TestListLivePositionsHandler_EmptyFleetReturnsEmptySlicesNotNil(t *testing.T) {
	h := newTestListLivePositionsHandler(nil, nil, nil, nil)

	result, err := h.Handle(context.Background(), ListLivePositions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Drivers == nil || len(result.Drivers) != 0 {
		t.Fatalf("got drivers %+v, want empty non-nil slice", result.Drivers)
	}
	if result.Clients == nil || len(result.Clients) != 0 {
		t.Fatalf("got clients %+v, want empty non-nil slice", result.Clients)
	}
}

func TestListLivePositionsHandler_DriversWithNoActiveRides(t *testing.T) {
	driverPos := domain.Position{Coordinate: domain.Coordinate{Lat: 1, Lon: 1}}
	h := newTestListLivePositionsHandler(
		[]domain.DriverPosition{{DriverID: "driver-1", Position: driverPos}},
		nil, nil, nil,
	)

	result, err := h.Handle(context.Background(), ListLivePositions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Drivers) != 1 || result.Drivers[0].DriverID != "driver-1" || result.Drivers[0].Position != driverPos {
		t.Fatalf("got %+v, want one driver-1 position", result.Drivers)
	}
	if len(result.Clients) != 0 {
		t.Fatalf("got clients %+v, want none", result.Clients)
	}
}

func TestListLivePositionsHandler_ActiveRideClientNotYetPingedIsExcluded(t *testing.T) {
	participants := map[string]domain.Participants{
		"ride-1": {RideID: "ride-1", ClientID: "client-1", DriverID: "driver-1", OpenedAt: time.Now()},
	}
	h := newTestListLivePositionsHandler(nil, []string{"ride-1"}, participants, nil)

	result, err := h.Handle(context.Background(), ListLivePositions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Clients) != 0 {
		t.Fatalf("got %+v, want no clients (never pinged)", result.Clients)
	}
}

func TestListLivePositionsHandler_ActiveRideClientPositionIsIncludedWithRideID(t *testing.T) {
	participants := map[string]domain.Participants{
		"ride-1": {RideID: "ride-1", ClientID: "client-1", DriverID: "driver-1", OpenedAt: time.Now()},
	}
	clientPos := domain.Position{Coordinate: domain.Coordinate{Lat: 2, Lon: 2}}
	h := newTestListLivePositionsHandler(nil, []string{"ride-1"}, participants, map[string]*domain.Position{"client-1": &clientPos})

	result, err := h.Handle(context.Background(), ListLivePositions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Clients) != 1 {
		t.Fatalf("got %+v, want one client", result.Clients)
	}
	got := result.Clients[0]
	if got.ClientID != "client-1" || got.RideID != "ride-1" || got.Position != clientPos {
		t.Fatalf("got %+v, want clientID=client-1 rideID=ride-1 position=%+v", got, clientPos)
	}
}

func TestListLivePositionsHandler_StaleActiveRideIDIsSkipped(t *testing.T) {
	// ride-1 is in ActiveRideIDs but its window has since closed (Participants
	// returns ok=false) — simulates the gap between SMEMBERS and CloseWindow.
	h := newTestListLivePositionsHandler(nil, []string{"ride-1"}, map[string]domain.Participants{}, nil)

	result, err := h.Handle(context.Background(), ListLivePositions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Clients) != 0 {
		t.Fatalf("got %+v, want no clients (stale index entry silently skipped)", result.Clients)
	}
}
