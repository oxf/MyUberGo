package query

import (
	"context"
	"errors"
	"testing"
	"time"

	cmnerrors "location-service/internal/common/errors"
	"location-service/internal/domain"
)

type fakeTrackingForCounterparty struct {
	domain.TrackingRepository
	participants map[string]domain.Participants // rideID -> participants
}

func (f *fakeTrackingForCounterparty) Participants(ctx context.Context, rideID string) (domain.Participants, bool, error) {
	p, ok := f.participants[rideID]
	return p, ok, nil
}

type fakeOwnerForCounterparty struct {
	domain.OwnerRepository
	driverByUser map[string]string
}

func (f *fakeOwnerForCounterparty) DriverIDForUser(ctx context.Context, userID string) (string, error) {
	return f.driverByUser[userID], nil
}

type fakeDriversForCounterparty struct {
	domain.DriverLocationRepository
	positions map[string]*domain.Position // driverID -> position
}

func (f *fakeDriversForCounterparty) LastPosition(ctx context.Context, driverID string) (*domain.Position, error) {
	return f.positions[driverID], nil
}

type fakeClientsForCounterparty struct {
	domain.ClientLocationRepository
	positions map[string]*domain.Position // clientID -> position
}

func (f *fakeClientsForCounterparty) LastPosition(ctx context.Context, clientID string) (*domain.Position, error) {
	return f.positions[clientID], nil
}

func newTestHandler(participants map[string]domain.Participants, driverByUser map[string]string, driverPositions map[string]*domain.Position, clientPositions map[string]*domain.Position) *GetCounterpartyPositionHandler {
	return &GetCounterpartyPositionHandler{
		tracking: &fakeTrackingForCounterparty{participants: participants},
		owner:    &fakeOwnerForCounterparty{driverByUser: driverByUser},
		drivers:  &fakeDriversForCounterparty{positions: driverPositions},
		clients:  &fakeClientsForCounterparty{positions: clientPositions},
	}
}

func TestGetCounterpartyPositionHandler_WindowClosedIsForbidden(t *testing.T) {
	h := newTestHandler(map[string]domain.Participants{}, nil, nil, nil)

	_, err := h.Handle(context.Background(), GetCounterpartyPosition{RideID: "ride-1", CallerUserID: "user-1"})
	if !errors.Is(err, cmnerrors.ErrForbidden) {
		t.Fatalf("got err %v, want ErrForbidden", err)
	}
}

func TestGetCounterpartyPositionHandler_NeitherCallerIdMatchesIsForbidden(t *testing.T) {
	participants := map[string]domain.Participants{
		"ride-1": {RideID: "ride-1", ClientID: "client-1", DriverID: "driver-1", OpenedAt: time.Now()},
	}
	h := newTestHandler(participants, map[string]string{"user-9": "driver-9"}, nil, nil)

	_, err := h.Handle(context.Background(), GetCounterpartyPosition{RideID: "ride-1", CallerUserID: "user-9"})
	if !errors.Is(err, cmnerrors.ErrForbidden) {
		t.Fatalf("got err %v, want ErrForbidden", err)
	}
}

func TestGetCounterpartyPositionHandler_ClientCallerGetsDriverPosition(t *testing.T) {
	participants := map[string]domain.Participants{
		"ride-1": {RideID: "ride-1", ClientID: "client-1", DriverID: "driver-1", OpenedAt: time.Now()},
	}
	driverPos := &domain.Position{Coordinate: domain.Coordinate{Lat: 1, Lon: 1}}
	h := newTestHandler(participants, nil, map[string]*domain.Position{"driver-1": driverPos}, nil)

	result, err := h.Handle(context.Background(), GetCounterpartyPosition{RideID: "ride-1", CallerClientID: "client-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Subject != domain.SubjectDriver || result.Position != *driverPos {
		t.Fatalf("got %+v, want driver position %+v", result, *driverPos)
	}
}

func TestGetCounterpartyPositionHandler_DriverCallerGetsClientPosition(t *testing.T) {
	participants := map[string]domain.Participants{
		"ride-1": {RideID: "ride-1", ClientID: "client-1", DriverID: "driver-1", OpenedAt: time.Now()},
	}
	clientPos := &domain.Position{Coordinate: domain.Coordinate{Lat: 2, Lon: 2}}
	h := newTestHandler(participants, map[string]string{"user-1": "driver-1"}, nil, map[string]*domain.Position{"client-1": clientPos})

	result, err := h.Handle(context.Background(), GetCounterpartyPosition{RideID: "ride-1", CallerUserID: "user-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Subject != domain.SubjectClient || result.Position != *clientPos {
		t.Fatalf("got %+v, want client position %+v", result, *clientPos)
	}
}

func TestGetCounterpartyPositionHandler_CounterpartyNeverPingedIsNotFound(t *testing.T) {
	participants := map[string]domain.Participants{
		"ride-1": {RideID: "ride-1", ClientID: "client-1", DriverID: "driver-1", OpenedAt: time.Now()},
	}
	h := newTestHandler(participants, nil, nil, nil)

	_, err := h.Handle(context.Background(), GetCounterpartyPosition{RideID: "ride-1", CallerClientID: "client-1"})
	if !errors.Is(err, cmnerrors.ErrNotFound) {
		t.Fatalf("got err %v, want ErrNotFound", err)
	}
}
