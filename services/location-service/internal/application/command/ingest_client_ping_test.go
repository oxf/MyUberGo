package command

import (
	"context"
	"errors"
	"testing"
	"time"

	cmnerrors "location-service/internal/common/errors"
	"location-service/internal/domain"
)

type fakeTrackingForIngest struct {
	domain.TrackingRepository
	activeRideByClient map[string]string
}

func (f *fakeTrackingForIngest) ActiveRideForClient(ctx context.Context, clientID string) (string, error) {
	return f.activeRideByClient[clientID], nil
}

type fakeClients struct {
	domain.ClientLocationRepository
	last     *domain.Position
	upserted []domain.Position
}

func (f *fakeClients) LastPosition(ctx context.Context, clientID string) (*domain.Position, error) {
	return f.last, nil
}

func (f *fakeClients) UpsertPosition(ctx context.Context, clientID string, pos domain.Position) error {
	f.upserted = append(f.upserted, pos)
	return nil
}

func TestIngestClientPingHandler_NoOpenWindowIsForbidden(t *testing.T) {
	h := &IngestClientPingHandler{
		tracking:  &fakeTrackingForIngest{activeRideByClient: map[string]string{}},
		clients:   &fakeClients{},
		config:    testCfg(),
		publisher: &fakePublisher{},
	}

	_, err := h.Handle(context.Background(), IngestClientPing{
		ClientID: "client-1",
		Ping:     PingInput{Lat: 1, Lon: 1, DeviceTs: time.Now()},
	})
	if !errors.Is(err, cmnerrors.ErrForbidden) {
		t.Fatalf("got err %v, want ErrForbidden", err)
	}
}

func TestIngestClientPingHandler_AcceptsValidPingWithinOpenWindow(t *testing.T) {
	clients := &fakeClients{}
	publisher := &fakePublisher{}
	h := &IngestClientPingHandler{
		tracking:  &fakeTrackingForIngest{activeRideByClient: map[string]string{"client-1": "ride-1"}},
		clients:   clients,
		config:    testCfg(),
		publisher: publisher,
	}

	result, err := h.Handle(context.Background(), IngestClientPing{
		ClientID: "client-1",
		Ping:     PingInput{Lat: 34.707, Lon: 33.022, AccuracyM: 10, DeviceTs: time.Now()},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Accepted != 1 || result.Rejected != 0 {
		t.Fatalf("got %+v, want 1 accepted", result)
	}
	if len(clients.upserted) != 1 {
		t.Fatalf("got %d writes, want 1", len(clients.upserted))
	}
	if len(publisher.published) != 1 || publisher.published[0].RideID != "ride-1" || publisher.published[0].Subject != domain.SubjectClient {
		t.Fatalf("got published %+v, want one update for ride-1/client", publisher.published)
	}
}

func TestIngestClientPingHandler_RejectsInvalidPingWithinOpenWindow(t *testing.T) {
	clients := &fakeClients{}
	h := &IngestClientPingHandler{
		tracking:  &fakeTrackingForIngest{activeRideByClient: map[string]string{"client-1": "ride-1"}},
		clients:   clients,
		config:    testCfg(),
		publisher: &fakePublisher{},
	}

	result, err := h.Handle(context.Background(), IngestClientPing{
		ClientID: "client-1",
		Ping:     PingInput{Lat: 999, Lon: 33.022, DeviceTs: time.Now()},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Accepted != 0 || result.Rejected != 1 {
		t.Fatalf("got %+v, want 1 rejected", result)
	}
	if len(clients.upserted) != 0 {
		t.Fatalf("expected no write for a rejected ping, got %d", len(clients.upserted))
	}
}
