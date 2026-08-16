package command

import (
	"context"
	"errors"
	"testing"
	"time"

	cmnerrors "location-service/internal/common/errors"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/metrics"
)

type fakeOwner struct {
	domain.OwnerRepository
	driverByUser map[string]string
}

func (f *fakeOwner) DriverIDForUser(ctx context.Context, userID string) (string, error) {
	return f.driverByUser[userID], nil
}

type fakeDrivers struct {
	domain.DriverLocationRepository
	last     *domain.Position
	upserted []domain.Position
}

func (f *fakeDrivers) LastPosition(ctx context.Context, driverID string) (*domain.Position, error) {
	return f.last, nil
}

func (f *fakeDrivers) UpsertPosition(ctx context.Context, driverID string, pos domain.Position) error {
	f.upserted = append(f.upserted, pos)
	return nil
}

func testCfg() domain.ValidationConfig {
	return domain.ValidationConfig{
		MaxAccuracyM:  100,
		MaxSpeedKmh:   200,
		MaxFutureSkew: 2 * time.Minute,
		MaxPastSkew:   10 * time.Minute,
	}
}

func TestIngestPingsHandler_UnknownCallerIsForbidden(t *testing.T) {
	h := &IngestPingsHandler{
		owner:   &fakeOwner{driverByUser: map[string]string{}},
		drivers: &fakeDrivers{},
		config:  testCfg(),
		metrics: metrics.NewNoopMetricsClient(),
	}

	_, err := h.Handle(context.Background(), IngestPings{UserID: "user-1", Pings: []PingInput{{Lat: 1, Lon: 1, DeviceTs: time.Now()}}})
	if !errors.Is(err, cmnerrors.ErrForbidden) {
		t.Fatalf("got err %v, want ErrForbidden", err)
	}
}

func TestIngestPingsHandler_AcceptsValidBatchAndWritesOnlyLastPosition(t *testing.T) {
	now := time.Now().UTC()
	drivers := &fakeDrivers{}
	h := &IngestPingsHandler{
		owner:   &fakeOwner{driverByUser: map[string]string{"user-1": "driver-1"}},
		drivers: drivers,
		config:  testCfg(),
		metrics: metrics.NewNoopMetricsClient(),
	}

	result, err := h.Handle(context.Background(), IngestPings{
		UserID: "user-1",
		Pings: []PingInput{
			{Lat: 34.707, Lon: 33.022, AccuracyM: 10, DeviceTs: now},
			{Lat: 34.7076, Lon: 33.022, AccuracyM: 10, DeviceTs: now.Add(5 * time.Second)},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Accepted != 2 || result.Rejected != 0 {
		t.Fatalf("got %+v, want 2 accepted, 0 rejected", result)
	}
	if len(drivers.upserted) != 1 {
		t.Fatalf("got %d Redis writes, want exactly 1 (only the last accepted position)", len(drivers.upserted))
	}
	if drivers.upserted[0].Coordinate.Lat != 34.7076 {
		t.Fatalf("upserted position is not the last accepted one: %+v", drivers.upserted[0])
	}
}

func TestIngestPingsHandler_RejectsOutOfOrderBatchEntries(t *testing.T) {
	now := time.Now().UTC()
	drivers := &fakeDrivers{}
	h := &IngestPingsHandler{
		owner:   &fakeOwner{driverByUser: map[string]string{"user-1": "driver-1"}},
		drivers: drivers,
		config:  testCfg(),
		metrics: metrics.NewNoopMetricsClient(),
	}

	// Pings submitted out of order — handler must sort by DeviceTs before
	// validating, so both are accepted rather than the second looking stale.
	result, err := h.Handle(context.Background(), IngestPings{
		UserID: "user-1",
		Pings: []PingInput{
			{Lat: 34.7076, Lon: 33.022, AccuracyM: 10, DeviceTs: now.Add(5 * time.Second)},
			{Lat: 34.707, Lon: 33.022, AccuracyM: 10, DeviceTs: now},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Accepted != 2 {
		t.Fatalf("got %d accepted, want 2 (reordered before validation)", result.Accepted)
	}
}

func TestIngestPingsHandler_RejectsTeleportWithinSameBatch(t *testing.T) {
	now := time.Now().UTC()
	drivers := &fakeDrivers{}
	h := &IngestPingsHandler{
		owner:   &fakeOwner{driverByUser: map[string]string{"user-1": "driver-1"}},
		drivers: drivers,
		config:  testCfg(),
		metrics: metrics.NewNoopMetricsClient(),
	}

	result, err := h.Handle(context.Background(), IngestPings{
		UserID: "user-1",
		Pings: []PingInput{
			{Lat: 34.707, Lon: 33.022, AccuracyM: 10, DeviceTs: now},
			// ~1.1km away 5s later implies ~800km/h — rejected against the
			// *first* ping in this same batch, not just a Redis-stored value.
			{Lat: 34.717, Lon: 33.022, AccuracyM: 10, DeviceTs: now.Add(5 * time.Second)},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Accepted != 1 || result.Rejected != 1 {
		t.Fatalf("got %+v, want 1 accepted, 1 rejected", result)
	}
	if len(drivers.upserted) != 1 || drivers.upserted[0].DeviceTs != now {
		t.Fatalf("expected only the first ping to be upserted, got %+v", drivers.upserted)
	}
}

func TestIngestPingsHandler_AllRejectedWritesNothing(t *testing.T) {
	drivers := &fakeDrivers{}
	h := &IngestPingsHandler{
		owner:   &fakeOwner{driverByUser: map[string]string{"user-1": "driver-1"}},
		drivers: drivers,
		config:  testCfg(),
		metrics: metrics.NewNoopMetricsClient(),
	}

	result, err := h.Handle(context.Background(), IngestPings{
		UserID: "user-1",
		Pings:  []PingInput{{Lat: 999, Lon: 33.022, DeviceTs: time.Now()}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Accepted != 0 || result.Rejected != 1 {
		t.Fatalf("got %+v, want 0 accepted, 1 rejected", result)
	}
	if len(drivers.upserted) != 0 {
		t.Fatalf("expected no Redis write when every ping is rejected, got %d", len(drivers.upserted))
	}
}
