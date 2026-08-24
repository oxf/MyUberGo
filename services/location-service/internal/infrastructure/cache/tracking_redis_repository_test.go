package cache

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestTrackingRepository_RequestedThenAccepted_CompletesWindow(t *testing.T) {
	ctx := context.Background()
	repo := NewTrackingRepository(testRedis)

	rideID := nextCacheID("ride")
	clientID := nextCacheID("client")
	driverID := nextCacheID("driver")
	acceptedAt := time.Now().UTC().Truncate(time.Second)

	if err := repo.RecordRideRequested(ctx, rideID, clientID); err != nil {
		t.Fatalf("record ride requested: %v", err)
	}

	participants, ok, err := repo.Participants(ctx, rideID)
	if err != nil {
		t.Fatalf("participants: %v", err)
	}
	if ok {
		t.Fatalf("window should not be open yet, got participants %+v", participants)
	}

	if err := repo.RecordRideAccepted(ctx, rideID, driverID, acceptedAt); err != nil {
		t.Fatalf("record ride accepted: %v", err)
	}

	participants, ok, err = repo.Participants(ctx, rideID)
	if err != nil {
		t.Fatalf("participants: %v", err)
	}
	if !ok {
		t.Fatal("expected window to be open after both events observed")
	}
	if participants.ClientID != clientID || participants.DriverID != driverID {
		t.Fatalf("got participants %+v, want clientID=%q driverID=%q", participants, clientID, driverID)
	}
	if !participants.OpenedAt.Equal(acceptedAt) {
		t.Fatalf("got openedAt %v, want %v", participants.OpenedAt, acceptedAt)
	}

	assertActiveRide(t, ctx, repo, driverID, clientID, rideID)
}

func TestTrackingRepository_AcceptedThenRequested_CompletesWindow(t *testing.T) {
	ctx := context.Background()
	repo := NewTrackingRepository(testRedis)

	rideID := nextCacheID("ride")
	clientID := nextCacheID("client")
	driverID := nextCacheID("driver")
	acceptedAt := time.Now().UTC().Truncate(time.Second)

	if err := repo.RecordRideAccepted(ctx, rideID, driverID, acceptedAt); err != nil {
		t.Fatalf("record ride accepted: %v", err)
	}

	_, ok, err := repo.Participants(ctx, rideID)
	if err != nil {
		t.Fatalf("participants: %v", err)
	}
	if ok {
		t.Fatal("window should not be open yet")
	}

	if err := repo.RecordRideRequested(ctx, rideID, clientID); err != nil {
		t.Fatalf("record ride requested: %v", err)
	}

	participants, ok, err := repo.Participants(ctx, rideID)
	if err != nil {
		t.Fatalf("participants: %v", err)
	}
	if !ok {
		t.Fatal("expected window to be open regardless of arrival order")
	}
	if participants.ClientID != clientID || participants.DriverID != driverID {
		t.Fatalf("got participants %+v, want clientID=%q driverID=%q", participants, clientID, driverID)
	}

	assertActiveRide(t, ctx, repo, driverID, clientID, rideID)
}

func TestTrackingRepository_CloseWindow_ClearsParticipantsAndActiveRideKeys(t *testing.T) {
	ctx := context.Background()
	repo := NewTrackingRepository(testRedis)

	rideID := nextCacheID("ride")
	clientID := nextCacheID("client")
	driverID := nextCacheID("driver")

	if err := repo.RecordRideRequested(ctx, rideID, clientID); err != nil {
		t.Fatalf("record ride requested: %v", err)
	}
	if err := repo.RecordRideAccepted(ctx, rideID, driverID, time.Now().UTC()); err != nil {
		t.Fatalf("record ride accepted: %v", err)
	}

	if err := repo.CloseWindow(ctx, rideID); err != nil {
		t.Fatalf("close window: %v", err)
	}

	if _, ok, err := repo.Participants(ctx, rideID); err != nil {
		t.Fatalf("participants: %v", err)
	} else if ok {
		t.Fatal("expected window to be closed")
	}

	if got, err := repo.ActiveRideForDriver(ctx, driverID); err != nil {
		t.Fatalf("active ride for driver: %v", err)
	} else if got != "" {
		t.Fatalf("expected no active ride for driver after close, got %q", got)
	}
	if got, err := repo.ActiveRideForClient(ctx, clientID); err != nil {
		t.Fatalf("active ride for client: %v", err)
	} else if got != "" {
		t.Fatalf("expected no active ride for client after close, got %q", got)
	}
}

func TestTrackingRepository_CloseWindow_NeverOpenedIsNotAnError(t *testing.T) {
	ctx := context.Background()
	repo := NewTrackingRepository(testRedis)

	if err := repo.CloseWindow(ctx, nextCacheID("ride")); err != nil {
		t.Fatalf("expected close of a never-opened window to succeed, got %v", err)
	}
}

func TestTrackingRepository_ActiveRideForDriverOrClient_MissingReturnsEmptyNoError(t *testing.T) {
	ctx := context.Background()
	repo := NewTrackingRepository(testRedis)

	if got, err := repo.ActiveRideForDriver(ctx, nextCacheID("never-matched-driver")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if got != "" {
		t.Fatalf("got %q, want empty string", got)
	}
	if got, err := repo.ActiveRideForClient(ctx, nextCacheID("never-matched-client")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if got != "" {
		t.Fatalf("got %q, want empty string", got)
	}
}

func TestTrackingRepository_ActiveRideIDs_ReflectsOpenAndClosedWindows(t *testing.T) {
	ctx := context.Background()
	repo := NewTrackingRepository(testRedis)

	ride1 := nextCacheID("ride")
	ride2 := nextCacheID("ride")

	if err := repo.RecordRideRequested(ctx, ride1, nextCacheID("client")); err != nil {
		t.Fatalf("record ride1 requested: %v", err)
	}
	if err := repo.RecordRideAccepted(ctx, ride1, nextCacheID("driver"), time.Now().UTC()); err != nil {
		t.Fatalf("record ride1 accepted: %v", err)
	}
	if err := repo.RecordRideRequested(ctx, ride2, nextCacheID("client")); err != nil {
		t.Fatalf("record ride2 requested: %v", err)
	}
	if err := repo.RecordRideAccepted(ctx, ride2, nextCacheID("driver"), time.Now().UTC()); err != nil {
		t.Fatalf("record ride2 accepted: %v", err)
	}

	active, err := repo.ActiveRideIDs(ctx)
	if err != nil {
		t.Fatalf("active ride ids: %v", err)
	}
	if !slices.Contains(active, ride1) || !slices.Contains(active, ride2) {
		t.Fatalf("got %v, want both %q and %q present", active, ride1, ride2)
	}

	if err := repo.CloseWindow(ctx, ride1); err != nil {
		t.Fatalf("close ride1: %v", err)
	}

	active, err = repo.ActiveRideIDs(ctx)
	if err != nil {
		t.Fatalf("active ride ids after close: %v", err)
	}
	if slices.Contains(active, ride1) {
		t.Fatalf("expected %q to be removed from active ride ids after CloseWindow, got %v", ride1, active)
	}
	if !slices.Contains(active, ride2) {
		t.Fatalf("expected %q to remain in active ride ids, got %v", ride2, active)
	}
}

func assertActiveRide(t *testing.T, ctx context.Context, repo *TrackingRepository, driverID, clientID, wantRideID string) {
	t.Helper()

	if got, err := repo.ActiveRideForDriver(ctx, driverID); err != nil {
		t.Fatalf("active ride for driver: %v", err)
	} else if got != wantRideID {
		t.Fatalf("got active ride %q for driver, want %q", got, wantRideID)
	}
	if got, err := repo.ActiveRideForClient(ctx, clientID); err != nil {
		t.Fatalf("active ride for client: %v", err)
	} else if got != wantRideID {
		t.Fatalf("got active ride %q for client, want %q", got, wantRideID)
	}
}
