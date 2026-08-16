package cache

import (
	"context"
	"testing"
)

func TestSetOwnerAndDriverIDForUser_RoundTrips(t *testing.T) {
	ctx := context.Background()
	repo := NewOwnerRepository(testRedis)

	driverID := nextCacheID("driver")
	userID := nextCacheID("user")

	if err := repo.SetOwner(ctx, driverID, userID); err != nil {
		t.Fatalf("set owner: %v", err)
	}

	got, err := repo.DriverIDForUser(ctx, userID)
	if err != nil {
		t.Fatalf("driver id for user: %v", err)
	}
	if got != driverID {
		t.Fatalf("got driverID %q, want %q", got, driverID)
	}
}

func TestDriverIDForUser_MissingReturnsEmptyNoError(t *testing.T) {
	ctx := context.Background()
	repo := NewOwnerRepository(testRedis)

	got, err := repo.DriverIDForUser(ctx, nextCacheID("never-had-a-shift"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "" {
		t.Fatalf("got %q, want empty string for an unknown user", got)
	}
}
