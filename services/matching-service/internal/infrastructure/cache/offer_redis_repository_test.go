package cache

import (
	"context"
	"testing"
	"time"

	"matching-service/internal/domain"
)

func TestCurrentOffer_ReturnsRideAndExpiry(t *testing.T) {
	ctx := context.Background()
	r := NewOfferRepository(testRedis)

	if _, err := r.TryOffer(ctx, "co-ride", "co-driver", 30*time.Second); err != nil {
		t.Fatal(err)
	}

	rideID, expiresAt, err := r.CurrentOffer(ctx, "co-driver")
	if err != nil {
		t.Fatal(err)
	}
	if rideID != "co-ride" {
		t.Fatalf("rideID = %q, want co-ride", rideID)
	}
	if d := time.Until(expiresAt); d <= 0 || d > 30*time.Second {
		t.Fatalf("expiresAt is %v away, want (0, 30s]", d)
	}
}

func TestCurrentOffer_NoOfferIsNotAnError(t *testing.T) {
	rideID, expiresAt, err := NewOfferRepository(testRedis).CurrentOffer(context.Background(), "co-nobody")
	if err != nil {
		t.Fatalf("a missing offer must not be an error: %v", err)
	}
	if rideID != "" || !expiresAt.IsZero() {
		t.Fatalf("got (%q, %v), want (\"\", zero)", rideID, expiresAt)
	}
}

func TestIncrOfferCount_IncrementsAndSetsTTL(t *testing.T) {
	ctx := context.Background()
	r := NewOfferRepository(testRedis)

	for i := 0; i < 3; i++ {
		if err := r.IncrOfferCount(ctx, "rate-driver"); err != nil {
			t.Fatal(err)
		}
	}

	n, err := r.OfferCount(ctx, "rate-driver")
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("offer count = %d, want 3", n)
	}
	ttl, err := testRedis.TTL(ctx, rateKey("rate-driver")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > time.Minute {
		t.Fatalf("TTL = %v, want (0, 1m]", ttl)
	}
}

// ListPending scans pending_ride:* across the whole DB, and the harness never flushes —
// so this is the only test in the package allowed to create pending_ride keys.
func TestListPending_RoundTripsEveryPendingRide(t *testing.T) {
	ctx := context.Background()
	r := NewOfferRepository(testRedis)

	deadline := time.Now().Add(30 * time.Second).UTC().Truncate(time.Millisecond)
	for _, id := range []string{"lp-1", "lp-2", "lp-3"} {
		if err := r.SetPending(ctx, domain.PendingRide{RideID: id, Attempt: 2, Deadline: deadline}); err != nil {
			t.Fatal(err)
		}
	}

	got, err := r.ListPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d pending rides, want 3", len(got))
	}
	seen := map[string]domain.PendingRide{}
	for _, p := range got {
		seen[p.RideID] = p
	}
	for _, id := range []string{"lp-1", "lp-2", "lp-3"} {
		p, ok := seen[id]
		if !ok {
			t.Fatalf("%s missing from ListPending: %+v", id, got)
		}
		if p.Attempt != 2 {
			t.Fatalf("%s attempt = %d, want 2", id, p.Attempt)
		}
		if !p.Deadline.Equal(deadline) {
			t.Fatalf("%s deadline = %v, want %v", id, p.Deadline, deadline)
		}
	}
}
