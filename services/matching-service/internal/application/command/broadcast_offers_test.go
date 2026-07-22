package command

import (
	"context"
	"reflect"
	"testing"
	"time"

	"matching-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/kafka"
)

type fakeRides struct {
	domain.RideRepository
	ride      *domain.Ride
	failed    []string
	matched   []string
	cancelled []string
}

func (f *fakeRides) GetRide(ctx context.Context, id string) (*domain.Ride, error) { return f.ride, nil }
func (f *fakeRides) MarkFailed(ctx context.Context, id string) error {
	f.failed = append(f.failed, id)
	return nil
}
func (f *fakeRides) MarkMatched(ctx context.Context, rideID, driverID string, driverRating float64) error {
	f.matched = append(f.matched, rideID+"/"+driverID)
	return nil
}
func (f *fakeRides) MarkCancelled(ctx context.Context, id string) error {
	f.cancelled = append(f.cancelled, id)
	return nil
}

type fakeDrivers struct {
	domain.DriverRepository
	pool      []domain.Candidate
	removed   []string
	addedBack map[string]float64
}

func (f *fakeDrivers) TopOnlineDrivers(ctx context.Context, limit int) ([]domain.Candidate, error) {
	if limit > len(f.pool) {
		limit = len(f.pool)
	}
	return f.pool[:limit], nil
}
func (f *fakeDrivers) RemoveOnline(ctx context.Context, id string) error {
	f.removed = append(f.removed, id)
	return nil
}
func (f *fakeDrivers) Rating(ctx context.Context, id string) (float64, error) {
	return 0, nil
}
func (f *fakeDrivers) AddOnline(ctx context.Context, id string, rating float64) error {
	if f.addedBack == nil {
		f.addedBack = map[string]float64{}
	}
	f.addedBack[id] = rating
	return nil
}

type fakeOffers struct {
	domain.OfferRepository
	offered        map[string]bool
	busy           map[string]bool
	counts         map[string]int64
	offeredNow     []string
	pending        []domain.PendingRide
	deletedPending []string
	currentOffer   map[string]string // driverID -> rideID
	acceptedBy     string
	cancelled      bool
	cleared        []string
}

func (f *fakeOffers) OfferedDrivers(ctx context.Context, rideID string) (map[string]bool, error) {
	return f.offered, nil
}
func (f *fakeOffers) HasCurrentOffer(ctx context.Context, id string) (bool, error) {
	return f.busy[id], nil
}
func (f *fakeOffers) OfferCount(ctx context.Context, id string) (int64, error) {
	return f.counts[id], nil
}
func (f *fakeOffers) IncrOfferCount(ctx context.Context, id string) error { return nil }
func (f *fakeOffers) TryOffer(ctx context.Context, rideID, driverID string, ttl time.Duration) (bool, error) {
	f.offeredNow = append(f.offeredNow, driverID)
	return true, nil
}
func (f *fakeOffers) SetPending(ctx context.Context, p domain.PendingRide) error {
	f.pending = append(f.pending, p)
	return nil
}
func (f *fakeOffers) DeletePending(ctx context.Context, rideID string) error {
	f.deletedPending = append(f.deletedPending, rideID)
	return nil
}
func (f *fakeOffers) CurrentOffer(ctx context.Context, driverID string) (string, time.Time, error) {
	return f.currentOffer[driverID], time.Now().Add(10 * time.Second), nil
}
func (f *fakeOffers) IsCancelled(ctx context.Context, rideID string) (bool, error) {
	return f.cancelled, nil
}
func (f *fakeOffers) SetCancelled(ctx context.Context, rideID string) error {
	f.cancelled = true
	return nil
}
func (f *fakeOffers) AcceptedBy(ctx context.Context, rideID string) (string, error) {
	return f.acceptedBy, nil
}
func (f *fakeOffers) TryAccept(ctx context.Context, rideID, driverID string, ttl time.Duration) (bool, error) {
	if f.acceptedBy != "" {
		return false, nil
	}
	f.acceptedBy = driverID
	return true, nil
}
func (f *fakeOffers) ClearCurrentOffer(ctx context.Context, driverID string) error {
	f.cleared = append(f.cleared, driverID)
	delete(f.currentOffer, driverID)
	return nil
}

func pool(n int) []domain.Candidate {
	out := make([]domain.Candidate, n)
	for i := range out {
		out[i] = domain.Candidate{DriverID: string(rune('a' + i)), Rating: 5 - float64(i)*0.1}
	}
	return out
}

func newTestBroadcastHandler(rides *fakeRides, drivers *fakeDrivers, offers *fakeOffers) *BroadcastOffersHandler {
	return &BroadcastOffersHandler{rides: rides, drivers: drivers, offers: offers}
}

func TestBroadcastOffers_TopFiveFirstAttempt(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	offers := &fakeOffers{offered: map[string]bool{}, busy: map[string]bool{}, counts: map[string]int64{}}
	h := newTestBroadcastHandler(rides, &fakeDrivers{pool: pool(7)}, offers)

	if err := h.Handle(context.Background(), BroadcastOffers{RideID: "r1", Attempt: 1}); err != nil {
		t.Fatal(err)
	}
	if len(offers.offeredNow) != 5 {
		t.Fatalf("expected 5 offers, got %v", offers.offeredNow)
	}
	if len(offers.pending) != 1 || offers.pending[0].Attempt != 1 {
		t.Fatalf("expected pending attempt=1, got %+v", offers.pending)
	}
}

func TestBroadcastOffers_SkipsOfferedAndRateLimited(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	offers := &fakeOffers{
		offered: map[string]bool{"a": true},
		busy:    map[string]bool{"b": true},
		counts:  map[string]int64{"c": RateLimitPerMinute},
	}
	h := newTestBroadcastHandler(rides, &fakeDrivers{pool: pool(5)}, offers)

	if err := h.Handle(context.Background(), BroadcastOffers{RideID: "r1", Attempt: 1}); err != nil {
		t.Fatal(err)
	}
	want := []string{"d", "e"}
	if !reflect.DeepEqual(offers.offeredNow, want) {
		t.Fatalf("got %v want %v", offers.offeredNow, want)
	}
}

func TestBroadcastOffers_GivesUpAfterMaxAttempts(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	offers := &fakeOffers{offered: map[string]bool{}, busy: map[string]bool{}, counts: map[string]int64{}}
	h := newTestBroadcastHandler(rides, &fakeDrivers{pool: pool(3)}, offers)

	if err := h.Handle(context.Background(), BroadcastOffers{RideID: "r1", Attempt: MaxAttempts + 1}); err != nil {
		t.Fatal(err)
	}
	if len(rides.failed) != 1 || len(offers.deletedPending) != 1 {
		t.Fatalf("expected ride failed + pending deleted, got failed=%v deleted=%v", rides.failed, offers.deletedPending)
	}
	if len(offers.offeredNow) != 0 {
		t.Fatalf("no offers expected after give-up, got %v", offers.offeredNow)
	}
}

func TestBroadcastOffers_NonSearchingRideCleansUp(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusMatched}}
	offers := &fakeOffers{}
	h := newTestBroadcastHandler(rides, &fakeDrivers{pool: pool(3)}, offers)

	if err := h.Handle(context.Background(), BroadcastOffers{RideID: "r1", Attempt: 2}); err != nil {
		t.Fatal(err)
	}
	if len(offers.deletedPending) != 1 || len(offers.offeredNow) != 0 {
		t.Fatalf("expected only pending cleanup, got %+v / %v", offers.deletedPending, offers.offeredNow)
	}
}

var _ = contracts.RideRequestedEvent{} // keep import if unused after edits
