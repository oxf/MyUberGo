package command

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"matching-service/internal/application/services"
	"matching-service/internal/domain"
	"matching-service/internal/infrastructure/metrics"

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
	// userIDs maps driverID -> owning auth.user(id); GetUserID returns "" for an unlisted driver.
	userIDs map[string]string
	// onlineRatings backs OnlineRatings — a driverID absent (or mapped to 0) means not online.
	onlineRatings map[string]float64
}

func (f *fakeDrivers) OnlineRatings(ctx context.Context, ids []string) (map[string]float64, error) {
	out := make(map[string]float64, len(ids))
	for _, id := range ids {
		out[id] = f.onlineRatings[id]
	}
	return out, nil
}

// erroringOnlineRatingsDrivers overrides only OnlineRatings to fail, so the availability-intersect
// failure point (not just the Nearby call) is independently testable.
type erroringOnlineRatingsDrivers struct {
	*fakeDrivers
}

func (f *erroringOnlineRatingsDrivers) OnlineRatings(ctx context.Context, ids []string) (map[string]float64, error) {
	return nil, errors.New("redis down")
}

// fakeLocationClient is the services.LocationClient test double; recording the last call's
// arguments lets tests assert the ride's pickup coordinates and attempt-derived radius pass through.
type fakeLocationClient struct {
	nearby      []services.NearbyDriver
	err         error
	gotLat      float64
	gotLon      float64
	gotRadiusKm float64
	gotLimit    int
}

func (f *fakeLocationClient) Nearby(ctx context.Context, lat, lon, radiusKm float64, limit int) ([]services.NearbyDriver, error) {
	f.gotLat, f.gotLon, f.gotRadiusKm, f.gotLimit = lat, lon, radiusKm, limit
	return f.nearby, f.err
}

func (f *fakeDrivers) GetUserID(ctx context.Context, driverID string) (string, error) {
	return f.userIDs[driverID], nil
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
	pending        []domain.PendingRide
	deletedPending []string
	currentOffer   map[string]string // driverID -> rideID
	acceptedBy     string
	cancelled      bool
	cleared        []string

	// mu guards offeredNow: TryOffer is now called concurrently (one goroutine per
	// target), so this fake must be safe for concurrent mutation like a real repo.
	mu         sync.Mutex
	offeredNow []string
}

func (f *fakeOffers) OfferedDrivers(ctx context.Context, rideID string) (map[string]bool, error) {
	return f.offered, nil
}
func (f *fakeOffers) HasCurrentOffer(ctx context.Context, id string) (bool, error) {
	return f.busy[id], nil
}
func (f *fakeOffers) HasCurrentOffers(ctx context.Context, ids []string) (map[string]bool, error) {
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		out[id] = f.busy[id]
	}
	return out, nil
}
func (f *fakeOffers) OfferCount(ctx context.Context, id string) (int64, error) {
	return f.counts[id], nil
}
func (f *fakeOffers) OfferCounts(ctx context.Context, ids []string) (map[string]int64, error) {
	out := make(map[string]int64, len(ids))
	for _, id := range ids {
		out[id] = f.counts[id]
	}
	return out, nil
}
func (f *fakeOffers) IncrOfferCount(ctx context.Context, id string) error { return nil }
func (f *fakeOffers) TryOffer(ctx context.Context, rideID, driverID string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	f.offeredNow = append(f.offeredNow, driverID)
	f.mu.Unlock()
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
	return &BroadcastOffersHandler{rides: rides, drivers: drivers, offers: offers, metrics: metrics.NewNoopMetricsClient()}
}

func newTestBroadcastHandlerWithLocation(rides *fakeRides, drivers domain.DriverRepository, offers *fakeOffers, location services.LocationClient) *BroadcastOffersHandler {
	return &BroadcastOffersHandler{rides: rides, drivers: drivers, offers: offers, location: location, metrics: metrics.NewNoopMetricsClient()}
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
	// Targets are now offered concurrently (one goroutine per target), so
	// TryOffer's call order is no longer deterministic — compare as sets.
	want := []string{"d", "e"}
	got := append([]string(nil), offers.offeredNow...)
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
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

func TestRadiusForAttempt(t *testing.T) {
	tests := []struct {
		attempt int
		want    float64
	}{
		{1, 5}, {2, 7}, {3, 9}, {4, 11}, {5, 13}, {6, 15}, {100, 15},
	}
	for _, tt := range tests {
		if got := radiusForAttempt(tt.attempt); got != tt.want {
			t.Errorf("radiusForAttempt(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestBroadcastOffers_UsesLocationDiscoveryWhenAvailable(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching, PickupLat: 34.7, PickupLng: 33.0}}
	offers := &fakeOffers{offered: map[string]bool{}, busy: map[string]bool{}, counts: map[string]int64{}}
	drivers := &fakeDrivers{
		pool:          pool(7), // must NOT be used — this test asserts location's candidates win
		onlineRatings: map[string]float64{"near-1": 4.5, "near-2": 4.8},
	}
	loc := &fakeLocationClient{nearby: []services.NearbyDriver{
		{DriverID: "near-1", DistanceM: 500},
		{DriverID: "near-2", DistanceM: 200},
	}}
	h := newTestBroadcastHandlerWithLocation(rides, drivers, offers, loc)

	if err := h.Handle(context.Background(), BroadcastOffers{RideID: "r1", Attempt: 1}); err != nil {
		t.Fatal(err)
	}

	got := append([]string(nil), offers.offeredNow...)
	sort.Strings(got)
	want := []string{"near-1", "near-2"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v want %v (should use location's candidates, not the fallback pool)", got, want)
	}
	if loc.gotLat != 34.7 || loc.gotLon != 33.0 {
		t.Fatalf("nearby called with lat=%v lon=%v, want the ride's pickup (34.7, 33.0)", loc.gotLat, loc.gotLon)
	}
	if loc.gotRadiusKm != radiusForAttempt(1) {
		t.Fatalf("nearby called with radiusKm=%v, want %v", loc.gotRadiusKm, radiusForAttempt(1))
	}
}

func TestBroadcastOffers_FiltersOutOfflineLocationCandidates(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	offers := &fakeOffers{offered: map[string]bool{}, busy: map[string]bool{}, counts: map[string]int64{}}
	// "offline-1" is deliberately absent from onlineRatings — Location returns it as a
	// geographic candidate (it doesn't know shift state), but it must never be offered.
	drivers := &fakeDrivers{onlineRatings: map[string]float64{"online-1": 4.5}}
	loc := &fakeLocationClient{nearby: []services.NearbyDriver{
		{DriverID: "online-1", DistanceM: 100},
		{DriverID: "offline-1", DistanceM: 50},
	}}
	h := newTestBroadcastHandlerWithLocation(rides, drivers, offers, loc)

	if err := h.Handle(context.Background(), BroadcastOffers{RideID: "r1", Attempt: 1}); err != nil {
		t.Fatal(err)
	}

	if len(offers.offeredNow) != 1 || offers.offeredNow[0] != "online-1" {
		t.Fatalf("got %v, want exactly [online-1] — the read-path intersection must exclude a geographically-close but offline driver", offers.offeredNow)
	}
}

func TestBroadcastOffers_FallsBackWhenLocationErrors(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	offers := &fakeOffers{offered: map[string]bool{}, busy: map[string]bool{}, counts: map[string]int64{}}
	drivers := &fakeDrivers{pool: pool(3)}
	loc := &fakeLocationClient{err: errors.New("connection refused")}
	h := newTestBroadcastHandlerWithLocation(rides, drivers, offers, loc)

	if err := h.Handle(context.Background(), BroadcastOffers{RideID: "r1", Attempt: 1}); err != nil {
		t.Fatal(err)
	}

	if len(offers.offeredNow) != 3 {
		t.Fatalf("got %d offers, want 3 (the fallback rating-only pool) — a Location error must never fail the ride", len(offers.offeredNow))
	}
}

func TestBroadcastOffers_FallsBackWhenOnlineRatingsErrors(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	offers := &fakeOffers{offered: map[string]bool{}, busy: map[string]bool{}, counts: map[string]int64{}}
	drivers := &erroringOnlineRatingsDrivers{fakeDrivers: &fakeDrivers{pool: pool(3)}}
	loc := &fakeLocationClient{nearby: []services.NearbyDriver{{DriverID: "x", DistanceM: 100}}}
	h := newTestBroadcastHandlerWithLocation(rides, drivers, offers, loc)

	if err := h.Handle(context.Background(), BroadcastOffers{RideID: "r1", Attempt: 1}); err != nil {
		t.Fatal(err)
	}

	if len(offers.offeredNow) != 3 {
		t.Fatalf("got %d offers, want 3 (the fallback rating-only pool) after the availability intersect itself errors", len(offers.offeredNow))
	}
}

var _ = contracts.RideRequestedEvent{} // keep import if unused after edits
