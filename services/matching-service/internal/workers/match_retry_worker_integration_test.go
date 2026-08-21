package workers

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"matching-service/internal/application/command"
	"matching-service/internal/application/services"
	"matching-service/internal/domain"
	"matching-service/internal/infrastructure/metrics"

	contracts "github.com/oxf/MyUber/contracts/kafka"
)

// integRides is a minimal domain.RideRepository: every ride pre-exists and
// stays Searching, so every sweep round genuinely rebroadcasts.
type integRides struct {
	mu     sync.Mutex
	rides  map[string]*domain.Ride
	failed []string
}

func (r *integRides) SaveRide(ctx context.Context, event contracts.RideRequestedEvent) error {
	return nil
}
func (r *integRides) GetRide(ctx context.Context, rideID string) (*domain.Ride, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rides[rideID], nil
}
func (r *integRides) MarkMatched(ctx context.Context, rideID, driverID string, rating float64) error {
	return nil
}
func (r *integRides) MarkFailed(ctx context.Context, rideID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed = append(r.failed, rideID)
	return nil
}
func (r *integRides) MarkCancelled(ctx context.Context, rideID string) error { return nil }

// integDrivers is a minimal domain.DriverRepository backed by a fixed,
// read-only online pool — safe for concurrent TopOnlineDrivers calls.
type integDrivers struct{ pool []domain.Candidate }

func (d *integDrivers) UpsertDriver(ctx context.Context, event contracts.ShiftUpdatedEvent) error {
	return nil
}
func (d *integDrivers) TopOnlineDrivers(ctx context.Context, limit int) ([]domain.Candidate, error) {
	if limit > len(d.pool) {
		limit = len(d.pool)
	}
	return d.pool[:limit], nil
}
func (d *integDrivers) RemoveOnline(ctx context.Context, driverID string) error { return nil }
func (d *integDrivers) Rating(ctx context.Context, driverID string) (float64, error) {
	return 0, nil
}
func (d *integDrivers) AddOnline(ctx context.Context, driverID string, rating float64) error {
	return nil
}
func (d *integDrivers) GetUserID(ctx context.Context, driverID string) (string, error) {
	return "", nil
}
func (d *integDrivers) OnlineRatings(ctx context.Context, ids []string) (map[string]float64, error) {
	return nil, nil
}

// integOfferTTL mirrors command.OfferTTL without importing an unexported constant.
const integOfferTTL = 30 * time.Second

// integOffers is a full, mutex-guarded domain.OfferRepository fake — shared by
// both the real BroadcastOffersHandler and the real MatchRetryWorker, so their
// composed fan-out (up to sweepConcurrency*BroadcastSize goroutines) hits one
// consistent, race-safe store, same as the real Redis-backed repo would.
type integOffers struct {
	mu           sync.Mutex
	offered      map[string]map[string]bool // rideID -> driverID -> true
	currentOffer map[string]string          // driverID -> rideID
	counts       map[string]int64           // driverID -> offer count
	pending      map[string]domain.PendingRide
	acceptedBy   map[string]string
	cancelled    map[string]bool
}

func newIntegOffers() *integOffers {
	return &integOffers{
		offered:      map[string]map[string]bool{},
		currentOffer: map[string]string{},
		counts:       map[string]int64{},
		pending:      map[string]domain.PendingRide{},
		acceptedBy:   map[string]string{},
		cancelled:    map[string]bool{},
	}
}

func (o *integOffers) OfferedDrivers(ctx context.Context, rideID string) (map[string]bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string]bool, len(o.offered[rideID]))
	for k, v := range o.offered[rideID] {
		out[k] = v
	}
	return out, nil
}

func (o *integOffers) TryOffer(ctx context.Context, rideID, driverID string, ttl time.Duration) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.currentOffer[driverID] != "" {
		return false, nil
	}
	o.currentOffer[driverID] = rideID
	if o.offered[rideID] == nil {
		o.offered[rideID] = map[string]bool{}
	}
	o.offered[rideID][driverID] = true
	return true, nil
}

func (o *integOffers) CurrentOffer(ctx context.Context, driverID string) (string, time.Time, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.currentOffer[driverID], time.Now().Add(integOfferTTL), nil
}
func (o *integOffers) HasCurrentOffer(ctx context.Context, driverID string) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.currentOffer[driverID] != "", nil
}
func (o *integOffers) HasCurrentOffers(ctx context.Context, ids []string) (map[string]bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		out[id] = o.currentOffer[id] != ""
	}
	return out, nil
}
func (o *integOffers) ClearCurrentOffer(ctx context.Context, driverID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.currentOffer, driverID)
	return nil
}
func (o *integOffers) TryAccept(ctx context.Context, rideID, driverID string, ttl time.Duration) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.acceptedBy[rideID] != "" {
		return false, nil
	}
	o.acceptedBy[rideID] = driverID
	return true, nil
}
func (o *integOffers) AcceptedBy(ctx context.Context, rideID string) (string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.acceptedBy[rideID], nil
}
func (o *integOffers) IsCancelled(ctx context.Context, rideID string) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.cancelled[rideID], nil
}
func (o *integOffers) SetCancelled(ctx context.Context, rideID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cancelled[rideID] = true
	return nil
}
func (o *integOffers) OfferCount(ctx context.Context, driverID string) (int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.counts[driverID], nil
}
func (o *integOffers) OfferCounts(ctx context.Context, ids []string) (map[string]int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string]int64, len(ids))
	for _, id := range ids {
		out[id] = o.counts[id]
	}
	return out, nil
}
func (o *integOffers) IncrOfferCount(ctx context.Context, driverID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.counts[driverID]++
	return nil
}
func (o *integOffers) SetPending(ctx context.Context, p domain.PendingRide) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending[p.RideID] = p
	return nil
}
func (o *integOffers) DeletePending(ctx context.Context, rideID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.pending, rideID)
	return nil
}
func (o *integOffers) ListPending(ctx context.Context) ([]domain.PendingRide, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]domain.PendingRide, 0, len(o.pending))
	for _, p := range o.pending {
		out = append(out, p)
	}
	return out, nil
}
func (o *integOffers) totalOffered() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, drivers := range o.offered {
		n += len(drivers)
	}
	return n
}

// integPublisher records published events under a mutex — TryOffer's fan-out
// and the sweep's own fan-out can both reach a give-up publish concurrently.
type integPublisher struct {
	mu     sync.Mutex
	topics []string
}

func (p *integPublisher) Publish(ctx context.Context, topic string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.topics = append(p.topics, topic)
	return nil
}

var _ services.EventPublisher = (*integPublisher)(nil)

func integDriverPool(n int) []domain.Candidate {
	out := make([]domain.Candidate, n)
	for i := range out {
		out[i] = domain.Candidate{DriverID: "driver-" + strconv.Itoa(i), Rating: 5 - float64(i)*0.1}
	}
	return out
}

// TestComposedFanOut_RealHandlerWiredIntoRealWorker is the one test in this repo that
// wires the REAL BroadcastOffersHandler into the REAL MatchRetryWorker and sweeps many
// rides concurrently — exercising the full composed fan-out (sweepConcurrency x
// BroadcastSize goroutines, real decorator chain) under -race.
func TestComposedFanOut_RealHandlerWiredIntoRealWorker(t *testing.T) {
	const numRides = 20
	rides := &integRides{rides: map[string]*domain.Ride{}}
	for i := 0; i < numRides; i++ {
		id := "r" + strconv.Itoa(i)
		rides.rides[id] = &domain.Ride{RideID: id, Status: domain.RideStatusSearching, PickupLat: 34.7, PickupLng: 33.0}
	}
	drivers := &integDrivers{pool: integDriverPool(8)}
	offers := newIntegOffers()
	for i := 0; i < numRides; i++ {
		id := "r" + strconv.Itoa(i)
		offers.pending[id] = domain.PendingRide{RideID: id, Attempt: 1, Deadline: past()}
	}
	pub := &integPublisher{}

	broadcastHandler := command.NewBroadcastOffersHandler(
		rides, drivers, offers, nil, pub, testLogger(), metrics.NewNoopMetricsClient(),
	)
	worker := NewMatchRetryWorker(offers, broadcastHandler, testLogger(), time.Hour)

	worker.sweep(context.Background())

	if got := offers.totalOffered(); got == 0 {
		t.Fatalf("expected at least some offers recorded across the sweep, got 0")
	}
	if len(offers.pending) != numRides {
		t.Fatalf("expected all %d rides to still have an armed retry deadline, got %d", numRides, len(offers.pending))
	}
	for id, p := range offers.pending {
		if p.Attempt != 2 {
			t.Fatalf("ride %s: expected re-armed pending at attempt 2, got %d", id, p.Attempt)
		}
	}
}
