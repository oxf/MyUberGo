package workers

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"matching-service/internal/application/command"
	"matching-service/internal/domain"

	"github.com/sirupsen/logrus"
)

func testLogger() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return logrus.NewEntry(l)
}

// fakeOffers embeds domain.OfferRepository so only the methods MatchRetryWorker
// actually calls need implementing (house idiom, see command/broadcast_offers_test.go).
type fakeOffers struct {
	domain.OfferRepository

	pending          []domain.PendingRide
	listErr          error
	acceptedBy       map[string]string // rideID -> driverID
	acceptedByErrFor map[string]error  // rideID -> error to return

	mu             sync.Mutex
	deletedPending []string
}

func (f *fakeOffers) ListPending(ctx context.Context) ([]domain.PendingRide, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.pending, nil
}

func (f *fakeOffers) AcceptedBy(ctx context.Context, rideID string) (string, error) {
	if err := f.acceptedByErrFor[rideID]; err != nil {
		return "", err
	}
	return f.acceptedBy[rideID], nil
}

func (f *fakeOffers) DeletePending(ctx context.Context, rideID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedPending = append(f.deletedPending, rideID)
	return nil
}

func (f *fakeOffers) deleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deletedPending...)
}

// fakeBroadcast satisfies decorator.CommandHandlerNoResult[command.BroadcastOffers].
// inFlight/maxSeen observe concurrency without a barrier that could deadlock a sequential sweep.
type fakeBroadcast struct {
	mu       sync.Mutex
	received []command.BroadcastOffers

	inFlight atomic.Int64
	maxSeen  atomic.Int64

	hold   time.Duration    // simulated work per call
	errFor map[string]error // rideID -> error to return
}

func newFakeBroadcast() *fakeBroadcast {
	return &fakeBroadcast{errFor: map[string]error{}}
}

func (f *fakeBroadcast) Handle(ctx context.Context, cmd command.BroadcastOffers) error {
	n := f.inFlight.Add(1)
	for {
		max := f.maxSeen.Load()
		if n <= max || f.maxSeen.CompareAndSwap(max, n) {
			break
		}
	}
	defer f.inFlight.Add(-1)

	f.mu.Lock()
	f.received = append(f.received, cmd)
	f.mu.Unlock()

	if f.hold > 0 {
		time.Sleep(f.hold)
	}
	return f.errFor[cmd.RideID]
}

func (f *fakeBroadcast) calls() []command.BroadcastOffers {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]command.BroadcastOffers(nil), f.received...)
}

func (f *fakeBroadcast) maxInFlight() int64 { return f.maxSeen.Load() }
