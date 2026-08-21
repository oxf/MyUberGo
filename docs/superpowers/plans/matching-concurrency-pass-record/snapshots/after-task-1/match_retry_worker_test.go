package workers

import (
	"context"
	"errors"
	"testing"
	"time"

	"matching-service/internal/domain"
)

func past() time.Time   { return time.Now().Add(-time.Minute) }
func future() time.Time { return time.Now().Add(time.Minute) }

func newTestWorker(offers *fakeOffers, broadcast *fakeBroadcast) *MatchRetryWorker {
	return NewMatchRetryWorker(offers, broadcast, testLogger(), time.Hour)
}

func TestSweep_RebroadcastsLapsedRideWithIncrementedAttempt(t *testing.T) {
	offers := &fakeOffers{
		pending:    []domain.PendingRide{{RideID: "r1", Attempt: 2, Deadline: past()}},
		acceptedBy: map[string]string{},
	}
	bc := newFakeBroadcast()

	newTestWorker(offers, bc).sweep(context.Background())

	calls := bc.calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 broadcast, got %d", len(calls))
	}
	if calls[0].RideID != "r1" || calls[0].Attempt != 3 {
		t.Fatalf("got %+v, want {r1 3}", calls[0])
	}
}

func TestSweep_SkipsRideNotYetDue(t *testing.T) {
	offers := &fakeOffers{
		pending:    []domain.PendingRide{{RideID: "r1", Attempt: 1, Deadline: future()}},
		acceptedBy: map[string]string{},
	}
	bc := newFakeBroadcast()

	newTestWorker(offers, bc).sweep(context.Background())

	if n := len(bc.calls()); n != 0 {
		t.Fatalf("expected no broadcast for a ride not yet due, got %d", n)
	}
	if d := offers.deleted(); len(d) != 0 {
		t.Fatalf("expected no pending deletion, got %v", d)
	}
}

func TestSweep_AlreadyAcceptedRideIsCleanedUpNotRebroadcast(t *testing.T) {
	offers := &fakeOffers{
		pending:    []domain.PendingRide{{RideID: "r1", Attempt: 1, Deadline: past()}},
		acceptedBy: map[string]string{"r1": "d1"},
	}
	bc := newFakeBroadcast()

	newTestWorker(offers, bc).sweep(context.Background())

	if n := len(bc.calls()); n != 0 {
		t.Fatalf("expected no broadcast for an accepted ride, got %d", n)
	}
	if d := offers.deleted(); len(d) != 1 || d[0] != "r1" {
		t.Fatalf("expected pending_ride:r1 deleted, got %v", d)
	}
}

func TestSweep_ListPendingErrorIsSurvivable(t *testing.T) {
	offers := &fakeOffers{listErr: errors.New("redis down")}
	bc := newFakeBroadcast()

	newTestWorker(offers, bc).sweep(context.Background()) // must not panic

	if n := len(bc.calls()); n != 0 {
		t.Fatalf("expected no broadcast when ListPending fails, got %d", n)
	}
}

func TestSweep_AcceptedByErrorSkipsOnlyThatRide(t *testing.T) {
	offers := &fakeOffers{
		pending: []domain.PendingRide{
			{RideID: "r1", Attempt: 1, Deadline: past()},
			{RideID: "r2", Attempt: 1, Deadline: past()},
		},
		acceptedBy:       map[string]string{},
		acceptedByErrFor: map[string]error{"r1": errors.New("redis timeout")},
	}
	bc := newFakeBroadcast()

	newTestWorker(offers, bc).sweep(context.Background())

	calls := bc.calls()
	for _, c := range calls {
		if c.RideID == "r1" {
			t.Fatalf("expected no broadcast for r1 (AcceptedBy error), got %+v", calls)
		}
	}
	if n := len(calls); n != 1 || calls[0].RideID != "r2" {
		t.Fatalf("expected exactly r2 broadcast despite r1's AcceptedBy error, got %+v", calls)
	}
}

// A failing broadcast must not abort the rest of the sweep — one stuck ride
// can't starve every other pending ride.
func TestSweep_BroadcastErrorDoesNotAbortRemainingRides(t *testing.T) {
	offers := &fakeOffers{
		pending: []domain.PendingRide{
			{RideID: "r1", Attempt: 1, Deadline: past()},
			{RideID: "r2", Attempt: 1, Deadline: past()},
			{RideID: "r3", Attempt: 1, Deadline: past()},
		},
		acceptedBy: map[string]string{},
	}
	bc := newFakeBroadcast()
	bc.errFor["r1"] = errors.New("broadcast blew up")

	newTestWorker(offers, bc).sweep(context.Background())

	if n := len(bc.calls()); n != 3 {
		t.Fatalf("expected all 3 rides swept despite r1 failing, got %d", n)
	}
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	offers := &fakeOffers{acceptedBy: map[string]string{}}
	w := NewMatchRetryWorker(offers, newFakeBroadcast(), testLogger(), 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}
