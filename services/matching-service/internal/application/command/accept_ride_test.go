package command

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	cmnerrors "matching-service/internal/common/errors"
	"matching-service/internal/domain"
	"matching-service/internal/infrastructure/metrics"

	contracts "github.com/oxf/MyUber/contracts/kafka"
)

type fakePublisher struct {
	topics   []string
	payloads [][]byte
}

func (f *fakePublisher) Publish(ctx context.Context, topic string, payload []byte) error {
	f.topics = append(f.topics, topic)
	f.payloads = append(f.payloads, payload)
	return nil
}

func TestAcceptRide_HappyPathPublishes(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching, CreatedAt: "2026-07-21T10:00:00Z"}}
	drivers := &fakeDrivers{userIDs: map[string]string{"d1": "u1"}}
	offers := &fakeOffers{currentOffer: map[string]string{"d1": "r1"}}
	pub := &fakePublisher{}
	h := &AcceptRideHandler{rides: rides, drivers: drivers, offers: offers, publisher: pub, metrics: metrics.NewNoopMetricsClient()}

	if err := h.Handle(context.Background(), AcceptRide{RideID: "r1", DriverID: "d1", CallerUserID: "u1"}); err != nil {
		t.Fatal(err)
	}
	if len(rides.matched) != 1 || rides.matched[0] != "r1/d1" {
		t.Fatalf("MarkMatched not called: %v", rides.matched)
	}
	if len(pub.topics) != 1 || pub.topics[0] != "ride.accepted" {
		t.Fatalf("expected ride.accepted publish, got %v", pub.topics)
	}
	if len(drivers.removed) != 1 || len(offers.cleared) != 1 || len(offers.deletedPending) != 1 {
		t.Fatal("expected pool removal, offer clear, and pending cleanup")
	}

	var event contracts.RideAcceptedEvent
	if err := json.Unmarshal(pub.payloads[0], &event); err != nil {
		t.Fatal(err)
	}
	if event.RequestedAt != "2026-07-21T10:00:00Z" {
		t.Fatalf("expected RequestedAt threaded from ride.CreatedAt, got %q", event.RequestedAt)
	}
}

func TestAcceptRide_LostRaceIsErrRideTaken(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	offers := &fakeOffers{currentOffer: map[string]string{"d2": "r1"}, acceptedBy: "d1"}
	drivers := &fakeDrivers{userIDs: map[string]string{"d2": "u2"}}
	h := &AcceptRideHandler{rides: rides, drivers: drivers, offers: offers, publisher: &fakePublisher{}, metrics: metrics.NewNoopMetricsClient()}

	err := h.Handle(context.Background(), AcceptRide{RideID: "r1", DriverID: "d2", CallerUserID: "u2"})
	if !errors.Is(err, cmnerrors.ErrRideTaken) {
		t.Fatalf("want ErrRideTaken, got %v", err)
	}
}

func TestAcceptRide_WinnerReacceptingIsErrRideTaken(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	drivers := &fakeDrivers{userIDs: map[string]string{"d1": "u1"}}
	offers := &fakeOffers{currentOffer: map[string]string{"d1": "r1"}}
	pub := &fakePublisher{}
	h := &AcceptRideHandler{rides: rides, drivers: drivers, offers: offers, publisher: pub, metrics: metrics.NewNoopMetricsClient()}

	if err := h.Handle(context.Background(), AcceptRide{RideID: "r1", DriverID: "d1", CallerUserID: "u1"}); err != nil {
		t.Fatalf("first accept should succeed, got %v", err)
	}

	// Same driver re-accepts the ride they already won. ClearCurrentOffer ran
	// as part of the first accept, so without the AcceptedBy short-circuit
	// this would fall through to the "no live offer" check and wrongly
	// return ErrOfferGone (400) instead of ErrRideTaken (409).
	err := h.Handle(context.Background(), AcceptRide{RideID: "r1", DriverID: "d1", CallerUserID: "u1"})
	if !errors.Is(err, cmnerrors.ErrRideTaken) {
		t.Fatalf("want ErrRideTaken on re-accept by winner, got %v", err)
	}
}

func TestAcceptRide_NoLiveOfferIsErrOfferGone(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	offers := &fakeOffers{currentOffer: map[string]string{}}
	drivers := &fakeDrivers{userIDs: map[string]string{"d9": "u9"}}
	h := &AcceptRideHandler{rides: rides, drivers: drivers, offers: offers, publisher: &fakePublisher{}, metrics: metrics.NewNoopMetricsClient()}

	err := h.Handle(context.Background(), AcceptRide{RideID: "r1", DriverID: "d9", CallerUserID: "u9"})
	if !errors.Is(err, cmnerrors.ErrOfferGone) {
		t.Fatalf("want ErrOfferGone, got %v", err)
	}
}

func TestAcceptRide_CallerNotOwningDriverIsForbidden(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	offers := &fakeOffers{currentOffer: map[string]string{"d1": "r1"}}
	drivers := &fakeDrivers{userIDs: map[string]string{"d1": "u1"}}
	h := &AcceptRideHandler{rides: rides, drivers: drivers, offers: offers, publisher: &fakePublisher{}, metrics: metrics.NewNoopMetricsClient()}

	err := h.Handle(context.Background(), AcceptRide{RideID: "r1", DriverID: "d1", CallerUserID: "someone-else"})
	if !errors.Is(err, cmnerrors.ErrForbidden) {
		t.Fatalf("want ErrForbidden for mismatched caller, got %v", err)
	}
}

func TestAcceptRide_UnknownDriverIsForbidden(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	offers := &fakeOffers{currentOffer: map[string]string{}}
	drivers := &fakeDrivers{} // no cached userId for any driver
	h := &AcceptRideHandler{rides: rides, drivers: drivers, offers: offers, publisher: &fakePublisher{}, metrics: metrics.NewNoopMetricsClient()}

	err := h.Handle(context.Background(), AcceptRide{RideID: "r1", DriverID: "d1", CallerUserID: "u1"})
	if !errors.Is(err, cmnerrors.ErrForbidden) {
		t.Fatalf("want ErrForbidden for a driver with no cached userId, got %v", err)
	}
}
