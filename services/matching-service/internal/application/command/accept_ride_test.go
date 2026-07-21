package command

import (
	"context"
	"errors"
	"testing"

	cmnerrors "matching-service/internal/common/errors"
	"matching-service/internal/domain"
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
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	drivers := &fakeDrivers{}
	offers := &fakeOffers{currentOffer: map[string]string{"d1": "r1"}}
	pub := &fakePublisher{}
	h := &AcceptRideHandler{rides: rides, drivers: drivers, offers: offers, publisher: pub}

	if err := h.Handle(context.Background(), AcceptRide{RideID: "r1", DriverID: "d1"}); err != nil {
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
}

func TestAcceptRide_LostRaceIsErrRideTaken(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	offers := &fakeOffers{currentOffer: map[string]string{"d2": "r1"}, acceptedBy: "d1"}
	h := &AcceptRideHandler{rides: rides, drivers: &fakeDrivers{}, offers: offers, publisher: &fakePublisher{}}

	err := h.Handle(context.Background(), AcceptRide{RideID: "r1", DriverID: "d2"})
	if !errors.Is(err, cmnerrors.ErrRideTaken) {
		t.Fatalf("want ErrRideTaken, got %v", err)
	}
}

func TestAcceptRide_WinnerReacceptingIsErrRideTaken(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	drivers := &fakeDrivers{}
	offers := &fakeOffers{currentOffer: map[string]string{"d1": "r1"}}
	pub := &fakePublisher{}
	h := &AcceptRideHandler{rides: rides, drivers: drivers, offers: offers, publisher: pub}

	if err := h.Handle(context.Background(), AcceptRide{RideID: "r1", DriverID: "d1"}); err != nil {
		t.Fatalf("first accept should succeed, got %v", err)
	}

	// Same driver re-accepts the ride they already won. ClearCurrentOffer ran
	// as part of the first accept, so without the AcceptedBy short-circuit
	// this would fall through to the "no live offer" check and wrongly
	// return ErrOfferGone (400) instead of ErrRideTaken (409).
	err := h.Handle(context.Background(), AcceptRide{RideID: "r1", DriverID: "d1"})
	if !errors.Is(err, cmnerrors.ErrRideTaken) {
		t.Fatalf("want ErrRideTaken on re-accept by winner, got %v", err)
	}
}

func TestAcceptRide_NoLiveOfferIsErrOfferGone(t *testing.T) {
	rides := &fakeRides{ride: &domain.Ride{RideID: "r1", Status: domain.RideStatusSearching}}
	offers := &fakeOffers{currentOffer: map[string]string{}}
	h := &AcceptRideHandler{rides: rides, drivers: &fakeDrivers{}, offers: offers, publisher: &fakePublisher{}}

	err := h.Handle(context.Background(), AcceptRide{RideID: "r1", DriverID: "d9"})
	if !errors.Is(err, cmnerrors.ErrOfferGone) {
		t.Fatalf("want ErrOfferGone, got %v", err)
	}
}
