package query

import (
	"context"
	"errors"
	"testing"
	"time"

	cmnerrors "matching-service/internal/common/errors"
	"matching-service/internal/domain"
)

type fakeOfferRides struct {
	domain.RideRepository
	ride *domain.Ride
}

func (f *fakeOfferRides) GetRide(ctx context.Context, id string) (*domain.Ride, error) {
	return f.ride, nil
}

type fakeOfferOffers struct {
	domain.OfferRepository
	rideID    string
	expiresAt time.Time
}

func (f *fakeOfferOffers) CurrentOffer(ctx context.Context, driverID string) (string, time.Time, error) {
	return f.rideID, f.expiresAt, nil
}

type fakeOfferDrivers struct {
	domain.DriverRepository
	userIDs map[string]string
}

func (f *fakeOfferDrivers) GetUserID(ctx context.Context, driverID string) (string, error) {
	return f.userIDs[driverID], nil
}

func TestGetDriverOffer_HappyPath(t *testing.T) {
	rides := &fakeOfferRides{ride: &domain.Ride{RideID: "r1"}}
	offers := &fakeOfferOffers{rideID: "r1", expiresAt: time.Now().Add(time.Minute)}
	drivers := &fakeOfferDrivers{userIDs: map[string]string{"d1": "u1"}}
	h := &GetDriverOfferHandler{rides: rides, offers: offers, drivers: drivers}

	offer, err := h.Handle(context.Background(), GetDriverOffer{DriverID: "d1", CallerUserID: "u1"})
	if err != nil {
		t.Fatal(err)
	}
	if offer.Ride.RideID != "r1" {
		t.Fatalf("unexpected offer: %+v", offer)
	}
}

func TestGetDriverOffer_CallerNotOwningDriverIsForbidden(t *testing.T) {
	rides := &fakeOfferRides{ride: &domain.Ride{RideID: "r1"}}
	offers := &fakeOfferOffers{rideID: "r1", expiresAt: time.Now().Add(time.Minute)}
	drivers := &fakeOfferDrivers{userIDs: map[string]string{"d1": "u1"}}
	h := &GetDriverOfferHandler{rides: rides, offers: offers, drivers: drivers}

	_, err := h.Handle(context.Background(), GetDriverOffer{DriverID: "d1", CallerUserID: "someone-else"})
	if !errors.Is(err, cmnerrors.ErrForbidden) {
		t.Fatalf("want ErrForbidden, got %v", err)
	}
}

func TestGetDriverOffer_UnknownDriverIsForbidden(t *testing.T) {
	rides := &fakeOfferRides{ride: &domain.Ride{RideID: "r1"}}
	offers := &fakeOfferOffers{rideID: "r1", expiresAt: time.Now().Add(time.Minute)}
	drivers := &fakeOfferDrivers{}
	h := &GetDriverOfferHandler{rides: rides, offers: offers, drivers: drivers}

	_, err := h.Handle(context.Background(), GetDriverOffer{DriverID: "d1", CallerUserID: "u1"})
	if !errors.Is(err, cmnerrors.ErrForbidden) {
		t.Fatalf("want ErrForbidden for a driver with no cached userId, got %v", err)
	}
}

func TestGetDriverOffer_NoLiveOfferIsErrNotFound(t *testing.T) {
	rides := &fakeOfferRides{ride: &domain.Ride{RideID: "r1"}}
	offers := &fakeOfferOffers{rideID: ""}
	drivers := &fakeOfferDrivers{userIDs: map[string]string{"d1": "u1"}}
	h := &GetDriverOfferHandler{rides: rides, offers: offers, drivers: drivers}

	_, err := h.Handle(context.Background(), GetDriverOffer{DriverID: "d1", CallerUserID: "u1"})
	if !errors.Is(err, cmnerrors.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
