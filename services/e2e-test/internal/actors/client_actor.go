package actors

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	contracts "github.com/oxf/MyUber/contracts/http"

	"e2e-test/internal/apiclient"
)

// decoyUserID is a well-formed but never-signed-up user ID, used only to
// provoke the ownership check on someone else's ride.
const decoyUserID = "00000000-0000-0000-0000-000000000000"

// ClientActor simulates a rider: signup, login, then request rides forever,
// deep-verifying each one by reading it back. Rides never get a driver
// assigned today (matching-service doesn't assign yet), so the lifecycle
// stops at "ride exists, status Requested".
type ClientActor struct {
	Deps
	ID       string
	Email    string
	Interval time.Duration
	Rnd      *rand.Rand

	// pending is the last ride request sent, kept so the read-back can be
	// compared field by field.
	pending contracts.CreateRideRequest
}

func (a *ClientActor) Run(ctx context.Context) {
	acc := a.signupAndLogin(ctx, a.ID, a.Email, "E2E "+a.ID, a.randomPhone(), contracts.RoleClient, a.Rnd)
	if acc == nil {
		return
	}
	// Once, right after signup: the fresh account is guaranteed on page 1 of
	// the createdAt-desc default sort.
	a.verifyUserInList(ctx, acc)

	for iter := 1; sleepJitter(ctx, a.Interval, a.Rnd); iter++ {
		rideID := a.requestRide(ctx, acc)
		if rideID == "" {
			continue
		}
		if iter%3 == 0 {
			a.cancelAndVerifyRide(ctx, acc, rideID)
		} else {
			a.verifyRide(ctx, acc, rideID)
		}
		if iter%10 == 0 {
			a.refresh(ctx, a.ID, acc)
		}
		if iter%5 == 0 {
			a.verifyRideInList(ctx, rideID)
		}
	}
}

func (a *ClientActor) requestRide(ctx context.Context, acc *account) string {
	req := a.randomRideRequest()
	a.pending = req

	start := time.Now()
	resp, err := a.Ride.RequestRide(ctx, acc.userID, req)
	v := &Verify{}
	if err == nil {
		v.NotEmpty("rideId", resp.RideID)
		v.Eq("clientId", resp.ClientID, acc.userID)
		v.Eq("status", resp.Status, "Requested")
	}
	a.record(a.ID, "ride.request", start, err, v)
	if err != nil {
		return ""
	}
	return resp.RideID
}

func (a *ClientActor) verifyRide(ctx context.Context, acc *account, rideID string) {
	start := time.Now()
	ride, err := a.Ride.GetRide(ctx, rideID)
	v := &Verify{}
	if err == nil {
		v.Eq("id", ride.ID, rideID)
		v.Eq("clientId", ride.ClientID, acc.userID)
		v.Eq("status", ride.Status, "Requested")
		v.True("driverId", ride.DriverID == nil, "expected no driver assigned")
		v.EqFloat("pickup.lat", ride.Pickup.Latitude, a.pending.PickupLat)
		v.EqFloat("pickup.lng", ride.Pickup.Longitude, a.pending.PickupLng)
		v.Eq("pickup.address", ride.Pickup.Address, a.pending.PickupAddress)
		v.EqFloat("dest.lat", ride.Destination.Latitude, a.pending.DestLat)
		v.EqFloat("dest.lng", ride.Destination.Longitude, a.pending.DestLng)
		v.Eq("dest.address", ride.Destination.Address, a.pending.DestAddress)
		v.True("estimatedPrice", ride.EstimatedPrice > 0, "expected > 0")
	}
	a.record(a.ID, "ride.get", start, err, v)
}

// cancelAndVerifyRide deep-verifies the whole cancellation contract for a
// just-requested (still "Requested") ride: a non-owner can't cancel it, the
// owner can, the ride reads back as Cancelled, and a repeat cancel 409s.
func (a *ClientActor) cancelAndVerifyRide(ctx context.Context, acc *account, rideID string) {
	var apiErr *apiclient.APIError

	start := time.Now()
	_, err := a.Ride.CancelRide(ctx, decoyUserID, rideID, contracts.CancelRideRequest{})
	v := &Verify{}
	v.True("forbidden", errors.As(err, &apiErr) && apiErr.Status == http.StatusForbidden,
		"expected 403 cancelling someone else's ride")
	a.record(a.ID, "ride.cancel.forbidden", start, nil, v)

	start = time.Now()
	resp, err := a.Ride.CancelRide(ctx, acc.userID, rideID, contracts.CancelRideRequest{Reason: "e2e test cancel"})
	v = &Verify{}
	if err == nil {
		v.Eq("status", resp.Status, "Cancelled")
	}
	a.record(a.ID, "ride.cancel", start, err, v)
	if err != nil {
		return
	}

	start = time.Now()
	ride, err := a.Ride.GetRide(ctx, rideID)
	v = &Verify{}
	if err == nil {
		v.Eq("status", ride.Status, "Cancelled")
	}
	a.record(a.ID, "ride.get", start, err, v)

	start = time.Now()
	_, err = a.Ride.CancelRide(ctx, acc.userID, rideID, contracts.CancelRideRequest{})
	v = &Verify{}
	v.True("conflict", errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict,
		"expected 409 on repeat cancel")
	a.record(a.ID, "ride.cancel.conflict", start, nil, v)
}

func (a *ClientActor) verifyRideInList(ctx context.Context, rideID string) {
	start := time.Now()
	resp, err := a.Ride.ListRides(ctx, 1, 50)
	v := &Verify{}
	if err == nil {
		found := false
		for _, r := range resp.Items {
			if r.ID == rideID {
				found = true
				break
			}
		}
		v.True("list", found, fmt.Sprintf("ride %s not in first 50 of GET /ride", rideID))
		v.True("totalCount", resp.TotalCount >= 1, "expected totalCount >= 1")
	}
	a.record(a.ID, "ride.list", start, err, v)
}

func (a *ClientActor) verifyUserInList(ctx context.Context, acc *account) {
	start := time.Now()
	resp, err := a.Auth.ListUsers(ctx, 1, 50)
	v := &Verify{}
	if err == nil {
		found := false
		for _, u := range resp.Items {
			if u.ID == acc.userID {
				found = true
				break
			}
		}
		v.True("list", found, fmt.Sprintf("user %s not in first 50 of GET /users", acc.userID))
		v.True("totalCount", resp.TotalCount >= 1, "expected totalCount >= 1")
	}
	a.record(a.ID, "auth.users.list", start, err, v)
}

func (a *ClientActor) randomRideRequest() contracts.CreateRideRequest {
	n := a.Rnd.Intn(1000)
	return contracts.CreateRideRequest{
		PickupLat:     50.40 + a.Rnd.Float64()*0.1,
		PickupLng:     30.50 + a.Rnd.Float64()*0.1,
		PickupAddress: fmt.Sprintf("Pickup St %d", n),
		DestLat:       50.40 + a.Rnd.Float64()*0.1,
		DestLng:       30.50 + a.Rnd.Float64()*0.1,
		DestAddress:   fmt.Sprintf("Destination Ave %d", n),
	}
}

func (a *ClientActor) randomPhone() string {
	return fmt.Sprintf("+35799%07d", a.Rnd.Intn(10000000))
}
