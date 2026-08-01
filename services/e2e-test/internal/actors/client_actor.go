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

	// PaymentMethodToken picks the payment provider's outcome for every
	// charge attempt on this client's invoices — cmd/main.go resolves the
	// right fixture token for whichever PAYMENT_PROVIDER billing-service is
	// running (defaultPaymentMethodToken/declinePaymentMethodToken) and sets
	// this explicitly for every actor. The "pm_stub_ok" fallback below is
	// only a defensive default for the empty-string case; it is never what
	// actually selects the token in normal operation.
	PaymentMethodToken string

	// pending is the last ride request sent, kept so the read-back can be
	// compared field by field.
	pending contracts.CreateRideRequest

	// decoy is a second, genuinely signed-up account used only to provoke
	// the ownership check on someone else's ride. Kong derives X-User-Id
	// from a valid token's own claims (see gateway/kong.yml), so a spoofed
	// header no longer works — "someone else" has to be a real account.
	decoy *account
}

func (a *ClientActor) Run(ctx context.Context) {
	acc := a.signupAndLogin(ctx, a.ID, a.Email, "E2E "+a.ID, a.randomPhone(), contracts.RoleClient, a.Rnd)
	if acc == nil {
		return
	}
	// Once, right after signup: the fresh account is guaranteed on page 1 of
	// the createdAt-desc default sort.
	a.verifyUserInList(ctx, acc)
	a.verifyMe(ctx, acc)
	a.attachAndVerifyPaymentMethod(ctx, acc)

	a.decoy = a.signupAndLogin(ctx, a.ID+"-decoy", "decoy-"+a.Email, "E2E "+a.ID+" decoy", a.randomPhone(), contracts.RoleClient, a.Rnd)
	if a.decoy == nil {
		return
	}
	a.logoutDecoyAndVerify(ctx)

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
	resp, err := a.Ride.RequestRide(ctx, acc.accessToken, req)
	v := &Verify{}
	if err == nil {
		v.NotEmpty("rideId", resp.RideID)
		v.Eq("clientId", resp.ClientID, acc.clientID)
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
	ride, err := a.Ride.GetRide(ctx, acc.accessToken, rideID)
	v := &Verify{}
	if err == nil {
		v.Eq("id", ride.ID, rideID)
		v.Eq("clientId", ride.ClientID, acc.clientID)
		v.Eq("status", ride.Status, "Requested")
		v.True("driverId", ride.DriverID == nil, "expected no driver assigned")
		v.EqFloat("pickup.lat", ride.Pickup.Latitude, a.pending.PickupLat)
		v.EqFloat("pickup.lng", ride.Pickup.Longitude, a.pending.PickupLng)
		v.Eq("pickup.address", ride.Pickup.Address, a.pending.PickupAddress)
		v.EqFloat("dest.lat", ride.Destination.Latitude, a.pending.DestLat)
		v.EqFloat("dest.lng", ride.Destination.Longitude, a.pending.DestLng)
		v.Eq("dest.address", ride.Destination.Address, a.pending.DestAddress)
		v.True("estimatedPriceMinor", ride.EstimatedPriceMinor > 0, "expected > 0")
	}
	a.record(a.ID, "ride.get", start, err, v)
}

// cancelAndVerifyRide deep-verifies the whole cancellation contract for a
// just-requested (still "Requested") ride: a non-owner can't cancel it, the
// owner can, the ride reads back as Cancelled, and a repeat cancel 409s.
func (a *ClientActor) cancelAndVerifyRide(ctx context.Context, acc *account, rideID string) {
	var apiErr *apiclient.APIError

	start := time.Now()
	_, err := a.Ride.CancelRide(ctx, a.decoy.accessToken, rideID, contracts.CancelRideRequest{})
	v := &Verify{}
	v.True("forbidden", errors.As(err, &apiErr) && apiErr.Status == http.StatusForbidden,
		"expected 403 cancelling someone else's ride")
	a.record(a.ID, "ride.cancel.forbidden", start, nil, v)

	start = time.Now()
	resp, err := a.Ride.CancelRide(ctx, acc.accessToken, rideID, contracts.CancelRideRequest{Reason: "e2e test cancel"})
	v = &Verify{}
	if err == nil {
		v.Eq("status", resp.Status, "Cancelled")
	}
	a.record(a.ID, "ride.cancel", start, err, v)
	if err != nil {
		return
	}

	start = time.Now()
	ride, err := a.Ride.GetRide(ctx, acc.accessToken, rideID)
	v = &Verify{}
	if err == nil {
		v.Eq("status", ride.Status, "Cancelled")
	}
	a.record(a.ID, "ride.get", start, err, v)

	start = time.Now()
	_, err = a.Ride.CancelRide(ctx, acc.accessToken, rideID, contracts.CancelRideRequest{})
	v = &Verify{}
	v.True("conflict", errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict,
		"expected 409 on repeat cancel")
	a.record(a.ID, "ride.cancel.conflict", start, nil, v)
}

// verifyRideInList uses the shared admin token: GET /ride is Admin-only at
// the Kong gateway now (see gateway/kong.yml), not reachable with the
// client's own token.
func (a *ClientActor) verifyRideInList(ctx context.Context, rideID string) {
	start := time.Now()
	resp, err := a.Ride.ListRides(ctx, a.Deps.AdminAccessToken, 1, 50)
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

// verifyMe confirms GET /me round-trips the caller's own id/email/role —
// derived from the bearer token's own claims, not any spoofable header (see
// gateway/kong.yml's inject_user_headers post-function).
func (a *ClientActor) verifyMe(ctx context.Context, acc *account) {
	start := time.Now()
	me, err := a.Auth.Me(ctx, acc.accessToken)
	v := &Verify{}
	if err == nil {
		v.Eq("id", me.ID, acc.userID)
		v.Eq("email", me.Email, a.Email)
		v.Eq("role", string(me.Role), string(contracts.RoleClient))
		v.True("clientId", me.ClientId != nil && *me.ClientId != "", "expected a non-empty clientId for a Client")
	}
	a.record(a.ID, "auth.me", start, err, v)
}

// logoutDecoyAndVerify logs the decoy account out, then confirms its refresh
// token is rejected afterward — proving /logout actually revokes it.
func (a *ClientActor) logoutDecoyAndVerify(ctx context.Context) {
	start := time.Now()
	err := a.Auth.Logout(ctx, a.decoy.accessToken, contracts.LogoutRequest{RefreshToken: a.decoy.refreshToken})
	a.record(a.ID, "auth.logout", start, err, nil)
	if err != nil {
		return
	}

	start = time.Now()
	var apiErr *apiclient.APIError
	_, err = a.Auth.Refresh(ctx, contracts.RefreshRequest{RefreshToken: a.decoy.refreshToken})
	v := &Verify{}
	v.True("unauthorized", errors.As(err, &apiErr) && apiErr.Status == http.StatusUnauthorized,
		"expected 401 refreshing a revoked token")
	a.record(a.ID, "auth.logout.refresh_rejected", start, nil, v)
}

// verifyUserInList uses the shared admin token: GET /users is Admin-only at
// the Kong gateway now (see gateway/kong.yml), not reachable with the
// client's own token.
func (a *ClientActor) verifyUserInList(ctx context.Context, acc *account) {
	start := time.Now()
	resp, err := a.Auth.ListUsers(ctx, a.Deps.AdminAccessToken, 1, 50)
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
	tariffName := "Standard"
	if n%5 == 0 {
		// Occasionally exercise the USD tariff so a client accumulates
		// rides in two currencies (BILLING_SPEC.md §10: a client with one
		// EUR and one USD ride must have two distinct client_receivable
		// balances, never a single summed one).
		tariffName = "Standard USD"
	}
	return contracts.CreateRideRequest{
		PickupLat:     50.40 + a.Rnd.Float64()*0.1,
		PickupLng:     30.50 + a.Rnd.Float64()*0.1,
		PickupAddress: fmt.Sprintf("Pickup St %d", n),
		DestLat:       50.40 + a.Rnd.Float64()*0.1,
		DestLng:       30.50 + a.Rnd.Float64()*0.1,
		DestAddress:   fmt.Sprintf("Destination Ave %d", n),
		TariffName:    tariffName,
	}
}

// attachAndVerifyPaymentMethod adds this client's billing payment method
// (the success token by default, the decline token for the dedicated
// declining client actor — see cmd/main.go's provider-aware token mapping)
// right after signup, so every ride this client completes has something
// for the ChargeWorker to charge.
func (a *ClientActor) attachAndVerifyPaymentMethod(ctx context.Context, acc *account) {
	token := a.PaymentMethodToken
	if token == "" {
		token = "pm_stub_ok"
	}

	start := time.Now()
	added, err := a.Billing.AddPaymentMethod(ctx, acc.accessToken, contracts.AddPaymentMethodRequest{
		ProviderPaymentMethodId: token,
		Brand:                   "visa",
		Last4:                   "4242",
		ExpMonth:                12,
		ExpYear:                 2030,
		SetDefault:              true,
	})
	v := &Verify{}
	if err == nil {
		v.NotEmpty("id", added.Id)
	}
	a.record(a.ID, "billing.paymentmethod.add", start, err, v)
	if err != nil {
		return
	}

	start = time.Now()
	methods, err := a.Billing.ListPaymentMethods(ctx, acc.accessToken)
	v = &Verify{}
	if err == nil {
		found := false
		for _, m := range methods {
			if m.Id == added.Id {
				found = true
				v.Eq("isDefault", m.IsDefault, true)
				v.Eq("status", m.Status, "active")
			}
		}
		v.True("found", found, "expected the just-added method in the list")
	}
	a.record(a.ID, "billing.paymentmethod.list", start, err, v)
}

func (a *ClientActor) randomPhone() string {
	return fmt.Sprintf("+35799%07d", a.Rnd.Intn(10000000))
}
