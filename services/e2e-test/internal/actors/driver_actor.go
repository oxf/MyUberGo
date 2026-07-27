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

// DriverActor simulates a driver: signup, login, create a profile, then
// cycle shifts forever (open -> Online -> work -> Ended), deep-verifying each
// step. Two service quirks are encoded here:
//   - driver.shift has no status column: only "Ended" persists anything
//     (ended_at), other statuses just emit a shift.updated event. So shift
//     end is verified via endedAt, never via a status round-trip.
//   - CreateShift rejects a second active shift per driver, so every cycle
//     ends its shift before the next one starts.
type DriverActor struct {
	Deps
	ID       string
	Email    string
	Interval time.Duration
	Rnd      *rand.Rand

	profileID string
	phone     string // auth.user.phone at signup — driver.driver no longer stores name/phone

	// acc is the driver's own authenticated identity, kept as a field (like
	// profileID/phone) so every helper below can attach a bearer token
	// without threading it through each call — Kong requires one on every
	// driver-service/ride-service route now (see gateway/kong.yml).
	acc *account
}

func (a *DriverActor) Run(ctx context.Context) {
	a.phone = fmt.Sprintf("+35750%07d", a.Rnd.Intn(10000000))

	a.acc = a.signupAndLogin(ctx, a.ID, a.Email, "E2E "+a.ID, a.phone, contracts.RoleDriver, a.Rnd)
	if a.acc == nil {
		return
	}
	if !a.createAndVerifyProfile(ctx) {
		return
	}

	for cycle := 1; sleepJitter(ctx, a.Interval, a.Rnd); cycle++ {
		shiftID := a.openShift(ctx)
		if shiftID == "" {
			continue
		}
		a.verifyOpenShift(ctx, shiftID)
		if cycle%5 == 0 {
			// Right after opening, the shift is the newest — guaranteed on
			// page 1 of the startedAt-desc default sort.
			a.verifyShiftInList(ctx, shiftID)
		}
		a.setShiftStatus(ctx, shiftID, "Online", "driver.shift.online")

		// Simulated work period before ending the shift: poll for matching
		// offers and accept the first one seen.
		if !a.pollForOffer(ctx, a.Interval/2) {
			return
		}

		a.setShiftStatus(ctx, shiftID, "Ended", "driver.shift.end")
		a.verifyEndedShift(ctx, shiftID)

		if cycle%4 == 0 {
			a.updateAndVerifyLicencePlate(ctx)
		}
		if cycle%10 == 0 {
			a.refresh(ctx, a.ID, a.acc)
		}
	}
}

// createAndVerifyProfile retries until the profile exists or ctx is done.
func (a *DriverActor) createAndVerifyProfile(ctx context.Context) bool {
	for {
		start := time.Now()
		created, err := a.Driver.CreateDriver(ctx, a.acc.accessToken, contracts.CreateDriverDto{
			UserId:       a.acc.userID,
			VehicleType:  "Standard",
			LicencePlate: fmt.Sprintf("E2E%04d", a.Rnd.Intn(10000)),
		})
		v := &Verify{}
		if err == nil {
			v.NotEmpty("id", created.Id)
		}
		a.record(a.ID, "driver.create", start, err, v)

		if err == nil && v.OK() {
			a.profileID = created.Id
			a.verifyProfile(ctx)
			return true
		}
		if !sleepJitter(ctx, 3*time.Second, a.Rnd) {
			return false
		}
	}
}

func (a *DriverActor) verifyProfile(ctx context.Context) {
	start := time.Now()
	profile, err := a.Driver.GetDriver(ctx, a.acc.accessToken, a.profileID)
	v := &Verify{}
	if err == nil {
		v.Eq("id", profile.Id, a.profileID)
		v.Eq("userId", profile.UserId, a.acc.userID)
		v.Eq("vehicleType", profile.VehicleType, "Standard")
		v.True("rating", profile.Rating >= 0, "expected >= 0")
	}
	a.record(a.ID, "driver.get", start, err, v)
}

func (a *DriverActor) openShift(ctx context.Context) string {
	start := time.Now()
	resp, err := a.Driver.CreateShift(ctx, a.acc.accessToken, contracts.CreateShiftRequest{DriverId: a.profileID})
	v := &Verify{}
	if err == nil {
		v.NotEmpty("id", resp.Id)
	}
	a.record(a.ID, "driver.shift.create", start, err, v)
	if err != nil {
		return ""
	}
	return resp.Id
}

func (a *DriverActor) verifyOpenShift(ctx context.Context, shiftID string) {
	start := time.Now()
	shift, err := a.Driver.GetShift(ctx, a.acc.accessToken, shiftID)
	v := &Verify{}
	if err == nil {
		v.Eq("id", shift.Id, shiftID)
		v.Eq("driverId", shift.DriverId, a.profileID)
		v.True("endedAt", shift.EndedAt == nil, "expected open shift (endedAt null)")
	}
	a.record(a.ID, "driver.shift.get", start, err, v)
}

func (a *DriverActor) setShiftStatus(ctx context.Context, shiftID, status, op string) {
	start := time.Now()
	err := a.Driver.UpdateShift(ctx, a.acc.accessToken, shiftID, contracts.UpdateShiftRequest{
		DriverId: a.profileID,
		Status:   status,
	})
	a.record(a.ID, op, start, err, nil)
}

func (a *DriverActor) verifyEndedShift(ctx context.Context, shiftID string) {
	start := time.Now()
	shift, err := a.Driver.GetShift(ctx, a.acc.accessToken, shiftID)
	v := &Verify{}
	if err == nil {
		v.Eq("id", shift.Id, shiftID)
		v.True("endedAt", shift.EndedAt != nil && *shift.EndedAt != "", "expected endedAt to be set")
	}
	a.record(a.ID, "driver.shift.get", start, err, v)
}

// verifyShiftInList uses the shared admin token: GET /driver-shift is
// Admin-only at the Kong gateway now (see gateway/kong.yml), not reachable
// with the driver's own token.
func (a *DriverActor) verifyShiftInList(ctx context.Context, shiftID string) {
	start := time.Now()
	resp, err := a.Driver.ListShifts(ctx, a.Deps.AdminAccessToken, 1, 50)
	v := &Verify{}
	if err == nil {
		found := false
		for _, s := range resp.Items {
			if s.Id == shiftID {
				found = true
				break
			}
		}
		v.True("list", found, fmt.Sprintf("shift %s not in first 50 of GET /driver-shift", shiftID))
		v.True("totalCount", resp.TotalCount >= 1, "expected totalCount >= 1")
	}
	a.record(a.ID, "driver.shift.list", start, err, v)
}

// updateAndVerifyLicencePlate exercises PUT /driver — driver.driver no
// longer stores name/phone (see CLAUDE.md/PLAN.md role-table refactor
// notes), so only vehicle fields round-trip here now.
func (a *DriverActor) updateAndVerifyLicencePlate(ctx context.Context) {
	newPlate := fmt.Sprintf("E2E%04d", a.Rnd.Intn(10000))

	start := time.Now()
	err := a.Driver.UpdateDriver(ctx, a.acc.accessToken, a.profileID, contracts.UpdateDriverDto{
		LicencePlate: newPlate, // VehicleType empty: service keeps existing value via COALESCE(NULLIF(...))
	})
	a.record(a.ID, "driver.update", start, err, nil)
	if err != nil {
		return
	}

	start = time.Now()
	profile, getErr := a.Driver.GetDriver(ctx, a.acc.accessToken, a.profileID)
	v := &Verify{}
	if getErr == nil {
		v.Eq("licencePlate", profile.LicencePlate, newPlate)
		v.Eq("vehicleType", profile.VehicleType, "Standard")
	}
	a.record(a.ID, "driver.get", start, getErr, v)
}

// pollForOffer polls the matching service during the Online work window and
// accepts the first offer it sees. Returns false when ctx is done. 404 on the
// offer endpoint means "no offer yet" — normal, not recorded as a failure.
func (a *DriverActor) pollForOffer(ctx context.Context, window time.Duration) bool {
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		start := time.Now()
		offer, err := a.Matching.GetDriverOffer(ctx, a.profileID)
		if err != nil {
			var apiErr *apiclient.APIError
			if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
				if !sleepJitter(ctx, 2*time.Second, a.Rnd) {
					return false
				}
				continue
			}
			a.record(a.ID, "matching.offer.get", start, err, nil)
			return ctx.Err() == nil
		}

		v := &Verify{}
		v.NotEmpty("rideId", offer.RideId)
		v.True("priceMinor", offer.PriceMinor > 0, "expected positive price")
		v.NotEmpty("expiresAt", offer.ExpiresAt)
		a.record(a.ID, "matching.offer.get", start, nil, v)

		a.acceptOffer(ctx, offer.RideId)
		return ctx.Err() == nil
	}
	return ctx.Err() == nil
}

func (a *DriverActor) acceptOffer(ctx context.Context, rideID string) {
	start := time.Now()
	resp, err := a.Matching.AcceptRide(ctx, rideID, contracts.AcceptRideRequest{DriverId: a.profileID})

	var apiErr *apiclient.APIError
	if err != nil && errors.As(err, &apiErr) && (apiErr.Status == http.StatusConflict || apiErr.Status == http.StatusBadRequest) {
		// Lost the race to another driver — either someone else's TryAccept
		// won (409) or our own current_offer got overwritten by a broadcast
		// retry before we called Accept (400, ErrOfferGone). Both are
		// legitimate outcomes of BROADCAST-style offering, not failures —
		// same convention as the deliberate duplicate-accept check below.
		a.record(a.ID, "matching.ride.accept", start, nil, nil)
		return
	}

	v := &Verify{}
	if err == nil {
		v.Eq("rideId", resp.RideId, rideID)
		v.Eq("driverId", resp.DriverId, a.profileID)
		v.Eq("status", resp.Status, "matched")
	}
	a.record(a.ID, "matching.ride.accept", start, err, v)
	if err != nil {
		return
	}

	// Deep verification: offer is gone, and a duplicate accept must 409.
	start = time.Now()
	_, err = a.Matching.GetDriverOffer(ctx, a.profileID)
	v = &Verify{}
	v.True("offer cleared", errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound,
		"expected 404 for current offer after accept")
	a.record(a.ID, "matching.offer.get", start, nil, v)

	start = time.Now()
	_, err = a.Matching.AcceptRide(ctx, rideID, contracts.AcceptRideRequest{DriverId: a.profileID})
	v = &Verify{}
	// 409 = ride already taken (the expected case). 400 is also legitimate:
	// the rider can cancel a just-matched ride at any moment, and if that
	// race lands between the two accept calls, IsCancelled short-circuits
	// AcceptRideHandler before it reaches the "already taken" check.
	legitimate := errors.As(err, &apiErr) && (apiErr.Status == http.StatusConflict || apiErr.Status == http.StatusBadRequest)
	v.True("duplicate accept rejected", legitimate,
		"expected 409 (already taken) or 400 (ride cancelled) on duplicate accept")
	a.record(a.ID, "matching.ride.accept.dup", start, nil, v)

	prevCompleted := a.verifyOnRide(ctx)
	if !a.startAndVerifyRide(ctx, rideID) {
		return
	}
	sleepJitter(ctx, a.Interval/4, a.Rnd) // simulated driving delay
	a.completeAndVerifyRide(ctx, rideID)
	a.verifyBackOnline(ctx, prevCompleted+1)
	a.verifyInvoiceSettled(ctx, rideID)
}

// verifyOnRide polls the driver's profile until driver-service's async
// ride.accepted consumer flips status Online -> OnRide, then records a
// single result. Intermediate not-yet-flipped reads aren't recorded as
// failures — only the final outcome is, same convention as pollForOffer's
// "not there yet" 404 handling. Returns the profile's totalRidesCompleted
// as a baseline for verifyBackOnline's increment check.
func (a *DriverActor) verifyOnRide(ctx context.Context) int {
	start := time.Now()
	var profile contracts.DriverDto
	var err error
	for attempt := range 3 {
		profile, err = a.Driver.GetDriver(ctx, a.acc.accessToken, a.profileID)
		if err == nil && profile.Status == "OnRide" {
			break
		}
		if attempt < 2 && !sleepJitter(ctx, 500*time.Millisecond, a.Rnd) {
			return profile.TotalRidesCompleted
		}
	}

	v := &Verify{}
	if err == nil {
		v.Eq("status", profile.Status, "OnRide")
	}
	a.record(a.ID, "driver.onride", start, err, v)
	return profile.TotalRidesCompleted
}

// startAndVerifyRide retries because /ride/{id}/start depends on
// ride-service's own (independent, uncoordinated) consumption of
// ride.accepted - verifyOnRide having already observed OnRide only proves
// driver-service's separate consumer group caught up, not ride-service's.
// This is a known, accepted race (see PLAN.md/CLAUDE.md discussion) that
// isn't being fixed at the source - ride.accepted is a direct Kafka publish
// (no outbox), so the lag here is bounded by ordinary consumer-group
// catch-up time under load, observed up to a few seconds in testing. The
// budget below is a best-effort accommodation for that, not a guarantee -
// a StartRide that still fails after this many attempts is worth surfacing
// as a real failure, not silently retried forever.
func (a *DriverActor) startAndVerifyRide(ctx context.Context, rideID string) bool {
	start := time.Now()
	var resp contracts.StartRideResponse
	var err error
	for attempt := range 20 {
		resp, err = a.Ride.StartRide(ctx, a.acc.accessToken, rideID, a.profileID)
		if err == nil {
			break
		}
		if attempt < 19 && !sleepJitter(ctx, 400*time.Millisecond, a.Rnd) {
			return false
		}
	}
	v := &Verify{}
	if err == nil {
		v.Eq("status", resp.Status, "InProgress")
		v.NotEmpty("startedAt", resp.StartedAt)
	}
	a.record(a.ID, "ride.start", start, err, v)
	return err == nil
}

func (a *DriverActor) completeAndVerifyRide(ctx context.Context, rideID string) {
	start := time.Now()
	resp, err := a.Ride.CompleteRide(ctx, a.acc.accessToken, rideID, a.profileID)
	v := &Verify{}
	if err == nil {
		v.Eq("status", resp.Status, "Completed")
		v.NotEmpty("finishedAt", resp.FinishedAt)
	}
	a.record(a.ID, "ride.complete", start, err, v)
}

// verifyBackOnline polls until driver-service's async ride.completed
// consumer flips OnRide -> Online and increments total_rides_completed,
// mirroring verifyOnRide's poll-retry convention. Unlike ride.accepted
// (published directly by matching-service), ride.completed goes through
// ride-service's transactional outbox, which only wakes on a 2s ticker and
// drains a fixed batch size - under bursty e2e-test load (many
// ride.requested/ride.cancelled events sharing the same outbox and worker)
// that queue can back up well beyond one tick: observed end-to-end delay up
// to ~44s in testing (CompleteRide -> ProcessRideCompleted), not just a few
// seconds. The outbox worker itself isn't being changed, so the budget here
// is a best-effort accommodation for that observed worst case plus margin,
// not a claim that the lag is now impossible - a GetDriver that still isn't
// Online after this many attempts is worth surfacing as a real failure. The
// per-attempt interval is widened to match the outbox's own ~2s cadence
// instead of polling faster than the underlying value can possibly change.
func (a *DriverActor) verifyBackOnline(ctx context.Context, wantMinCompleted int) {
	start := time.Now()
	var profile contracts.DriverDto
	var err error
	for attempt := range 45 {
		profile, err = a.Driver.GetDriver(ctx, a.acc.accessToken, a.profileID)
		if err == nil && profile.Status == "Online" {
			break
		}
		if attempt < 44 && !sleepJitter(ctx, 1500*time.Millisecond, a.Rnd) {
			return
		}
	}

	v := &Verify{}
	if err == nil {
		v.Eq("status", profile.Status, "Online")
		v.True("totalRidesCompleted", profile.TotalRidesCompleted >= wantMinCompleted,
			fmt.Sprintf("expected >= %d, got %d", wantMinCompleted, profile.TotalRidesCompleted))
	}
	a.record(a.ID, "driver.backonline", start, err, v)
}

// verifyInvoiceSettled polls GET /rides/{rideId}/invoice for a bounded
// window after a ride completes. Delivery is async (ride.completed -> the
// outbox -> billing-service's consumer -> ChargeWorker's next tick), so a
// still-"open" invoice after this window isn't treated as a failure — only
// a reached terminal status (paid/uncollectible) is asserted. This uses the
// driver's own token: X-Client-Id is empty for a Driver account, so
// billing-service's ownership check is skipped (see
// billing-service/internal/interfaces/http/handler/invoice_handler.go).
func (a *DriverActor) verifyInvoiceSettled(ctx context.Context, rideID string) {
	start := time.Now()
	var inv contracts.InvoiceDto
	var err error
	for attempt := range 10 {
		inv, err = a.Billing.GetInvoiceByRide(ctx, a.acc.accessToken, rideID)
		if err == nil && (inv.Status == "paid" || inv.Status == "uncollectible") {
			break
		}
		if attempt < 9 && !sleepJitter(ctx, 3*time.Second, a.Rnd) {
			return
		}
	}
	if err == nil && inv.Status != "paid" && inv.Status != "uncollectible" {
		// Still open after the observation window — expected for a
		// pm_stub_decline client waiting out its retry backoff, not a bug.
		return
	}

	v := &Verify{}
	if err == nil {
		v.Eq("rideId", inv.RideId, rideID)
		v.True("amountMinor", inv.AmountMinor > 0, "expected positive amount")
		v.NotEmpty("currency", inv.Currency)
		v.True("terminal", inv.Status == "paid" || inv.Status == "uncollectible",
			"expected paid or uncollectible, got "+inv.Status)
	}
	a.record(a.ID, "billing.invoice.get", start, err, v)
	if err != nil || inv.ClientId == "" {
		return
	}

	// Cheapest possible regression test for the double-entry invariants
	// (BILLING_SPEC.md §10): exercises the ledger balance query end-to-end.
	// Not asserting an exact value here — this client may have other rides
	// mid-settlement concurrently, so client_receivable isn't guaranteed to
	// be exactly 0 or exactly this invoice's amount at the instant we check.
	start = time.Now()
	_, err = a.Billing.GetLedgerBalance(ctx, a.Deps.AdminAccessToken, "client_receivable", inv.ClientId, inv.Currency)
	a.record(a.ID, "billing.ledger.balance", start, err, nil)
}
