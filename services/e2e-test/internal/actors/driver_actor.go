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

// DriverActor cycles shifts forever (open -> Online -> work -> Ended), deep-verifying each step.
// Shift end is checked via endedAt (no status column); each cycle ends its shift before the next starts.
type DriverActor struct {
	Deps
	ID       string
	Email    string
	Interval time.Duration
	Rnd      *rand.Rand

	profileID string
	phone     string // auth.user.phone at signup — driver.driver no longer stores name/phone

	// acc holds the driver's identity so helpers can attach a bearer token without
	// threading it through every call — Kong requires one on every route.
	acc *account

	// position is lazily seeded on the first ping, then advanced tick by tick for the
	// actor's lifetime — a fresh random position per ping would defeat the movement model.
	position *driverPosition
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

// verifyShiftInList uses the shared admin token: GET /driver-shift is Admin-only at
// Kong, not reachable with the driver's own token.
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

// updateAndVerifyLicencePlate exercises PUT /driver — driver.driver no longer stores
// name/phone, so only vehicle fields round-trip here.
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

// pollForOffer polls during the Online window and accepts the first offer seen (404 = "no
// offer yet", not a failure), emitting one GPS ping per iteration to stay geo-discoverable.
func (a *DriverActor) pollForOffer(ctx context.Context, window time.Duration) bool {
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		a.emitLocationPing(ctx)

		start := time.Now()
		offer, err := a.Matching.GetDriverOffer(ctx, a.acc.accessToken, a.profileID)
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

// emitLocationPing sends one simulated GPS ping, advancing position by real elapsed time so
// displacement matches claimed speed; deviceTs uses RFC3339Nano so same-second calls don't collide.
func (a *DriverActor) emitLocationPing(ctx context.Context) {
	now := time.Now().UTC()
	if a.position == nil {
		a.position = newDriverPosition(a.Rnd, rideBoxLat, rideBoxLon, rideBoxSpanDeg, now)
	}
	lat, lon := a.position.advanceTo(now, a.Rnd)

	start := time.Now()
	resp, err := a.Location.IngestBatch(ctx, a.acc.accessToken, contracts.LocationBatchRequest{
		Pings: []contracts.LocationPingDto{{
			Lat:        lat,
			Lon:        lon,
			AccuracyM:  10,
			HeadingDeg: a.position.bearingDeg,
			SpeedMps:   a.position.speedMps,
			DeviceTs:   now.Format(time.RFC3339Nano),
		}},
	})
	v := &Verify{}
	if err == nil {
		v.Eq("accepted", resp.Accepted, 1)
		v.Eq("rejected", resp.Rejected, 0)
	}
	a.record(a.ID, "location.ping", start, err, v)
}

func (a *DriverActor) acceptOffer(ctx context.Context, rideID string) {
	start := time.Now()
	resp, err := a.Matching.AcceptRide(ctx, a.acc.accessToken, rideID, contracts.AcceptRideRequest{DriverId: a.profileID})

	var apiErr *apiclient.APIError
	if err != nil && errors.As(err, &apiErr) && (apiErr.Status == http.StatusConflict || apiErr.Status == http.StatusBadRequest) {
		// Lost the race: another driver's accept won (409) or our offer got overwritten by a
		// broadcast retry (400, ErrOfferGone) — both legitimate outcomes of BROADCAST offering.
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
	_, err = a.Matching.GetDriverOffer(ctx, a.acc.accessToken, a.profileID)
	v = &Verify{}
	v.True("offer cleared", errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound,
		"expected 404 for current offer after accept")
	a.record(a.ID, "matching.offer.get", start, nil, v)

	start = time.Now()
	_, err = a.Matching.AcceptRide(ctx, a.acc.accessToken, rideID, contracts.AcceptRideRequest{DriverId: a.profileID})
	v = &Verify{}
	// 409 = already taken (expected). 400 is also legitimate: a rider cancel racing between
	// the two accept calls makes IsCancelled short-circuit before the "already taken" check.
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
}

// verifyOnRide polls until driver-service's ride.accepted consumer flips Online -> OnRide,
// recording only the final outcome; returns totalRidesCompleted as a baseline for verifyBackOnline.
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

// startAndVerifyRide retries since ride-service consumes ride.accepted independently of
// driver-service's own consumer group; a known, accepted lag (up to a few seconds observed), not fixed at the source.
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

// verifyBackOnline polls until ride.completed flips OnRide -> Online and increments
// total_rides_completed; unlike ride.accepted, this goes through ride-service's outbox (2s ticker, batched), observed up to ~44s under load — budget sized for that plus margin.
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
