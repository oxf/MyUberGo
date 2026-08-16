package actors

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	contracts "github.com/oxf/MyUber/contracts/http"

	"e2e-test/internal/apiclient"
	"e2e-test/internal/stats"
)

// RunLocationRadiusScenario proves 4 location-service behaviors (in/out-of-range, staleness eviction,
// offline exclusion, radius expansion) via public APIs, run concurrently; never accepts/starts a ride.
func RunLocationRadiusScenario(ctx context.Context, deps Deps, seed int64) bool {
	log.Println("scenario: location-radius — starting (expect ~2.5-3 minutes; dominated by location-service's staleness window)")

	type namedFunc struct {
		name string
		fn   func(context.Context, Deps, *rand.Rand) scenarioResult
	}
	assertions := []namedFunc{
		{"in-range vs out-of-range", runInRangeAssertion},
		{"offline but pinging", runOfflineAssertion},
		{"radius expansion", runExpansionAssertion},
		{"staleness eviction", runStalenessAssertion},
	}

	results := make([]scenarioResult, len(assertions))
	var wg sync.WaitGroup
	for i, a := range assertions {
		wg.Add(1)
		go func(i int, a namedFunc) {
			defer wg.Done()
			results[i] = a.fn(ctx, deps, rand.New(rand.NewSource(seed+int64(i)+1)))
		}(i, a)
	}
	wg.Wait()

	allPassed := true
	for _, r := range results {
		status := "PASS"
		if !r.passed {
			status = "FAIL"
			allPassed = false
		}
		log.Printf("scenario: [%s] %s — %s", status, r.name, r.detail)
		deps.Stats.Record(stats.Event{
			Actor: "scenario", Op: r.name, OK: r.passed, VerifyOK: r.passed, Detail: r.detail,
		})
	}

	if allPassed {
		log.Println("scenario: location-radius — ALL ASSERTIONS PASSED")
	} else {
		log.Println("scenario: location-radius — ONE OR MORE ASSERTIONS FAILED")
	}
	return allPassed
}

type scenarioResult struct {
	name   string
	passed bool
	detail string
}

func fail(name, format string, args ...any) scenarioResult {
	return scenarioResult{name: name, passed: false, detail: fmt.Sprintf(format, args...)}
}

func pass(name, format string, args ...any) scenarioResult {
	return scenarioResult{name: name, passed: true, detail: fmt.Sprintf(format, args...)}
}

// --- Assertion 1: in-range vs out-of-range -------------------------------

func runInRangeAssertion(ctx context.Context, deps Deps, rnd *rand.Rand) scenarioResult {
	const name = "in-range vs out-of-range"
	// Pickup points are spaced 1° latitude apart (~111km), well beyond maxRadiusKm (15km) plus
	// driver offset, so no assertion's ride can pick up another's driver as a stray candidate.
	const pickupLat, pickupLon = 50.00, 30.00

	accIn := scenarioSignupDriver(ctx, deps, rnd, "scenario-inrange-in")
	accOut := scenarioSignupDriver(ctx, deps, rnd, "scenario-inrange-out")
	if accIn == nil || accOut == nil {
		return fail(name, "driver signup failed")
	}

	driverIn, ok1 := scenarioCreateDriverProfile(ctx, deps, accIn, rnd)
	driverOut, ok2 := scenarioCreateDriverProfile(ctx, deps, accOut, rnd)
	if !ok1 || !ok2 {
		return fail(name, "driver profile creation failed")
	}

	shiftIn, ok3 := scenarioGoOnline(ctx, deps, accIn, driverIn)
	shiftOut, ok4 := scenarioGoOnline(ctx, deps, accOut, driverOut)
	if !ok3 || !ok4 {
		return fail(name, "shift open/online failed")
	}
	defer scenarioGoOffline(ctx, deps, accIn, shiftIn, driverIn)
	defer scenarioGoOffline(ctx, deps, accOut, shiftOut, driverOut)

	// ~2km north: inside attempt-1's 5km radius.
	latIn, lonIn := pointAtDistance(pickupLat, pickupLon, 0, 2)
	// ~10km east: outside even attempt-3's 9km radius.
	latOut, lonOut := pointAtDistance(pickupLat, pickupLon, 90, 10)

	if !scenarioPingUntilAccepted(ctx, deps, accIn, latIn, lonIn, rnd) ||
		!scenarioPingUntilAccepted(ctx, deps, accOut, latOut, lonOut, rnd) {
		return fail(name, "initial ping never accepted (owner mapping never propagated from shift.updated)")
	}

	client := scenarioSignupClient(ctx, deps, rnd, "scenario-inrange-client")
	if client == nil {
		return fail(name, "client signup failed")
	}
	rideID, ok := scenarioRequestRide(ctx, deps, client, pickupLat, pickupLon, "scenario-inrange-pickup")
	if !ok {
		return fail(name, "ride request failed")
	}

	// Attempt-1's offer window is 30s (OfferTTL) — poll well inside it.
	offer := pollOffer(ctx, deps, accIn, driverIn, time.Now().Add(20*time.Second), rnd)
	if offer == nil || offer.RideId != rideID {
		return fail(name, "in-range driver was never offered ride %s", rideID)
	}

	if ok, detail := pollNoOfferThrough(ctx, deps, accOut, driverOut, time.Now().Add(5*time.Second), rnd); !ok {
		return fail(name, "out-of-range driver: %s", detail)
	}

	return pass(name, "in-range driver offered ride %s within attempt-1 window; out-of-range driver never offered", rideID)
}

// --- Assertion 2: offline but pinging ------------------------------------

func runOfflineAssertion(ctx context.Context, deps Deps, rnd *rand.Rand) scenarioResult {
	const name = "offline but pinging"
	const pickupLat, pickupLon = 51.00, 30.00 // see runInRangeAssertion's comment on point spacing

	acc := scenarioSignupDriver(ctx, deps, rnd, "scenario-offline")
	if acc == nil {
		return fail(name, "driver signup failed")
	}
	driverID, ok := scenarioCreateDriverProfile(ctx, deps, acc, rnd)
	if !ok {
		return fail(name, "driver profile creation failed")
	}
	shiftID, ok := scenarioGoOnline(ctx, deps, acc, driverID)
	if !ok {
		return fail(name, "shift open/online failed")
	}

	// ~2km south: well inside every attempt's radius.
	lat, lon := pointAtDistance(pickupLat, pickupLon, 180, 2)

	// Ping once while Online — populates location-service's owner mapping via shift.updated,
	// which depends only on that event having fired, not on current shift status.
	if !scenarioPingUntilAccepted(ctx, deps, acc, lat, lon, rnd) {
		return fail(name, "initial ping never accepted")
	}

	// Go offline (removed from drivers:online) but keep pinging, isolating the exclusion
	// to the availability intersection, not geography or a stale ping.
	if !scenarioGoOffline(ctx, deps, acc, shiftID, driverID) {
		return fail(name, "ending shift failed")
	}
	// Let matching-service's independent shift.updated consumer catch up before the ride exists.
	if !sleepFor(ctx, 5*time.Second) {
		return fail(name, "context cancelled")
	}

	stopPinging := make(chan struct{})
	defer close(stopPinging)
	go keepPinging(ctx, deps, acc, lat, lon, 15*time.Second, stopPinging)

	client := scenarioSignupClient(ctx, deps, rnd, "scenario-offline-client")
	if client == nil {
		return fail(name, "client signup failed")
	}
	if _, ok := scenarioRequestRide(ctx, deps, client, pickupLat, pickupLon, "scenario-offline-pickup"); !ok {
		return fail(name, "ride request failed")
	}

	// Span past the attempt-1/2 boundary (~30s) to rule out a timing fluke.
	if ok, detail := pollNoOfferThrough(ctx, deps, acc, driverID, time.Now().Add(40*time.Second), rnd); !ok {
		return fail(name, "%s", detail)
	}
	return pass(name, "offline-but-pinging driver was never offered despite being geographically in range")
}

// --- Assertion 3: radius expansion ---------------------------------------

func runExpansionAssertion(ctx context.Context, deps Deps, rnd *rand.Rand) scenarioResult {
	const name = "radius expansion"
	const pickupLat, pickupLon = 52.00, 30.00 // see runInRangeAssertion's comment on point spacing

	acc := scenarioSignupDriver(ctx, deps, rnd, "scenario-expansion")
	if acc == nil {
		return fail(name, "driver signup failed")
	}
	driverID, ok := scenarioCreateDriverProfile(ctx, deps, acc, rnd)
	if !ok {
		return fail(name, "driver profile creation failed")
	}
	shiftID, ok := scenarioGoOnline(ctx, deps, acc, driverID)
	if !ok {
		return fail(name, "shift open/online failed")
	}
	defer scenarioGoOffline(ctx, deps, acc, shiftID, driverID)

	// ~8km away: outside attempt-1 (5km) and attempt-2 (7km), inside
	// attempt-3 (9km) — see broadcast_offers.go's radiusForAttempt.
	lat, lon := pointAtDistance(pickupLat, pickupLon, 45, 8)
	if !scenarioPingUntilAccepted(ctx, deps, acc, lat, lon, rnd) {
		return fail(name, "initial ping never accepted")
	}

	// Stay under location-service's staleness window during the wait.
	stopPinging := make(chan struct{})
	defer close(stopPinging)
	go keepPinging(ctx, deps, acc, lat, lon, 20*time.Second, stopPinging)

	client := scenarioSignupClient(ctx, deps, rnd, "scenario-expansion-client")
	if client == nil {
		return fail(name, "client signup failed")
	}
	rideID, ok := scenarioRequestRide(ctx, deps, client, pickupLat, pickupLon, "scenario-expansion-pickup")
	if !ok {
		return fail(name, "ride request failed")
	}

	// Not offered during attempts 1-2 (5km/7km, roughly the first 55s).
	if ok, detail := pollNoOfferThrough(ctx, deps, acc, driverID, time.Now().Add(50*time.Second), rnd); !ok {
		return fail(name, "offered too early: %s", detail)
	}

	// Offered once the radius expands to attempt 3 (9km).
	offer := pollOffer(ctx, deps, acc, driverID, time.Now().Add(40*time.Second), rnd)
	if offer == nil || offer.RideId != rideID {
		return fail(name, "driver never offered even after the radius should have expanded to include it")
	}
	return pass(name, "driver excluded at 5/7km, offered ride %s once radius reached 9km", rideID)
}

// --- Assertion 4: staleness eviction --------------------------------------

func runStalenessAssertion(ctx context.Context, deps Deps, rnd *rand.Rand) scenarioResult {
	const name = "staleness eviction"
	const pickupLat, pickupLon = 53.00, 30.00 // see runInRangeAssertion's comment on point spacing

	acc := scenarioSignupDriver(ctx, deps, rnd, "scenario-staleness")
	if acc == nil {
		return fail(name, "driver signup failed")
	}
	driverID, ok := scenarioCreateDriverProfile(ctx, deps, acc, rnd)
	if !ok {
		return fail(name, "driver profile creation failed")
	}
	shiftID, ok := scenarioGoOnline(ctx, deps, acc, driverID)
	if !ok {
		return fail(name, "shift open/online failed")
	}
	defer scenarioGoOffline(ctx, deps, acc, shiftID, driverID)

	// ~2km west: well inside every attempt's radius.
	lat, lon := pointAtDistance(pickupLat, pickupLon, 270, 2)
	if !scenarioPingUntilAccepted(ctx, deps, acc, lat, lon, rnd) {
		return fail(name, "initial ping never accepted")
	}

	// Then go silent (shift stays Online) — isolates eviction from the offline case above.
	// Wait past LOCATION_STALENESS_SECONDS=120 + one sweep tick (fixed sleep, not jittered: undershoot = false failure).
	if !sleepFor(ctx, 155*time.Second) {
		return fail(name, "context cancelled while waiting out the staleness window")
	}

	client := scenarioSignupClient(ctx, deps, rnd, "scenario-staleness-client")
	if client == nil {
		return fail(name, "client signup failed")
	}
	if _, ok := scenarioRequestRide(ctx, deps, client, pickupLat, pickupLon, "scenario-staleness-pickup"); !ok {
		return fail(name, "ride request failed")
	}

	if ok, detail := pollNoOfferThrough(ctx, deps, acc, driverID, time.Now().Add(15*time.Second), rnd); !ok {
		return fail(name, "%s", detail)
	}
	return pass(name, "driver evicted by the staleness sweep was never offered despite historically being in range")
}

// --- Shared low-level helpers ---------------------------------------------

// scenarioSignupDriver/scenarioSignupClient wrap Deps's signupAndLogin with a scenario-local email
// domain, so these accounts are trivially distinguishable from the continuous simulation's own.
func scenarioSignupDriver(ctx context.Context, deps Deps, rnd *rand.Rand, id string) *account {
	phone := fmt.Sprintf("+35750%07d", rnd.Intn(10000000))
	return deps.signupAndLogin(ctx, id, id+"@e2e-scenario.local", "E2E "+id, phone, contracts.RoleDriver, rnd)
}

func scenarioSignupClient(ctx context.Context, deps Deps, rnd *rand.Rand, id string) *account {
	phone := fmt.Sprintf("+35799%07d", rnd.Intn(10000000))
	return deps.signupAndLogin(ctx, id, id+"@e2e-scenario.local", "E2E "+id, phone, contracts.RoleClient, rnd)
}

func scenarioCreateDriverProfile(ctx context.Context, deps Deps, acc *account, rnd *rand.Rand) (driverID string, ok bool) {
	created, err := deps.Driver.CreateDriver(ctx, acc.accessToken, contracts.CreateDriverDto{
		UserId:       acc.userID,
		VehicleType:  "Standard",
		LicencePlate: fmt.Sprintf("SC%04d", rnd.Intn(10000)),
	})
	if err != nil {
		return "", false
	}
	return created.Id, true
}

func scenarioGoOnline(ctx context.Context, deps Deps, acc *account, driverID string) (shiftID string, ok bool) {
	resp, err := deps.Driver.CreateShift(ctx, acc.accessToken, contracts.CreateShiftRequest{DriverId: driverID})
	if err != nil {
		return "", false
	}
	if err := deps.Driver.UpdateShift(ctx, acc.accessToken, resp.Id, contracts.UpdateShiftRequest{
		DriverId: driverID, Status: "Online",
	}); err != nil {
		return "", false
	}
	return resp.Id, true
}

// scenarioGoOffline is also used as deferred cleanup, so its error is swallowed there —
// a failed cleanup shouldn't mask the assertion's real pass/fail result.
func scenarioGoOffline(ctx context.Context, deps Deps, acc *account, shiftID, driverID string) bool {
	err := deps.Driver.UpdateShift(ctx, acc.accessToken, shiftID, contracts.UpdateShiftRequest{
		DriverId: driverID, Status: "Ended",
	})
	return err == nil
}

// scenarioPingUntilAccepted retries a single ping at (lat, lon) for up to 20s. A 403 here means
// location-service's owner mapping hasn't propagated yet — an expected transient, not a failure.
func scenarioPingUntilAccepted(ctx context.Context, deps Deps, acc *account, lat, lon float64, rnd *rand.Rand) bool {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if scenarioPing(ctx, deps, acc, lat, lon) {
			return true
		}
		if !sleepJitter(ctx, time.Second, rnd) {
			return false
		}
	}
	return false
}

func scenarioPing(ctx context.Context, deps Deps, acc *account, lat, lon float64) bool {
	_, err := deps.Location.IngestBatch(ctx, acc.accessToken, contracts.LocationBatchRequest{
		Pings: []contracts.LocationPingDto{{
			Lat: lat, Lon: lon, AccuracyM: 10, HeadingDeg: 0, SpeedMps: 0,
			DeviceTs: time.Now().UTC().Format(time.RFC3339Nano),
		}},
	})
	return err == nil
}

// keepPinging re-pings a fixed point every interval until stop is closed or ctx is done — keeps
// long-running assertions under location-service's staleness window without implying movement.
func keepPinging(ctx context.Context, deps Deps, acc *account, lat, lon float64, interval time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			scenarioPing(ctx, deps, acc, lat, lon)
		}
	}
}

func scenarioRequestRide(ctx context.Context, deps Deps, acc *account, lat, lon float64, label string) (rideID string, ok bool) {
	resp, err := deps.Ride.RequestRide(ctx, acc.accessToken, contracts.CreateRideRequest{
		PickupLat:     lat,
		PickupLng:     lon,
		PickupAddress: label,
		DestLat:       lat + 0.01,
		DestLng:       lon + 0.01,
		DestAddress:   label + "-dest",
		TariffName:    "Standard",
	})
	if err != nil {
		return "", false
	}
	return resp.RideID, true
}

// pollOffer polls GET /drivers/{driverId}/offer until deadline. 404 means "no offer yet"; any
// other error stops the poll immediately.
func pollOffer(ctx context.Context, deps Deps, acc *account, driverID string, deadline time.Time, rnd *rand.Rand) *contracts.DriverOfferDto {
	for time.Now().Before(deadline) {
		offer, err := deps.Matching.GetDriverOffer(ctx, acc.accessToken, driverID)
		if err == nil {
			return offer
		}
		var apiErr *apiclient.APIError
		if !errors.As(err, &apiErr) || apiErr.Status != http.StatusNotFound {
			return nil
		}
		if !sleepJitter(ctx, 2*time.Second, rnd) {
			return nil
		}
	}
	return nil
}

// pollNoOfferThrough is pollOffer's negative counterpart: a real offer or a hard error
// fails it immediately rather than waiting out the rest of the window.
func pollNoOfferThrough(ctx context.Context, deps Deps, acc *account, driverID string, deadline time.Time, rnd *rand.Rand) (ok bool, detail string) {
	for time.Now().Before(deadline) {
		offer, err := deps.Matching.GetDriverOffer(ctx, acc.accessToken, driverID)
		if err == nil {
			return false, fmt.Sprintf("unexpected offer for ride %s", offer.RideId)
		}
		var apiErr *apiclient.APIError
		if !errors.As(err, &apiErr) || apiErr.Status != http.StatusNotFound {
			return false, fmt.Sprintf("unexpected error polling offer: %v", err)
		}
		if !sleepJitter(ctx, 2*time.Second, rnd) {
			return false, "context cancelled"
		}
	}
	return true, ""
}

// sleepFor sleeps for exactly d, or returns false if ctx is cancelled first — unlike sleepJitter's
// pacing variance, scenario waits need a deterministic lower bound (undershooting = false failure).
func sleepFor(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// pointAtDistance returns the point distanceKm km from (lat, lon) along bearingDeg (0=north,
// 90=east) — the same flat-earth approximation movement.go's advanceTo uses.
func pointAtDistance(lat, lon, bearingDeg, distanceKm float64) (float64, float64) {
	distanceM := distanceKm * 1000
	bearingRad := bearingDeg * math.Pi / 180
	metersPerDegreeLon := metersPerDegreeLat * math.Cos(lat*math.Pi/180)
	newLat := lat + (distanceM*math.Cos(bearingRad))/metersPerDegreeLat
	newLon := lon + (distanceM*math.Sin(bearingRad))/metersPerDegreeLon
	return newLat, newLon
}
