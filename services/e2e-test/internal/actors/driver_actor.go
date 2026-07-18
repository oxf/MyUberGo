package actors

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	contracts "github.com/oxf/MyUber/contracts/http"
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
	phone     string
}

func (a *DriverActor) Run(ctx context.Context) {
	a.phone = fmt.Sprintf("+38050%07d", a.Rnd.Intn(10000000))

	acc := a.signupAndLogin(ctx, a.ID, a.Email, "E2E "+a.ID, a.phone, contracts.RoleDriver, a.Rnd)
	if acc == nil {
		return
	}
	if !a.createAndVerifyProfile(ctx, acc) {
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

		// Simulated work period before ending the shift.
		if !sleepJitter(ctx, a.Interval/2, a.Rnd) {
			return
		}

		a.setShiftStatus(ctx, shiftID, "Ended", "driver.shift.end")
		a.verifyEndedShift(ctx, shiftID)

		if cycle%4 == 0 {
			a.updateAndVerifyPhone(ctx)
		}
		if cycle%10 == 0 {
			a.refresh(ctx, a.ID, acc)
		}
	}
}

// createAndVerifyProfile retries until the profile exists or ctx is done.
func (a *DriverActor) createAndVerifyProfile(ctx context.Context, acc *account) bool {
	for {
		start := time.Now()
		created, err := a.Driver.CreateProfile(ctx, contracts.CreateDriverProfileDto{
			UserId:       acc.userID,
			DriverName:   "E2E " + a.ID,
			Phone:        a.phone,
			VehicleType:  "Standard",
			LicencePlate: fmt.Sprintf("E2E%04d", a.Rnd.Intn(10000)),
		})
		v := &Verify{}
		if err == nil {
			v.NotEmpty("id", created.Id)
		}
		a.record(a.ID, "driver.profile.create", start, err, v)

		if err == nil && v.OK() {
			a.profileID = created.Id
			a.verifyProfile(ctx, acc, "E2E "+a.ID, a.phone)
			return true
		}
		if !sleepJitter(ctx, 3*time.Second, a.Rnd) {
			return false
		}
	}
}

func (a *DriverActor) verifyProfile(ctx context.Context, acc *account, wantName, wantPhone string) {
	start := time.Now()
	profile, err := a.Driver.GetProfile(ctx, a.profileID)
	v := &Verify{}
	if err == nil {
		v.Eq("id", profile.Id, a.profileID)
		v.Eq("userId", profile.UserId, acc.userID)
		v.Eq("driverName", profile.DriverName, wantName)
		v.Eq("phone", profile.Phone, wantPhone)
		v.Eq("vehicleType", profile.VehicleType, "Standard")
		v.True("rating", profile.Rating >= 0, "expected >= 0")
	}
	a.record(a.ID, "driver.profile.get", start, err, v)
}

func (a *DriverActor) openShift(ctx context.Context) string {
	start := time.Now()
	resp, err := a.Driver.CreateShift(ctx, contracts.CreateShiftRequest{DriverId: a.profileID})
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
	shift, err := a.Driver.GetShift(ctx, shiftID)
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
	err := a.Driver.UpdateShift(ctx, shiftID, contracts.UpdateShiftRequest{
		DriverId: a.profileID,
		Status:   status,
	})
	a.record(a.ID, op, start, err, nil)
}

func (a *DriverActor) verifyEndedShift(ctx context.Context, shiftID string) {
	start := time.Now()
	shift, err := a.Driver.GetShift(ctx, shiftID)
	v := &Verify{}
	if err == nil {
		v.Eq("id", shift.Id, shiftID)
		v.True("endedAt", shift.EndedAt != nil && *shift.EndedAt != "", "expected endedAt to be set")
	}
	a.record(a.ID, "driver.shift.get", start, err, v)
}

func (a *DriverActor) verifyShiftInList(ctx context.Context, shiftID string) {
	start := time.Now()
	resp, err := a.Driver.ListShifts(ctx, 1, 50)
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

func (a *DriverActor) updateAndVerifyPhone(ctx context.Context) {
	a.phone = fmt.Sprintf("+38050%07d", a.Rnd.Intn(10000000))

	start := time.Now()
	err := a.Driver.UpdateProfile(ctx, a.profileID, contracts.UpdateDriverProfileDto{
		Phone: a.phone, // other fields empty: service keeps existing values via COALESCE(NULLIF(...))
	})
	a.record(a.ID, "driver.profile.update", start, err, nil)
	if err != nil {
		return
	}

	start = time.Now()
	profile, getErr := a.Driver.GetProfile(ctx, a.profileID)
	v := &Verify{}
	if getErr == nil {
		v.Eq("phone", profile.Phone, a.phone)
		v.Eq("driverName", profile.DriverName, "E2E "+a.ID)
	}
	a.record(a.ID, "driver.profile.get", start, getErr, v)
}
