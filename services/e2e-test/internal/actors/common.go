package actors

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"time"

	contracts "github.com/oxf/MyUber/contracts/http"

	"e2e-test/internal/apiclient"
	"e2e-test/internal/stats"
)

const password = "e2e-password-123"

// Seeded admin account — the only way to reach GET /users/driver/driver-shift, which
// are Admin-only at the Kong gateway.
const adminEmail = "admin@myubergo.local"
const adminPassword = "admin123"

// Deps groups the API clients and the stats collector shared by all actors.
type Deps struct {
	Auth     *apiclient.AuthClient
	Driver   *apiclient.DriverClient
	Ride     *apiclient.RideClient
	Matching *apiclient.MatchingClient
	Billing  *apiclient.BillingClient
	Location *apiclient.LocationClient
	Stats    *stats.Collector

	// AdminAccessToken is fetched once at startup and reused by every actor; never mutated,
	// so sharing it by value across goroutines is safe.
	AdminAccessToken string
}

// account is the authenticated identity an actor operates as.
type account struct {
	userID       string
	accessToken  string
	refreshToken string
	// clientID is auth.client(id) from GET /me; only set for Client-role accounts
	// (Driver accounts have no auth.client row).
	clientID string
}

// LoginAsAdmin blocks until it obtains an admin access token or ctx is cancelled (empty return);
// called once at startup before actor goroutines start, so no concurrent access to worry about.
func LoginAsAdmin(ctx context.Context, auth *apiclient.AuthClient) string {
	for {
		resp, err := auth.Login(ctx, contracts.LoginRequest{Email: adminEmail, Password: adminPassword})
		if err == nil && resp.AccessToken != "" {
			return resp.AccessToken
		}
		select {
		case <-ctx.Done():
			return ""
		case <-time.After(3 * time.Second):
		}
	}
}

func (d Deps) record(actor, op string, start time.Time, err error, v *Verify) {
	e := stats.Event{
		Actor:    actor,
		Op:       op,
		OK:       err == nil,
		VerifyOK: true,
		Latency:  time.Since(start),
	}
	if err != nil {
		e.Detail = err.Error()
	} else if v != nil && !v.OK() {
		e.VerifyOK = false
		e.Detail = v.Detail()
	}
	d.Stats.Record(e)
}

// signupAndLogin retries until it has a working account or ctx is cancelled. Signup and Login
// are retried as independent phases — a combined retry would re-send a now-taken Signup on a Login blip.
func (d Deps) signupAndLogin(ctx context.Context, actor, email, name, phone string, role contracts.UserRole, rnd *rand.Rand) *account {
	if !d.ensureSignedUp(ctx, actor, email, name, phone, role, rnd) {
		return nil
	}
	acc := d.loginUntilReady(ctx, actor, email, rnd)
	if acc == nil {
		return nil
	}

	// userID/clientID come from GET /me, not the Signup response, since the "already
	// registered" fallback path has none; nil ClientId (Driver accounts) is expected, not an error.
	me, ok := d.getMeUntilReady(ctx, actor, acc.accessToken, rnd)
	if !ok {
		return nil
	}
	acc.userID = me.ID
	if me.ClientId != nil {
		acc.clientID = *me.ClientId
	}

	return acc
}

// ensureSignedUp retries Signup alone until it succeeds or ctx is done. A 409 is treated as
// success, not failure — the only way e2e-test sees one is an earlier retry having gone through.
func (d Deps) ensureSignedUp(ctx context.Context, actor, email, name, phone string, role contracts.UserRole, rnd *rand.Rand) bool {
	for {
		start := time.Now()
		signup, err := d.Auth.Signup(ctx, contracts.SignupRequest{
			Email: email, Password: password, Name: name, Phone: phone, Role: role,
		})

		var apiErr *apiclient.APIError
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict {
			d.record(actor, "auth.signup", start, nil, nil)
			return true
		}

		v := &Verify{}
		if err == nil {
			v.NotEmpty("userId", signup.UserID)
		}
		d.record(actor, "auth.signup", start, err, v)
		if err == nil && v.OK() {
			return true
		}
		if !sleepJitter(ctx, 3*time.Second, rnd) {
			return false
		}
	}
}

// loginUntilReady retries Login alone until it succeeds or ctx is done.
func (d Deps) loginUntilReady(ctx context.Context, actor, email string, rnd *rand.Rand) *account {
	for {
		start := time.Now()
		login, err := d.Auth.Login(ctx, contracts.LoginRequest{Email: email, Password: password})
		v := &Verify{}
		if err == nil {
			v.NotEmpty("accessToken", login.AccessToken)
			v.NotEmpty("refreshToken", login.RefreshToken)
		}
		d.record(actor, "auth.login", start, err, v)
		if err == nil && v.OK() {
			return &account{accessToken: login.AccessToken, refreshToken: login.RefreshToken}
		}
		if !sleepJitter(ctx, 3*time.Second, rnd) {
			return nil
		}
	}
}

// getMeUntilReady retries GET /me alone until it succeeds or ctx is done.
func (d Deps) getMeUntilReady(ctx context.Context, actor string, accessToken string, rnd *rand.Rand) (contracts.UserDto, bool) {
	for {
		start := time.Now()
		me, err := d.Auth.Me(ctx, accessToken)
		v := &Verify{}
		if err == nil {
			v.NotEmpty("id", me.ID)
		}
		d.record(actor, "auth.me", start, err, v)
		if err == nil && v.OK() {
			return me, true
		}
		if !sleepJitter(ctx, 3*time.Second, rnd) {
			return contracts.UserDto{}, false
		}
	}
}

// refresh exercises token refresh; the same refresh token stays valid (no
// rotation server-side), only the access token is replaced.
func (d Deps) refresh(ctx context.Context, actor string, acc *account) {
	start := time.Now()
	resp, err := d.Auth.Refresh(ctx, contracts.RefreshRequest{RefreshToken: acc.refreshToken})
	v := &Verify{}
	if err == nil {
		v.NotEmpty("accessToken", resp.AccessToken)
	}
	d.record(actor, "auth.refresh", start, err, v)
	if err == nil && resp.AccessToken != "" {
		acc.accessToken = resp.AccessToken
	}
}

// sleepJitter sleeps for base +/- 50% and reports false once ctx is done.
func sleepJitter(ctx context.Context, base time.Duration, rnd *rand.Rand) bool {
	d := base/2 + time.Duration(rnd.Int63n(int64(base)))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
