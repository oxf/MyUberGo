package actors

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	contracts "github.com/oxf/MyUber/contracts/http"

	"e2e-test/internal/apiclient"
	"e2e-test/internal/stats"
)

const password = "e2e-password-123"

// Seeded admin account (services/shared/migrations/init.sql) — the only way
// to reach GET /users, GET /driver, and GET /driver-shift now that those
// list endpoints are Admin-only at the Kong gateway (see gateway/kong.yml).
const adminEmail = "admin@myubergo.local"
const adminPassword = "admin123"

// Deps groups the API clients and the stats collector shared by all actors.
type Deps struct {
	Auth     *apiclient.AuthClient
	Driver   *apiclient.DriverClient
	Ride     *apiclient.RideClient
	Matching *apiclient.MatchingClient
	Stats    *stats.Collector

	// AdminAccessToken is fetched once at startup (see LoginAsAdmin) and
	// reused by every actor for the Admin-only list endpoints. Actors never
	// mutate it, so sharing it by value across goroutines is safe.
	AdminAccessToken string
}

// account is the authenticated identity an actor operates as.
type account struct {
	userID       string
	accessToken  string
	refreshToken string
	// clientID is auth.client(id), populated from GET /me after login. Only
	// set for Client-role accounts (empty for Driver accounts, which have no
	// auth.client row).
	clientID string
}

// LoginAsAdmin blocks until it obtains an access token for the seeded admin
// account or ctx is cancelled (empty return) — called once at startup,
// before any actor goroutines start, so there's no concurrent access to
// worry about.
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

// signupAndLogin retries until it has a working account or ctx is cancelled
// (nil return). Errors are recorded, never fatal — the stack may still be
// starting up when the simulator launches.
func (d Deps) signupAndLogin(ctx context.Context, actor, email, name, phone string, role contracts.UserRole, rnd *rand.Rand) *account {
	for {
		acc, err := d.trySignupAndLogin(ctx, actor, email, name, phone, role)
		if err == nil {
			return acc
		}
		if !sleepJitter(ctx, 3*time.Second, rnd) {
			return nil
		}
	}
}

func (d Deps) trySignupAndLogin(ctx context.Context, actor, email, name, phone string, role contracts.UserRole) (*account, error) {
	start := time.Now()
	signup, err := d.Auth.Signup(ctx, contracts.SignupRequest{
		Email: email, Password: password, Name: name, Phone: phone, Role: role,
	})
	v := &Verify{}
	if err == nil {
		v.NotEmpty("userId", signup.UserID)
	}
	d.record(actor, "auth.signup", start, err, v)
	if err != nil || !v.OK() {
		return nil, fmt.Errorf("signup: %s", firstNonEmpty(errString(err), v.Detail()))
	}

	start = time.Now()
	login, err := d.Auth.Login(ctx, contracts.LoginRequest{Email: email, Password: password})
	v = &Verify{}
	if err == nil {
		v.NotEmpty("accessToken", login.AccessToken)
		v.NotEmpty("refreshToken", login.RefreshToken)
	}
	d.record(actor, "auth.login", start, err, v)
	if err != nil || !v.OK() {
		return nil, fmt.Errorf("login: %s", firstNonEmpty(errString(err), v.Detail()))
	}

	acc := &account{
		userID:       signup.UserID,
		accessToken:  login.AccessToken,
		refreshToken: login.RefreshToken,
	}

	// Best-effort: a Client's clientId (auth.client(id), distinct from
	// userID — see the role-table refactor notes in CLAUDE.md/PLAN.md) is
	// only known via GET /me. A Driver has no client row, so me.ClientId is
	// nil and acc.clientID stays empty, which is expected, not an error.
	if me, err := d.Auth.Me(ctx, acc.accessToken); err == nil && me.ClientId != nil {
		acc.clientID = *me.ClientId
	}

	return acc, nil
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

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
