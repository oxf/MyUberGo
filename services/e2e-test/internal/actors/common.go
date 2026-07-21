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

// Deps groups the API clients and the stats collector shared by all actors.
type Deps struct {
	Auth     *apiclient.AuthClient
	Driver   *apiclient.DriverClient
	Ride     *apiclient.RideClient
	Matching *apiclient.MatchingClient
	Stats    *stats.Collector
}

// account is the authenticated identity an actor operates as.
type account struct {
	userID       string
	accessToken  string
	refreshToken string
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

	return &account{
		userID:       signup.UserID,
		accessToken:  login.AccessToken,
		refreshToken: login.RefreshToken,
	}, nil
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
