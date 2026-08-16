package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"e2e-test/internal/actors"
	"e2e-test/internal/apiclient"
	"e2e-test/internal/stats"
)

type config struct {
	authURL     string
	rideURL     string
	driverURL   string
	matchingURL string
	billingURL  string
	locationURL string

	clients int
	drivers int

	rideInterval   time.Duration
	shiftInterval  time.Duration
	reportInterval time.Duration
	runFor         time.Duration

	seed int64

	// scenario selects a one-shot proof instead of the continuous simulation; empty
	// (default) keeps today's behavior — see actors.RunLocationRadiusScenario.
	scenario string
}

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// -run-for > 0 bounds the run (timed/CI runs); shutdown goes through the
	// same context cancellation as Ctrl-C.
	if cfg.runFor > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.runFor)
		defer cancel()
	}

	deps := actors.Deps{
		Auth:     apiclient.NewAuthClient(cfg.authURL),
		Driver:   apiclient.NewDriverClient(cfg.driverURL),
		Ride:     apiclient.NewRideClient(cfg.rideURL),
		Matching: apiclient.NewMatchingClient(cfg.matchingURL),
		Billing:  apiclient.NewBillingClient(cfg.billingURL),
		Location: apiclient.NewLocationClient(cfg.locationURL),
		Stats:    stats.NewCollector(256),
	}

	go deps.Stats.Run(cfg.reportInterval)

	// GET /users, /driver, /driver-shift are Admin-only at Kong — every actor's list-endpoint
	// verify needs this token, so it's fetched once up front rather than per actor.
	log.Println("logging in as seeded admin (services/shared/migrations/sql/0002_auth.up.sql)...")
	deps.AdminAccessToken = actors.LoginAsAdmin(ctx, deps.Auth)
	if deps.AdminAccessToken == "" {
		log.Println("warning: could not log in as admin before shutdown; list-endpoint verifies will fail")
	}

	if cfg.scenario != "" {
		runScenario(ctx, cfg, deps)
		return
	}

	// runID makes emails unique across runs — auth.user.email is unique and
	// the DB persists between simulator runs.
	runID := time.Now().Unix()

	// PAYMENT_PROVIDER mirrors billing-service's own env var. pm_stub_* tokens are
	// StubProvider-only fixtures — sending them to real Stripe 404s, hence the two helpers below.
	paymentProvider := getenv("PAYMENT_PROVIDER", "stub")
	log.Printf("payment provider: %s (default token %s, decline token %s)",
		paymentProvider, defaultPaymentMethodToken(paymentProvider), declinePaymentMethodToken(paymentProvider))

	log.Printf("starting simulator: %d clients, %d drivers (run %d, seed %d) — Ctrl-C to stop",
		cfg.clients, cfg.drivers, runID, cfg.seed)

	var wg sync.WaitGroup

	for i := 0; i < cfg.clients; i++ {
		actor := &actors.ClientActor{
			Deps:               deps,
			ID:                 fmt.Sprintf("client-%d", i),
			Email:              fmt.Sprintf("client-%d-%d@e2e.local", runID, i),
			Interval:           cfg.rideInterval,
			Rnd:                rand.New(rand.NewSource(cfg.seed + int64(i))),
			PaymentMethodToken: defaultPaymentMethodToken(paymentProvider),
		}
		wg.Go(func() { actor.Run(ctx) })
	}

	// One dedicated declining client (BILLING_SPEC.md §9): every invoice it generates
	// is charged against the decline token, so its rides eventually reach uncollectible.
	decliningClient := &actors.ClientActor{
		Deps:               deps,
		ID:                 "decline-client-0",
		Email:              fmt.Sprintf("decline-client-%d@e2e.local", runID),
		Interval:           cfg.rideInterval,
		Rnd:                rand.New(rand.NewSource(cfg.seed + 9000)),
		PaymentMethodToken: declinePaymentMethodToken(paymentProvider),
	}
	wg.Go(func() { decliningClient.Run(ctx) })

	for i := 0; i < cfg.drivers; i++ {
		actor := &actors.DriverActor{
			Deps:     deps,
			ID:       fmt.Sprintf("driver-%d", i),
			Email:    fmt.Sprintf("driver-%d-%d@e2e.local", runID, i),
			Interval: cfg.shiftInterval,
			Rnd:      rand.New(rand.NewSource(cfg.seed + 1000 + int64(i))),
		}
		wg.Go(func() { actor.Run(ctx) })
	}

	<-ctx.Done()
	log.Println("shutting down: waiting for actors to finish...")
	wg.Wait()
	deps.Stats.Close()
	log.Println("done")
}

func loadConfig() config {
	cfg := config{}

	// auth/ride/driver/matching go through Kong; each base URL includes the /api/<service>
	// prefix Kong strips before forwarding, so apiclient paths below don't change.
	flag.StringVar(&cfg.authURL, "auth-url", getenv("E2E_AUTH_URL", "http://localhost:8090/api/auth"), "auth-service base URL (via gateway)")
	flag.StringVar(&cfg.rideURL, "ride-url", getenv("E2E_RIDE_URL", "http://localhost:8090/api/ride"), "ride-service base URL (via gateway)")
	flag.StringVar(&cfg.driverURL, "driver-url", getenv("E2E_DRIVER_URL", "http://localhost:8090/api/driver"), "driver-service base URL (via gateway)")
	flag.StringVar(&cfg.matchingURL, "matching-url", getenv("E2E_MATCHING_URL", "http://localhost:8090/api/matching"), "matching-service base URL (via gateway)")
	flag.StringVar(&cfg.billingURL, "billing-url", getenv("E2E_BILLING_URL", "http://localhost:8090/api/billing"), "billing-service base URL (via gateway)")
	flag.StringVar(&cfg.locationURL, "location-url", getenv("E2E_LOCATION_URL", "http://localhost:8090/api/location"), "location-service base URL (via gateway)")
	flag.IntVar(&cfg.clients, "clients", getenvInt("E2E_CLIENTS", 5), "number of virtual clients")
	flag.IntVar(&cfg.drivers, "drivers", getenvInt("E2E_DRIVERS", 3), "number of virtual drivers")
	flag.DurationVar(&cfg.rideInterval, "ride-interval", getenvDuration("E2E_RIDE_INTERVAL", 5*time.Second), "base interval between rides per client (jittered +/-50%)")
	flag.DurationVar(&cfg.shiftInterval, "shift-interval", getenvDuration("E2E_SHIFT_INTERVAL", 15*time.Second), "base interval between shift cycles per driver (jittered +/-50%)")
	flag.DurationVar(&cfg.reportInterval, "report-interval", getenvDuration("E2E_REPORT_INTERVAL", 10*time.Second), "stats report interval")
	flag.DurationVar(&cfg.runFor, "run-for", getenvDuration("E2E_RUN_FOR", 0), "stop after this duration (0 = run until Ctrl-C)")
	flag.Int64Var(&cfg.seed, "seed", time.Now().UnixNano(), "base random seed (per-actor seeds derive from it)")
	flag.StringVar(&cfg.scenario, "scenario", getenv("E2E_SCENARIO", ""), "run a one-shot proof instead of the continuous simulation, then exit (supported: location-radius)")
	flag.Parse()

	return cfg
}

// runScenario dispatches a one-shot proof and exits non-zero on failure, unlike the open-ended
// continuous simulation. Its own 5min timeout (independent of -run-for) covers the ~150s staleness assertion.
func runScenario(ctx context.Context, cfg config, deps actors.Deps) {
	scenarioCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	var passed bool
	switch cfg.scenario {
	case "location-radius":
		passed = actors.RunLocationRadiusScenario(scenarioCtx, deps, cfg.seed)
	default:
		log.Fatalf("unknown -scenario %q (supported: location-radius)", cfg.scenario)
	}

	deps.Stats.Close()
	if !passed {
		os.Exit(1)
	}
}

// defaultPaymentMethodToken/declinePaymentMethodToken pick the fixture token for the active provider.
// Stripe forbids attaching decline cards directly, so the decline token is pm_card_chargeCustomerFail (attach succeeds, charge fails later).
func defaultPaymentMethodToken(provider string) string {
	if provider == "stripe" {
		return "pm_card_visa"
	}
	return "pm_stub_ok"
}

func declinePaymentMethodToken(provider string) string {
	if provider == "stripe" {
		return "pm_card_chargeCustomerFail"
	}
	return "pm_stub_decline"
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
