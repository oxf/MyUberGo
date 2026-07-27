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

	clients int
	drivers int

	rideInterval   time.Duration
	shiftInterval  time.Duration
	reportInterval time.Duration
	runFor         time.Duration

	seed int64
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
		Stats:    stats.NewCollector(256),
	}

	go deps.Stats.Run(cfg.reportInterval)

	// GET /users, GET /driver, and GET /driver-shift are Admin-only at the
	// Kong gateway now (see gateway/kong.yml) — every actor's list-endpoint
	// verifies need this token, so it's fetched once, up front, rather than
	// per actor.
	log.Println("logging in as seeded admin (services/shared/migrations/init.sql)...")
	deps.AdminAccessToken = actors.LoginAsAdmin(ctx, deps.Auth)
	if deps.AdminAccessToken == "" {
		log.Println("warning: could not log in as admin before shutdown; list-endpoint verifies will fail")
	}

	// runID makes emails unique across runs — auth.user.email is unique and
	// the DB persists between simulator runs.
	runID := time.Now().Unix()

	log.Printf("starting simulator: %d clients, %d drivers (run %d, seed %d) — Ctrl-C to stop",
		cfg.clients, cfg.drivers, runID, cfg.seed)

	var wg sync.WaitGroup

	for i := 0; i < cfg.clients; i++ {
		actor := &actors.ClientActor{
			Deps:     deps,
			ID:       fmt.Sprintf("client-%d", i),
			Email:    fmt.Sprintf("client-%d-%d@e2e.local", runID, i),
			Interval: cfg.rideInterval,
			Rnd:      rand.New(rand.NewSource(cfg.seed + int64(i))),
		}
		wg.Go(func() { actor.Run(ctx) })
	}

	// One dedicated declining client (BILLING_SPEC.md §9's e2e-test step):
	// every invoice it ever generates is charged against pm_stub_decline, so
	// its rides are the ones that eventually reach uncollectible.
	decliningClient := &actors.ClientActor{
		Deps:               deps,
		ID:                 "decline-client-0",
		Email:              fmt.Sprintf("decline-client-%d@e2e.local", runID),
		Interval:           cfg.rideInterval,
		Rnd:                rand.New(rand.NewSource(cfg.seed + 9000)),
		PaymentMethodToken: "pm_stub_decline",
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

	// auth/ride/driver now go through Kong (the API gateway) — see
	// gateway/kong.yml and CLAUDE.md's "API Gateway" section. Each base URL
	// includes the /api/<service> prefix Kong routes on and strips before
	// forwarding, so apiclient paths below (e.g. "/login", "/ride") don't
	// change. matching-service has no gateway route (internal/Kafka-driven
	// today), so it's still reached directly.
	flag.StringVar(&cfg.authURL, "auth-url", getenv("E2E_AUTH_URL", "http://localhost:8090/api/auth"), "auth-service base URL (via gateway)")
	flag.StringVar(&cfg.rideURL, "ride-url", getenv("E2E_RIDE_URL", "http://localhost:8090/api/ride"), "ride-service base URL (via gateway)")
	flag.StringVar(&cfg.driverURL, "driver-url", getenv("E2E_DRIVER_URL", "http://localhost:8090/api/driver"), "driver-service base URL (via gateway)")
	flag.StringVar(&cfg.matchingURL, "matching-url", getenv("E2E_MATCHING_URL", "http://localhost:8002"), "matching-service base URL (direct, no gateway route)")
	flag.StringVar(&cfg.billingURL, "billing-url", getenv("E2E_BILLING_URL", "http://localhost:8090/api/billing"), "billing-service base URL (via gateway)")
	flag.IntVar(&cfg.clients, "clients", getenvInt("E2E_CLIENTS", 5), "number of virtual clients")
	flag.IntVar(&cfg.drivers, "drivers", getenvInt("E2E_DRIVERS", 3), "number of virtual drivers")
	flag.DurationVar(&cfg.rideInterval, "ride-interval", getenvDuration("E2E_RIDE_INTERVAL", 5*time.Second), "base interval between rides per client (jittered +/-50%)")
	flag.DurationVar(&cfg.shiftInterval, "shift-interval", getenvDuration("E2E_SHIFT_INTERVAL", 15*time.Second), "base interval between shift cycles per driver (jittered +/-50%)")
	flag.DurationVar(&cfg.reportInterval, "report-interval", getenvDuration("E2E_REPORT_INTERVAL", 10*time.Second), "stats report interval")
	flag.DurationVar(&cfg.runFor, "run-for", getenvDuration("E2E_RUN_FOR", 0), "stop after this duration (0 = run until Ctrl-C)")
	flag.Int64Var(&cfg.seed, "seed", time.Now().UnixNano(), "base random seed (per-actor seeds derive from it)")
	flag.Parse()

	return cfg
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
