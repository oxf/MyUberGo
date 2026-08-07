package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	app "matching-service/internal/application"
	"matching-service/internal/application/command"
	"matching-service/internal/application/query"
	"matching-service/internal/consumers"
	"matching-service/internal/infrastructure/cache"
	"matching-service/internal/infrastructure/health"
	kafkainfra "matching-service/internal/infrastructure/kafka"
	"matching-service/internal/infrastructure/metrics"
	"matching-service/internal/infrastructure/shutdown"
	"matching-service/internal/interfaces/http/handler"
	"matching-service/internal/workers"

	httpmw "github.com/oxf/MyUber/common/httpmiddleware"
	"github.com/oxf/MyUber/observability/obshttp"
	"github.com/oxf/MyUber/observability/obslog"
	"github.com/oxf/MyUber/observability/otelinit"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

const serviceName = "matching-service"

func main() {
	// `app healthcheck` backs Docker's HEALTHCHECK: distroless has no shell/curl,
	// so the binary probes its own /health/ready and exits 0/1.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		healthcheckSelf("http://localhost:" + getenv("SERVICE_PORT", "8002") + "/health/live")
	}

	// otelinit.Setup installs the global tracer/meter/logger providers from OTEL_* env vars.
	// Never fails boot on a down Collector — OTLP/gRPC exporters dial lazily and retry in the background.
	ctx := context.Background()
	providers, err := otelinit.Setup(ctx, serviceName)
	if err != nil {
		log.Fatal(err)
	}

	logger := obslog.NewLogger(serviceName)

	redisUrl := getenv("REDIS_URL", "redis:6379")
	redisDb := redis.NewClient(&redis.Options{
		Addr:     redisUrl,
		Password: "",
		DB:       0,
	})
	if err := redisotel.InstrumentTracing(redisDb, redisotel.WithCommandFilter(commandFilter)); err != nil {
		log.Fatal(err)
	}
	if err := redisotel.InstrumentMetrics(redisDb); err != nil {
		log.Fatal(err)
	}

	kafkaBroker := getenv("KAFKA_BROKER", "kafka:29092")
	port := getenv("SERVICE_PORT", "8002")

	driverRepo := cache.NewDriverRepository(redisDb)
	rideRepo := cache.NewRideRepository(redisDb)
	offerRepo := cache.NewOfferRepository(redisDb)

	// Concrete *obsmetrics.Client (assignable to decorator.MetricsClient via structural
	// typing) so the drivers-online observable gauge below can register on it directly.
	metricsClient := metrics.NewOtelMetricsClient(serviceName)

	if err := metricsClient.Gauge(
		"myubergo.drivers.online",
		nil,
		func(ctx context.Context) (int64, error) {
			return redisDb.ZCard(ctx, "drivers:online").Result()
		},
	); err != nil {
		log.Fatal(err)
	}

	publisher := kafkainfra.NewPublisher(kafkaBroker)
	defer publisher.Close()

	application := app.Application{
		Commands: app.Commands{
			UpsertDriver:    command.NewUpsertDriverHandler(driverRepo, logger, metricsClient),
			CreateRide:      command.NewCreateRideHandler(rideRepo, logger, metricsClient),
			BroadcastOffers: command.NewBroadcastOffersHandler(rideRepo, driverRepo, offerRepo, logger, metricsClient),
			AcceptRide:      command.NewAcceptRideHandler(rideRepo, driverRepo, offerRepo, publisher, logger, metricsClient),
			CancelRide:      command.NewCancelRideHandler(rideRepo, driverRepo, offerRepo, logger, metricsClient),
		},
		Queries: app.Queries{
			GetDriverOffer: query.NewGetDriverOfferHandler(rideRepo, offerRepo, logger, metricsClient),
		},
	}

	matchingHandler := handler.NewMatchingHandler(application)

	// Initialize health checker (Redis-backed — matching-service has no Postgres)
	healthChecker := health.NewChecker(redisDb, 5*time.Second)
	healthChecker.Start()
	defer healthChecker.Stop()

	mux := http.NewServeMux()

	// Health check endpoints
	mux.HandleFunc("GET /health/live", healthChecker.LiveHandler)
	mux.HandleFunc("GET /health/ready", healthChecker.ReadyHandler)

	// API endpoints
	mux.HandleFunc("POST /rides/{rideId}/accept", matchingHandler.AcceptRide)
	mux.HandleFunc("GET /drivers/{driverId}/offer", matchingHandler.GetDriverOffer)

	// Create HTTP server
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      httpmw.BodyLimit(httpmw.RequestID(obshttp.Handler(httpmw.Recover(logger)(mux), serviceName))),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create shutdown manager with 30s timeout
	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// Flip readiness the moment shutdown begins, not up to checkInterval later —
	// the ticker-based Redis-ping check alone wouldn't catch this promptly.
	shutdownManager.OnStop(healthChecker.MarkNotReady)

	// Flush providers only after every worker below has actually drained (not merely told
	// to stop) — OnStop would be too early and silently drop the drain period's telemetry.
	shutdownManager.OnDrained(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := providers.Shutdown(shutdownCtx); err != nil {
			log.Printf("observability shutdown error: %v\n", err)
		}
	})

	// Cancellable context for background goroutines (Kafka consumers + retry worker)
	bgCtx, cancelBg := context.WithCancel(context.Background())
	shutdownManager.OnStop(cancelBg)

	rideConsumer := consumers.NewRideRequestedConsumer(application, kafkaBroker, logger)
	driverConsumer := consumers.NewShiftUpdatedConsumer(application, kafkaBroker, logger)
	rideCancelledConsumer := consumers.NewRideCancelledConsumer(application, kafkaBroker, logger)

	shutdownManager.Add(3)
	goSafe(logger, healthChecker, bgCtx, "ride-requested-consumer", func() {
		defer shutdownManager.Done()
		rideConsumer.Run(bgCtx, "ride.requested")
	})
	goSafe(logger, healthChecker, bgCtx, "shift-updated-consumer", func() {
		defer shutdownManager.Done()
		driverConsumer.Run(bgCtx, "shift.updated")
	})
	goSafe(logger, healthChecker, bgCtx, "ride-cancelled-consumer", func() {
		defer shutdownManager.Done()
		rideCancelledConsumer.Run(bgCtx, "ride.cancelled")
	})

	// Match retry worker: sweeps pending_ride:* and re-broadcasts offers for
	// rides whose deadline lapsed without an accept.
	retryWorker := workers.NewMatchRetryWorker(offerRepo, application.Commands.BroadcastOffers, logger, 5*time.Second)
	shutdownManager.Add(1)
	goSafe(logger, healthChecker, bgCtx, "match-retry-worker", func() {
		defer shutdownManager.Done()
		retryWorker.Run(bgCtx)
	})

	// Start server in a goroutine
	goSafe(logger, healthChecker, nil, "http-server", func() {
		logger.Info("matching-service listening on :" + port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Error("server error")
		}
	})

	// Wait for shutdown signal and perform graceful shutdown
	shutdownManager.WaitForShutdown()
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// commandFilter extends redisotel's DefaultCommandFilter to also exclude PING (from
// health.Checker's ticker), else every ping emits its own orphan trace in Tempo.
func commandFilter(cmd redis.Cmder) bool {
	if strings.EqualFold(cmd.Name(), "ping") {
		return true
	}
	return redisotel.DefaultCommandFilter(cmd)
}

// healthcheckSelf backs the `app healthcheck` subcommand: a plain HTTP GET
// against the service's own readiness endpoint, exiting 0/1 for Docker.
func healthcheckSelf(url string) {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	os.Exit(0)
}

// goSafe launches fn in a goroutine, recovering panics. If workerCtx is non-nil and fn
// returns while workerCtx.Err() is nil, a worker exited unexpectedly — mark not-live.
func goSafe(logger *logrus.Entry, healthChecker *health.Checker, workerCtx context.Context, name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.WithField("goroutine", name).WithField("panic", r).Error("recovered from panic")
			}
			if workerCtx != nil && workerCtx.Err() == nil {
				healthChecker.MarkNotLive(name + " exited unexpectedly")
			}
		}()
		fn()
	}()
}
