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
	"matching-service/internal/infrastructure/location"
	"matching-service/internal/infrastructure/metrics"
	"matching-service/internal/infrastructure/shutdown"
	"matching-service/internal/interfaces/http/handler"
	"matching-service/internal/workers"

	"github.com/oxf/MyUber/common/envconfig"
	"github.com/oxf/MyUber/common/httpclient"
	httpmw "github.com/oxf/MyUber/common/httpmiddleware"
	"github.com/oxf/MyUber/common/kafkapublisher"
	"github.com/oxf/MyUber/observability/obshttp"
	"github.com/oxf/MyUber/observability/obslog"
	"github.com/oxf/MyUber/observability/otelinit"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

const serviceName = "matching-service"

func main() {
	// `app healthcheck` backs Docker's HEALTHCHECK: distroless has no shell/curl,
	// so the binary probes its own /health/ready and exits 0/1.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		health.HealthcheckSelf("http://localhost:" + envconfig.String("SERVICE_PORT", "8002") + "/health/live")
	}

	// otelinit.Setup installs the global tracer/meter/logger providers from OTEL_* env vars.
	// Never fails boot on a down Collector — OTLP/gRPC exporters dial lazily and retry in the background.
	ctx := context.Background()
	providers, err := otelinit.Setup(ctx, serviceName)
	if err != nil {
		log.Fatal(err)
	}

	logger := obslog.NewLogger(serviceName)

	redisUrl := envconfig.String("REDIS_URL", "redis:6379")
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

	kafkaBroker := envconfig.String("KAFKA_BROKER", "kafka:29092")
	port := envconfig.String("SERVICE_PORT", "8002")

	driverRepo := cache.NewDriverRepository(redisDb)
	rideRepo := cache.NewRideRepository(redisDb)
	offerRepo := cache.NewOfferRepository(redisDb)

	// locationClient is location-service's geo-discovery adapter. Short timeout: BroadcastOffersHandler
	// must fall back to the rating-only pool rather than block on a slow/down dependency (LOCATION_SPEC.md §2.2).
	locationClient := location.NewHTTPClient(
		envconfig.String("LOCATION_URL", "http://location-service:8004"),
		httpclient.New(2*time.Second),
	)

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

	publisher := kafkapublisher.New(kafkaBroker, kafkapublisher.WithBatchTimeout(500*time.Millisecond))
	defer publisher.Close()

	application := app.Application{
		Commands: app.Commands{
			UpsertDriver:    command.NewUpsertDriverHandler(driverRepo, logger, metricsClient),
			CreateRide:      command.NewCreateRideHandler(rideRepo, logger, metricsClient),
			BroadcastOffers: command.NewBroadcastOffersHandler(rideRepo, driverRepo, offerRepo, locationClient, publisher, logger, metricsClient),
			AcceptRide:      command.NewAcceptRideHandler(rideRepo, driverRepo, offerRepo, publisher, logger, metricsClient),
			CancelRide:      command.NewCancelRideHandler(rideRepo, driverRepo, offerRepo, logger, metricsClient),
		},
		Queries: app.Queries{
			GetDriverOffer: query.NewGetDriverOfferHandler(rideRepo, offerRepo, driverRepo, logger, metricsClient),
		},
	}

	matchingHandler := handler.NewMatchingHandler(application, logger)

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
	health.GoSafe(logger, healthChecker, bgCtx, "ride-requested-consumer", func() {
		defer shutdownManager.Done()
		rideConsumer.Run(bgCtx, "ride.requested")
	})
	health.GoSafe(logger, healthChecker, bgCtx, "shift-updated-consumer", func() {
		defer shutdownManager.Done()
		driverConsumer.Run(bgCtx, "shift.updated")
	})
	health.GoSafe(logger, healthChecker, bgCtx, "ride-cancelled-consumer", func() {
		defer shutdownManager.Done()
		rideCancelledConsumer.Run(bgCtx, "ride.cancelled")
	})

	// Match retry worker: sweeps pending_ride:* and re-broadcasts offers for
	// rides whose deadline lapsed without an accept.
	retryWorker := workers.NewMatchRetryWorker(offerRepo, application.Commands.BroadcastOffers, logger, 5*time.Second)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, bgCtx, "match-retry-worker", func() {
		defer shutdownManager.Done()
		retryWorker.Run(bgCtx)
	})

	// Start server in a goroutine
	health.GoSafe(logger, healthChecker, nil, "http-server", func() {
		logger.Info("matching-service listening on :" + port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Error("server error")
		}
	})

	// Wait for shutdown signal and perform graceful shutdown
	shutdownManager.WaitForShutdown()
}

// commandFilter extends redisotel's DefaultCommandFilter to also exclude PING (from
// health.Checker's ticker), else every ping emits its own orphan trace in Tempo.
func commandFilter(cmd redis.Cmder) bool {
	if strings.EqualFold(cmd.Name(), "ping") {
		return true
	}
	return redisotel.DefaultCommandFilter(cmd)
}
