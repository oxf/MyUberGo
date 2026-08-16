package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	app "location-service/internal/application"
	"location-service/internal/application/command"
	"location-service/internal/application/query"
	"location-service/internal/consumers"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/cache"
	"location-service/internal/infrastructure/health"
	"location-service/internal/infrastructure/metrics"
	"location-service/internal/infrastructure/shutdown"
	"location-service/internal/interfaces/http/handler"
	"location-service/internal/workers"

	"github.com/oxf/MyUber/common/envconfig"
	httpmw "github.com/oxf/MyUber/common/httpmiddleware"
	"github.com/oxf/MyUber/observability/obshttp"
	"github.com/oxf/MyUber/observability/obslog"
	"github.com/oxf/MyUber/observability/otelinit"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

const serviceName = "location-service"

func main() {
	// `app healthcheck` backs Docker's HEALTHCHECK: distroless has no shell/curl,
	// so the binary probes its own /health/live and exits 0/1.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		health.HealthcheckSelf("http://localhost:" + envconfig.String("SERVICE_PORT", "8004") + "/health/live")
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
	port := envconfig.String("SERVICE_PORT", "8004")

	validationConfig := domain.ValidationConfig{
		MaxAccuracyM:  float64(envconfig.Int("LOCATION_MAX_ACCURACY_M", 100)),
		MaxSpeedKmh:   float64(envconfig.Int("LOCATION_MAX_SPEED_KMH", 200)),
		MaxFutureSkew: time.Duration(envconfig.Int("LOCATION_MAX_FUTURE_SKEW_SECONDS", 120)) * time.Second,
		MaxPastSkew:   time.Duration(envconfig.Int("LOCATION_MAX_PAST_SKEW_SECONDS", 600)) * time.Second,
	}
	stalenessThreshold := time.Duration(envconfig.Int("LOCATION_STALENESS_SECONDS", 120)) * time.Second
	sweepInterval := time.Duration(envconfig.Int("LOCATION_SWEEP_INTERVAL_SECONDS", 30)) * time.Second

	driverLocationRepo := cache.NewDriverLocationRepository(redisDb, stalenessThreshold)
	ownerRepo := cache.NewOwnerRepository(redisDb)

	metricsClient := metrics.NewOtelMetricsClient(serviceName)

	// geo-index-size-vs-drivers:online gauge (LOCATION_SPEC.md §12,
	// docs/AUDIT_2026-08-15.md #6) — same shape as matching-service's own
	// myubergo.drivers.online observable gauge.
	if err := metricsClient.Gauge(
		"myubergo.location.geo_index_size",
		nil,
		func(ctx context.Context) (int64, error) {
			return redisDb.ZCard(ctx, "loc:drivers:geo").Result()
		},
	); err != nil {
		log.Fatal(err)
	}

	application := app.Application{
		Commands: app.Commands{
			IngestPings: command.NewIngestPingsHandler(ownerRepo, driverLocationRepo, validationConfig, logger, metricsClient),
			UpsertOwner: command.NewUpsertOwnerHandler(ownerRepo, logger, metricsClient),
		},
		Queries: app.Queries{
			FindNearbyDrivers: query.NewFindNearbyDriversHandler(driverLocationRepo, logger, metricsClient),
		},
	}

	locationHandler := handler.NewLocationHandler(application, logger)

	// Initialize health checker (Redis-backed — location-service has no Postgres in Slice 1/2)
	healthChecker := health.NewChecker(redisDb, 5*time.Second)
	healthChecker.Start()
	defer healthChecker.Stop()

	mux := http.NewServeMux()

	// Health check endpoints
	mux.HandleFunc("GET /health/live", healthChecker.LiveHandler)
	mux.HandleFunc("GET /health/ready", healthChecker.ReadyHandler)

	// Client-facing (via Kong, /api/location -> strip_path -> here)
	mux.HandleFunc("POST /batch", locationHandler.IngestBatch)

	// Internal-only (no Kong route, network-isolated — see CLAUDE.md's API Gateway section)
	mux.HandleFunc("GET /internal/drivers/nearby", locationHandler.NearbyDrivers)

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

	// Cancellable context for background goroutines (Kafka consumer + staleness worker)
	bgCtx, cancelBg := context.WithCancel(context.Background())
	shutdownManager.OnStop(cancelBg)

	shiftUpdatedConsumer := consumers.NewShiftUpdatedConsumer(application, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, bgCtx, "shift-updated-consumer", func() {
		defer shutdownManager.Done()
		shiftUpdatedConsumer.Run(bgCtx, "shift.updated")
	})

	stalenessWorker := workers.NewStalenessWorker(driverLocationRepo, stalenessThreshold, sweepInterval, logger, metricsClient)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, bgCtx, "staleness-worker", func() {
		defer shutdownManager.Done()
		stalenessWorker.Run(bgCtx)
	})

	// Start server in a goroutine
	health.GoSafe(logger, healthChecker, nil, "http-server", func() {
		logger.Info("location-service listening on :" + port)
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
