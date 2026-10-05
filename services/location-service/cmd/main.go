package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	app "location-service/internal/application"
	"location-service/internal/application/command"
	"location-service/internal/application/query"
	"location-service/internal/consumers"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/cache"
	"location-service/internal/infrastructure/health"
	"location-service/internal/infrastructure/history"
	"location-service/internal/infrastructure/metrics"
	"location-service/internal/infrastructure/pubsub"
	"location-service/internal/infrastructure/shutdown"
	"location-service/internal/interfaces/http/handler"
	"location-service/internal/interfaces/ws"
	"location-service/internal/persistence"
	"location-service/internal/workers"

	"github.com/oxf/MyUber/common/dbconn"
	"github.com/oxf/MyUber/common/envconfig"
	httpmw "github.com/oxf/MyUber/common/httpmiddleware"
	"github.com/oxf/MyUber/common/kafkapublisher"
	"github.com/oxf/MyUber/common/outbox"
	"github.com/oxf/MyUber/common/redisconn"
	"github.com/oxf/MyUber/observability/obshttp"
	"github.com/oxf/MyUber/observability/obslog"
	"github.com/oxf/MyUber/observability/otelinit"

	_ "github.com/lib/pq"
)

const serviceName = "location-service"
const defaultRedisURL = "redis://redis:6379"
const defaultPgDsn = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"

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

	redisDb, err := redisconn.Open(defaultRedisURL)
	if err != nil {
		log.Fatal(err)
	}

	// Slice 3: the summary tier (location.ride_summary + outbox_message).
	// Redis/Kafka failures above are still fatal at boot, same as Slices 1-2;
	// Postgres joins that list here for the first time.
	db, err := dbconn.Open(defaultPgDsn)
	if err != nil {
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
	wsPingInterval := time.Duration(envconfig.Int("LOCATION_WS_PING_SECONDS", 25)) * time.Second
	archiveInterval := time.Duration(envconfig.Int("LOCATION_ARCHIVE_INTERVAL_SECONDS", 30)) * time.Second
	archiveBatch := envconfig.Int("LOCATION_ARCHIVE_BATCH", 500)
	historyTTLDays := envconfig.Int("LOCATION_HISTORY_TTL_DAYS", 30)
	rdpEpsilonM := float64(envconfig.Int("LOCATION_RDP_EPSILON_M", 5))
	dynamoEndpoint := envconfig.String("DYNAMODB_ENDPOINT", "")

	// DynamoDB Local raw-history tier (LOCATION_SPEC.md §6.2) — an
	// audit/verification input, never a system of record: EnsureTable's
	// failure is logged, not fatal, so a slow/absent DynamoDB Local never
	// blocks boot the way Redis/Kafka/Postgres above do.
	dynamoClient, err := history.NewClient(ctx, dynamoEndpoint)
	if err != nil {
		log.Fatal(err)
	}
	historyRepo := history.NewRepository(dynamoClient, historyTTLDays, logger)
	if err := historyRepo.EnsureTable(ctx); err != nil {
		logger.WithError(err).Error("failed to ensure DynamoDB history table exists at boot; ArchiveWorker will keep retrying")
	}

	driverLocationRepo := cache.NewDriverLocationRepository(redisDb, stalenessThreshold)
	ownerRepo := cache.NewOwnerRepository(redisDb)
	trackingRepo := cache.NewTrackingRepository(redisDb)
	clientLocationRepo := cache.NewClientLocationRepository(redisDb)
	rideTrackRepo := cache.NewRideTrackRepository(redisDb)
	positionPublisher := pubsub.NewRedisPublisher(redisDb)

	// Dispatcher first, then Hub(dispatcher) — avoids circular construction,
	// see pubsub.Dispatcher's doc comment.
	dispatcher := pubsub.NewDispatcher(redisDb, logger)
	hub := ws.NewHub(dispatcher, logger)

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

	transactionManager := persistence.NewPostgresTransactionManager(db)
	outboxRepo := persistence.NewPostgresOutboxRepository(db)
	summaryRepo := persistence.NewPostgresRideSummaryRepository(db)

	// Outbox worker: publishes location.outbox_message rows (ride.summary.ready) to Kafka.
	publisher := kafkapublisher.New(kafkaBroker)
	defer publisher.Close()
	outboxWorker := outbox.New(serviceName, outboxRepo, publisher, transactionManager, logger, 2*time.Second)

	if err := metricsClient.Gauge("myubergo.outbox.pending", nil, func(ctx context.Context) (int64, error) {
		pending, _, err := outboxRepo.CountByRetries(ctx, outboxWorker.MaxRetries())
		return pending, err
	}); err != nil {
		log.Fatal(err)
	}
	if err := metricsClient.Gauge("myubergo.outbox.parked", nil, func(ctx context.Context) (int64, error) {
		_, parked, err := outboxRepo.CountByRetries(ctx, outboxWorker.MaxRetries())
		return parked, err
	}); err != nil {
		log.Fatal(err)
	}

	application := app.Application{
		Commands: app.Commands{
			IngestPings:         command.NewIngestPingsHandler(ownerRepo, driverLocationRepo, trackingRepo, positionPublisher, rideTrackRepo, validationConfig, logger, metricsClient),
			IngestClientPing:    command.NewIngestClientPingHandler(trackingRepo, clientLocationRepo, positionPublisher, rideTrackRepo, validationConfig, logger, metricsClient),
			UpsertOwner:         command.NewUpsertOwnerHandler(ownerRepo, logger, metricsClient),
			RecordRideRequested: command.NewRecordRideRequestedHandler(trackingRepo, logger, metricsClient),
			RecordRideAccepted:  command.NewRecordRideAcceptedHandler(trackingRepo, logger, metricsClient),
			CloseTrackingWindow: command.NewCloseTrackingWindowHandler(trackingRepo, logger, metricsClient),
			// mapMatcher is nil: the Slice-4 Geoapify adapter doesn't exist
			// yet, so every summary today is source=Simplified.
			BuildRideSummary: command.NewBuildRideSummaryHandler(
				trackingRepo, rideTrackRepo, summaryRepo, outboxRepo, transactionManager, nil, rdpEpsilonM, logger, metricsClient,
			),
		},
		Queries: app.Queries{
			FindNearbyDrivers:       query.NewFindNearbyDriversHandler(driverLocationRepo, logger, metricsClient),
			GetCounterpartyPosition: query.NewGetCounterpartyPositionHandler(trackingRepo, ownerRepo, driverLocationRepo, clientLocationRepo, logger, metricsClient),
			ListLivePositions:       query.NewListLivePositionsHandler(driverLocationRepo, clientLocationRepo, trackingRepo, logger, metricsClient),
			GetRideTrack:            query.NewGetRideTrackHandler(summaryRepo, ownerRepo, logger, metricsClient),
		},
	}

	locationHandler := handler.NewLocationHandler(application, logger)

	// Health checker: Redis + Postgres (Slice 3) — deliberately NOT the
	// DynamoDB history tier, see internal/infrastructure/health.NewChecker's
	// doc comment.
	healthChecker := health.NewChecker(redisDb, db, 5*time.Second)
	healthChecker.Start()
	defer healthChecker.Stop()

	mux := http.NewServeMux()

	// Health check endpoints
	mux.HandleFunc("GET /health/live", healthChecker.LiveHandler)
	mux.HandleFunc("GET /health/ready", healthChecker.ReadyHandler)

	// Client-facing (via Kong, /api/location -> strip_path -> here)
	mux.HandleFunc("POST /batch", locationHandler.IngestBatch)
	// Full client-facing path, not bare /rides/{rideId}/counterparty — Kong's route for
	// this one uses strip_path:false, see gateway/kong.yml's location-service-counterparty.
	mux.HandleFunc("GET /api/location/rides/{rideId}/counterparty", locationHandler.GetCounterparty)
	// Same strip_path:false reasoning as counterparty — see gateway/kong.yml's
	// location-service-track.
	mux.HandleFunc("GET /api/location/rides/{rideId}/track", locationHandler.GetRideTrack)
	// Admin-only fleet-wide snapshot — gated at Kong (require_admin), see
	// gateway/kong.yml's location-service-admin-positions.
	mux.HandleFunc("GET /api/location/positions", locationHandler.ListLivePositions)

	// Internal-only (no Kong route, network-isolated — see CLAUDE.md's API Gateway section)
	mux.HandleFunc("GET /internal/drivers/nearby", locationHandler.NearbyDrivers)

	// Create HTTP server
	server := &http.Server{
		Addr: ":" + port,
		// /ws excluded from otelhttp: it would hold one span open for the connection's
		// whole lifetime, and it also keeps the ResponseWriter hijackable — see obshttp.Handler.
		Handler:      httpmw.BodyLimit(httpmw.RequestID(obshttp.Handler(httpmw.Recover(logger)(mux), serviceName, "/ws"))),
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

	// Registered here, not with the other routes: WSHandler needs shutdownManager, built
	// after mux — safe since mux is a live pointer and nothing's dispatched until Listen.
	wsHandler := ws.NewWSHandler(application, trackingRepo, ownerRepo, hub, healthChecker, shutdownManager, logger, metricsClient, wsPingInterval)
	mux.HandleFunc("GET /ws", wsHandler.ServeWS)

	// server.Shutdown() doesn't reach hijacked WS connections — force-close them explicitly.
	shutdownManager.OnStop(hub.CloseAll)

	// The one genuinely long-lived worker in Slice 2 — unlike the per-connection WS
	// pumps (nil workerCtx), this one's early exit is a real liveness failure.
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, bgCtx, "pubsub-dispatcher", func() {
		defer shutdownManager.Done()
		dispatcher.Run(bgCtx, hub)
	})

	shiftUpdatedConsumer := consumers.NewShiftUpdatedConsumer(application, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, bgCtx, "shift-updated-consumer", func() {
		defer shutdownManager.Done()
		shiftUpdatedConsumer.Run(bgCtx, "shift.updated")
	})

	// Tracking-window consumers (Slice 2): open on ride.requested/accepted (either order,
	// see domain.TrackingRepository), close + force-close WS on completed/cancelled.
	socketCloser := hub

	rideRequestedConsumer := consumers.NewRideRequestedConsumer(application, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, bgCtx, "ride-requested-consumer", func() {
		defer shutdownManager.Done()
		rideRequestedConsumer.Run(bgCtx, "ride.requested")
	})

	rideAcceptedConsumer := consumers.NewRideAcceptedConsumer(application, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, bgCtx, "ride-accepted-consumer", func() {
		defer shutdownManager.Done()
		rideAcceptedConsumer.Run(bgCtx, "ride.accepted")
	})

	rideCompletedConsumer := consumers.NewRideCompletedConsumer(application, socketCloser, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, bgCtx, "ride-completed-consumer", func() {
		defer shutdownManager.Done()
		rideCompletedConsumer.Run(bgCtx, "ride.completed")
	})

	rideCancelledConsumer := consumers.NewRideCancelledConsumer(application, socketCloser, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, bgCtx, "ride-cancelled-consumer", func() {
		defer shutdownManager.Done()
		rideCancelledConsumer.Run(bgCtx, "ride.cancelled")
	})

	stalenessWorker := workers.NewStalenessWorker(driverLocationRepo, stalenessThreshold, sweepInterval, logger, metricsClient)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, bgCtx, "staleness-worker", func() {
		defer shutdownManager.Done()
		stalenessWorker.Run(bgCtx)
	})

	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, bgCtx, "outbox-worker", func() {
		defer shutdownManager.Done()
		outboxWorker.Run(bgCtx)
	})

	archiveWorker := workers.NewArchiveWorker(trackingRepo, rideTrackRepo, historyRepo, archiveInterval, archiveBatch, logger, metricsClient)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, bgCtx, "archive-worker", func() {
		defer shutdownManager.Done()
		archiveWorker.Run(bgCtx)
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
