package main

import (
	"context"
	app "driver-service/internal/application"
	"driver-service/internal/application/command"
	"driver-service/internal/application/query"
	"driver-service/internal/consumers"
	"driver-service/internal/infrastructure/health"
	"driver-service/internal/infrastructure/metrics"
	"driver-service/internal/infrastructure/shutdown"
	"driver-service/internal/interfaces/http/handler"
	"driver-service/internal/persistence"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/oxf/MyUber/common/dbconn"
	"github.com/oxf/MyUber/common/envconfig"
	httpmw "github.com/oxf/MyUber/common/httpmiddleware"
	"github.com/oxf/MyUber/common/kafkapublisher"
	"github.com/oxf/MyUber/common/outbox"
	"github.com/oxf/MyUber/observability/obshttp"
	"github.com/oxf/MyUber/observability/obslog"
	"github.com/oxf/MyUber/observability/otelinit"

	_ "github.com/lib/pq"
)

const serviceName = "driver-service"

const defaultPgDsn = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"

func main() {
	// `app healthcheck` backs Docker's HEALTHCHECK: distroless has no shell/curl,
	// so the binary probes its own /health/ready itself and exits 0/1.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		health.HealthcheckSelf("http://localhost:8003/health/live")
	}

	// otelinit.Setup installs the global tracer/meter/logger providers from OTEL_* env vars.
	// It never fails boot on a down Collector — OTLP/gRPC exporters dial lazily and retry.
	ctx := context.Background()
	providers, err := otelinit.Setup(ctx, serviceName)
	if err != nil {
		log.Fatal(err)
	}

	logger := obslog.NewLogger(serviceName)

	db, err := dbconn.Open(defaultPgDsn)
	if err != nil {
		log.Fatal(err)
	}

	kafkaBroker := envconfig.String("KAFKA_BROKER", "kafka:29092")

	profileRepo := persistence.NewPostgresDriverRepository(db)
	shiftRepo := persistence.NewPostgresShiftRepository(db)
	outboxRepo := persistence.NewPostgresOutboxRepository(db)
	transactionManager := persistence.NewPostgresTransactionManager(db)

	// Outbox worker: publishes driver.outbox_message rows (e.g. shift.updated) to Kafka
	publisher := kafkapublisher.New(kafkaBroker)
	defer publisher.Close()
	outboxWorker := outbox.New(serviceName, outboxRepo, publisher, transactionManager, logger, 2*time.Second)

	metricsClient := metrics.NewOtelMetricsClient(serviceName)

	// Outbox backlog gauges: "pending" will still retry, "parked" exceeded outboxWorker.MaxRetries()
	// and needs manual triage (see PLAN.md) — previously this backlog had no signal at all.
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
			CreateDriver:         command.NewCreateDriverHandler(profileRepo, logger, metricsClient),
			UpdateDriver:         command.NewUpdateDriverHandler(profileRepo, logger, metricsClient),
			CreateShift:          command.NewCreateShiftHandler(shiftRepo),
			UpdateShift:          command.NewUpdateShiftHandler(shiftRepo, profileRepo, outboxRepo, transactionManager, logger, metricsClient),
			ProcessRideAccepted:  command.NewProcessRideAcceptedHandler(profileRepo, transactionManager, logger, metricsClient),
			ProcessRideCancelled: command.NewProcessRideCancelledHandler(profileRepo, transactionManager, logger, metricsClient),
			ProcessRideCompleted: command.NewProcessRideCompletedHandler(profileRepo, transactionManager, logger, metricsClient),
		},
		Queries: app.Queries{
			GetDriverList: query.NewGetDriverListHandler(profileRepo, logger, metricsClient),
			GetDriverByID: query.NewGetDriverByIDHandler(profileRepo, logger, metricsClient),
			GetShiftList:  query.NewGetShiftListHandler(shiftRepo, logger, metricsClient),
			GetShiftByID:  query.NewGetShiftByIDHandler(shiftRepo, logger, metricsClient),
		},
	}

	profileHandler := handler.NewDriverHandler(application, logger)
	shiftHandler := handler.NewShiftHandler(application, logger)

	// Initialize health checker
	healthChecker := health.NewChecker(db, 5*time.Second)
	healthChecker.Start()
	defer healthChecker.Stop()

	mux := http.NewServeMux()

	// Health check endpoints
	mux.HandleFunc("GET /health/live", healthChecker.LiveHandler)
	mux.HandleFunc("GET /health/ready", healthChecker.ReadyHandler)

	// API endpoints
	mux.HandleFunc("POST /driver", profileHandler.Create)
	mux.HandleFunc("PUT /driver/{id}", profileHandler.Update)
	mux.HandleFunc("GET /driver", profileHandler.GetList)
	mux.HandleFunc("GET /driver/{id}", profileHandler.GetByID)
	mux.HandleFunc("POST /driver-shift/create", shiftHandler.Create)
	mux.HandleFunc("PUT /driver-shift/{id}", shiftHandler.Update)
	mux.HandleFunc("GET /driver-shift", shiftHandler.GetList)
	mux.HandleFunc("GET /driver-shift/{id}", shiftHandler.GetByID)

	// Create HTTP server
	server := &http.Server{
		Addr:         ":8003",
		Handler:      httpmw.BodyLimit(httpmw.RequestID(obshttp.Handler(httpmw.Recover(logger)(mux), serviceName))),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create shutdown manager with 30s timeout
	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// Flip readiness the moment shutdown begins, not up to checkInterval later —
	// the ticker-based DB-ping check alone wouldn't catch this promptly.
	shutdownManager.OnStop(healthChecker.MarkNotReady)

	// Flush providers only after every worker below has actually drained, not merely
	// told to stop — an OnStop hook here would silently drop the drain period's telemetry.
	shutdownManager.OnDrained(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := providers.Shutdown(shutdownCtx); err != nil {
			log.Printf("observability shutdown error: %v\n", err)
		}
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	shutdownManager.OnStop(cancelWorker)

	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, workerCtx, "outbox-worker", func() {
		defer shutdownManager.Done()
		outboxWorker.Run(workerCtx)
	})

	// ride.accepted consumer: flips the matched driver's profile status
	// Online -> OnRide via ProcessRideAcceptedHandler.
	rideAcceptedConsumer := consumers.NewRideAcceptedConsumer(application, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, workerCtx, "ride-accepted-consumer", func() {
		defer shutdownManager.Done()
		rideAcceptedConsumer.Run(workerCtx, "ride.accepted")
	})

	// ride.cancelled consumer: flips status OnRide -> Online via ProcessRideCancelledHandler
	// (no-op if the ride was cancelled before a match existed).
	rideCancelledConsumer := consumers.NewRideCancelledConsumer(application, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, workerCtx, "ride-cancelled-consumer", func() {
		defer shutdownManager.Done()
		rideCancelledConsumer.Run(workerCtx, "ride.cancelled")
	})

	// ride.completed consumer: flips status OnRide -> Online and increments
	// total_rides_completed via ProcessRideCompletedHandler.
	rideCompletedConsumer := consumers.NewRideCompletedConsumer(application, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, workerCtx, "ride-completed-consumer", func() {
		defer shutdownManager.Done()
		rideCompletedConsumer.Run(workerCtx, "ride.completed")
	})

	// Start server in a goroutine
	health.GoSafe(logger, healthChecker, nil, "http-server", func() {
		logger.Info("driver-service listening on :8003")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Error("server error")
		}
	})

	// Wait for shutdown signal and perform graceful shutdown
	shutdownManager.WaitForShutdown()
}
