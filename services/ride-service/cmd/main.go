package main

import (
	"context"
	"log"
	"net/http"
	"os"
	app "ride-service/internal/application"
	"ride-service/internal/application/command"
	"ride-service/internal/application/query"
	"ride-service/internal/consumers"
	"ride-service/internal/infrastructure/fee"
	"ride-service/internal/infrastructure/health"
	"ride-service/internal/infrastructure/metrics"
	"ride-service/internal/infrastructure/shutdown"
	"ride-service/internal/interfaces/http/handler"
	"ride-service/internal/persistence"
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

const serviceName = "ride-service"

const defaultPgDsn = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"

func main() {
	// `app healthcheck` backs Docker's HEALTHCHECK: distroless has no shell/curl,
	// so the binary probes its own /health/ready and exits 0/1.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		health.HealthcheckSelf("http://localhost:8001/health/live")
	}

	// Installs the global tracer/meter/logger providers from standard OTEL_* env vars.
	// Never fails the boot on a down Collector — exporters dial lazily and retry in the background.
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

	rideRepo := persistence.NewPostgresRideRepository(db)
	tariffRepo := persistence.NewPostgresTariffRepository(db)
	outboxRepo := persistence.NewPostgresOutboxRepository(db)
	transactionManager := persistence.NewPostgresTransactionManager(db)
	feeCalculator := fee.NewStubCalculator()

	// Outbox worker: publishes ride.outbox_message rows (e.g. ride.requested) to Kafka
	publisher := kafkapublisher.New(kafkaBroker)
	defer publisher.Close()
	outboxWorker := outbox.New(serviceName, outboxRepo, publisher, transactionManager, logger, 2*time.Second)

	metricsClient := metrics.NewOtelMetricsClient(serviceName)

	// Outbox backlog gauges: "pending" will retry, "parked" exceeded outboxWorker.MaxRetries()
	// and needs manual triage (see PLAN.md) — previously this had no signal at all.
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
			CreateRide:      command.NewCreateRideHandler(rideRepo, tariffRepo, outboxRepo, transactionManager, logger, metricsClient),
			MarkRideMatched: command.NewMarkRideMatchedHandler(rideRepo, logger, metricsClient),
			MarkRideBilled:  command.NewMarkRideBilledHandler(rideRepo, logger, metricsClient),
			FailRide:        command.NewFailRideHandler(rideRepo, logger, metricsClient),
			CancelRide:      command.NewCancelRideHandler(rideRepo, outboxRepo, transactionManager, feeCalculator, logger, metricsClient),
			StartRide:       command.NewStartRideHandler(rideRepo, outboxRepo, transactionManager, logger, metricsClient),
			CompleteRide:    command.NewCompleteRideHandler(rideRepo, outboxRepo, transactionManager, logger, metricsClient),
		},
		Queries: app.Queries{
			GetRideList: query.NewGetRideListHandler(rideRepo, logger, metricsClient),
			GetRideByID: query.NewGetRideByIDHandler(rideRepo, logger, metricsClient),
		},
	}

	rideHandler := handler.NewRideHandler(application, logger)

	// Initialize health checker
	healthChecker := health.NewChecker(db, 5*time.Second)
	healthChecker.Start()
	defer healthChecker.Stop()

	mux := http.NewServeMux()

	// Health check endpoints
	mux.HandleFunc("GET /health/live", healthChecker.LiveHandler)
	mux.HandleFunc("GET /health/ready", healthChecker.ReadyHandler)

	// API endpoints
	mux.HandleFunc("POST /request-ride", rideHandler.Create)
	mux.HandleFunc("GET /ride", rideHandler.GetList)
	mux.HandleFunc("GET /ride/{id}", rideHandler.GetByID)
	mux.HandleFunc("DELETE /ride/{id}", rideHandler.Cancel)
	mux.HandleFunc("POST /ride/{id}/start", rideHandler.Start)
	mux.HandleFunc("POST /ride/{id}/complete", rideHandler.Complete)

	// Create HTTP server
	server := &http.Server{
		Addr:         ":8001",
		Handler:      httpmw.BodyLimit(httpmw.RequestID(obshttp.Handler(httpmw.Recover(logger)(mux), serviceName))),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create shutdown manager with 30s timeout
	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// Flip readiness the instant shutdown begins, rather than waiting up to
	// checkInterval for the ticker-based DB-ping check to catch up.
	shutdownManager.OnStop(healthChecker.MarkNotReady)

	// Flush providers only after every worker below has actually drained (not just
	// been told to stop) — see shutdown.Manager.OnDrained for why OnStop is too early.
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

	// ride.accepted consumer: flips a matched ride's driver_id/status/matched_at
	// once matching-service publishes the match.
	rideAcceptedConsumer := consumers.NewRideAcceptedConsumer(application, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, workerCtx, "ride-accepted-consumer", func() {
		defer shutdownManager.Done()
		rideAcceptedConsumer.Run(workerCtx, "ride.accepted")
	})

	// payment.completed consumer: sets ride.ride.bill_id once billing-service
	// collects payment for the ride.
	paymentCompletedConsumer := consumers.NewPaymentCompletedConsumer(application, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, workerCtx, "payment-completed-consumer", func() {
		defer shutdownManager.Done()
		paymentCompletedConsumer.Run(workerCtx, "payment.completed")
	})

	// ride.matching_failed consumer: flips a ride to Failed once matching-service
	// gives up after exhausting its retries (docs/AUDIT_2026-08-15.md #11).
	rideMatchingFailedConsumer := consumers.NewRideMatchingFailedConsumer(application, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, workerCtx, "ride-matching-failed-consumer", func() {
		defer shutdownManager.Done()
		rideMatchingFailedConsumer.Run(workerCtx, "ride.matching_failed")
	})

	// Start server in a goroutine
	health.GoSafe(logger, healthChecker, nil, "http-server", func() {
		logger.Info("ride-service listening on :8001")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Error("server error")
		}
	})

	// Wait for shutdown signal and perform graceful shutdown
	shutdownManager.WaitForShutdown()
}
