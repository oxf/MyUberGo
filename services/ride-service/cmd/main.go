package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	app "ride-service/internal/application"
	"ride-service/internal/application/command"
	"ride-service/internal/application/query"
	"ride-service/internal/consumers"
	"ride-service/internal/infrastructure/fee"
	"ride-service/internal/infrastructure/health"
	kafkainfra "ride-service/internal/infrastructure/kafka"
	"ride-service/internal/infrastructure/metrics"
	"ride-service/internal/infrastructure/shutdown"
	"ride-service/internal/interfaces/http/handler"
	"ride-service/internal/interfaces/http/middleware"
	"ride-service/internal/persistence"
	"ride-service/internal/workers"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

const defaultPgDsn = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"

func main() {
	// `app healthcheck` is invoked by Docker's HEALTHCHECK (see
	// docker-compose.yml) — distroless has no shell/curl for a CMD-SHELL
	// check, so the binary probes its own /health/ready and exits 0/1.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		healthcheckSelf("http://localhost:8001/health/ready")
	}

	dsn := getenv("PG_DSN", defaultPgDsn)
	if os.Getenv("APP_ENV") == "production" && dsn == defaultPgDsn {
		log.Fatal("refusing to start in production with the default PG_DSN — set a real PG_DSN")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(atoi(getenv("DB_MAX_OPEN_CONNS", "20")))
	db.SetMaxIdleConns(atoi(getenv("DB_MAX_IDLE_CONNS", "10")))
	db.SetConnMaxLifetime(time.Duration(atoi(getenv("DB_CONN_MAX_LIFETIME_MIN", "5"))) * time.Minute)

	kafkaBroker := getenv("KAFKA_BROKER", "kafka:29092")

	rideRepo := persistence.NewPostgresRideRepository(db)
	tariffRepo := persistence.NewPostgresTariffRepository(db)
	outboxRepo := persistence.NewPostgresOutboxRepository(db)
	transactionManager := persistence.NewPostgresTransactionManager(db)
	feeCalculator := fee.NewStubCalculator()

	// create logger and metrics client used by decorators
	logger := logrus.NewEntry(logrus.New())
	metricsClient := metrics.NewLoggingMetricsClient(logger)

	application := app.Application{
		Commands: app.Commands{
			CreateRide:      command.NewCreateRideHandler(rideRepo, tariffRepo, outboxRepo, transactionManager, logger, metricsClient),
			MarkRideMatched: command.NewMarkRideMatchedHandler(rideRepo, logger, metricsClient),
			MarkRideBilled:  command.NewMarkRideBilledHandler(rideRepo, logger, metricsClient),
			CancelRide:      command.NewCancelRideHandler(rideRepo, outboxRepo, transactionManager, feeCalculator, logger, metricsClient),
			StartRide:       command.NewStartRideHandler(rideRepo, outboxRepo, transactionManager, logger, metricsClient),
			CompleteRide:    command.NewCompleteRideHandler(rideRepo, outboxRepo, transactionManager, logger, metricsClient),
		},
		Queries: app.Queries{
			GetRideList: query.NewGetRideListHandler(rideRepo, logger, metricsClient),
			GetRideByID: query.NewGetRideByIDHandler(rideRepo, logger, metricsClient),
		},
	}

	rideHandler := handler.NewRideHandler(application)

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
		Handler:      middleware.BodyLimit(middleware.RequestID(middleware.Recover(logger)(mux))),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create shutdown manager with 30s timeout
	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// Outbox worker: publishes ride.outbox_message rows (e.g. ride.requested) to Kafka
	publisher := kafkainfra.NewPublisher(kafkaBroker)
	defer publisher.Close()

	outboxWorker := workers.NewOutboxWorker(outboxRepo, publisher, transactionManager, logger, 2*time.Second)
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	shutdownManager.OnStop(cancelWorker)

	shutdownManager.Add(1)
	goSafe(logger, "outbox-worker", func() {
		defer shutdownManager.Done()
		outboxWorker.Run(workerCtx)
	})

	// ride.accepted consumer: flips a matched ride's driver_id/status/matched_at
	// once matching-service publishes the match.
	rideAcceptedConsumer := consumers.NewRideAcceptedConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	goSafe(logger, "ride-accepted-consumer", func() {
		defer shutdownManager.Done()
		rideAcceptedConsumer.Run(workerCtx, "ride.accepted")
	})

	// payment.completed consumer: sets ride.ride.bill_id once billing-service
	// collects payment for the ride.
	paymentCompletedConsumer := consumers.NewPaymentCompletedConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	goSafe(logger, "payment-completed-consumer", func() {
		defer shutdownManager.Done()
		paymentCompletedConsumer.Run(workerCtx, "payment.completed")
	})

	// Start server in a goroutine
	goSafe(logger, "http-server", func() {
		log.Println("ride-service listening on :8001")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v\n", err)
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

func atoi(s string) int { v, _ := strconv.Atoi(s); return v }

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

// goSafe launches fn in a goroutine, recovering any panic so one bad
// message/query doesn't take the whole process down silently.
func goSafe(logger *logrus.Entry, name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.WithField("goroutine", name).WithField("panic", r).Error("recovered from panic")
			}
		}()
		fn()
	}()
}
