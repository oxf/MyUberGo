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

func main() {
	dsn := getenv("PG_DSN", "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable")
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
		Handler:      middleware.RequestID(mux),
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
	go func() {
		defer shutdownManager.Done()
		outboxWorker.Run(workerCtx)
	}()

	// ride.accepted consumer: flips a matched ride's driver_id/status/matched_at
	// once matching-service publishes the match.
	rideAcceptedConsumer := consumers.NewRideAcceptedConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	go func() {
		defer shutdownManager.Done()
		rideAcceptedConsumer.Run(workerCtx, "ride.accepted")
	}()

	// payment.completed consumer: sets ride.ride.bill_id once billing-service
	// collects payment for the ride.
	paymentCompletedConsumer := consumers.NewPaymentCompletedConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	go func() {
		defer shutdownManager.Done()
		paymentCompletedConsumer.Run(workerCtx, "payment.completed")
	}()

	// Start server in a goroutine
	go func() {
		log.Println("ride-service listening on :8001")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v\n", err)
		}
	}()

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
