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
	"ride-service/internal/infrastructure/health"
	kafkainfra "ride-service/internal/infrastructure/kafka"
	"ride-service/internal/infrastructure/metrics"
	"ride-service/internal/infrastructure/shutdown"
	"ride-service/internal/interfaces/http/handler"
	"ride-service/internal/interfaces/http/middleware"
	"ride-service/internal/persistence"
	"ride-service/internal/workers"
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

	kafkaBroker := getenv("KAFKA_BROKER", "kafka:29092")

	rideRepo := persistence.NewPostgresRideRepository(db)
	outboxRepo := persistence.NewPostgresOutboxRepository(db)
	transactionManager := persistence.NewPostgresTransactionManager(db)

	// create logger and metrics client used by decorators
	logger := logrus.NewEntry(logrus.New())
	metricsClient := metrics.NewLoggingMetricsClient(logger)

	application := app.Application{
		Commands: app.Commands{
			CreateRide: command.NewCreateRideHandler(rideRepo, outboxRepo, transactionManager, logger, metricsClient),
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
