package main

import (
	"context"
	"database/sql"
	app "driver-service/internal/application"
	"driver-service/internal/application/command"
	"driver-service/internal/application/query"
	"driver-service/internal/consumers"
	"driver-service/internal/infrastructure/health"
	kafkainfra "driver-service/internal/infrastructure/kafka"
	"driver-service/internal/infrastructure/metrics"
	"driver-service/internal/infrastructure/shutdown"
	"driver-service/internal/interfaces/http/handler"
	"driver-service/internal/interfaces/http/middleware"
	"driver-service/internal/persistence"
	"driver-service/internal/workers"
	"log"
	"net/http"
	"os"
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

	profileRepo := persistence.NewPostgresDriverRepository(db)
	shiftRepo := persistence.NewPostgresShiftRepository(db)
	outboxRepo := persistence.NewPostgresOutboxRepository(db)
	transactionManager := persistence.NewPostgresTransactionManager(db)

	// create logger and metrics client used by decorators
	logger := logrus.NewEntry(logrus.New())
	metricsClient := metrics.NewLoggingMetricsClient(logger)

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

	profileHandler := handler.NewDriverHandler(application)
	shiftHandler := handler.NewShiftHandler(application)

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
		Handler:      middleware.RequestID(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create shutdown manager with 30s timeout
	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// Outbox worker: publishes driver.outbox_message rows (e.g. shift.updated) to Kafka
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

	// ride.accepted consumer: flips the matched driver's profile status
	// Online -> OnRide via ProcessRideAcceptedHandler.
	rideAcceptedConsumer := consumers.NewRideAcceptedConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	go func() {
		defer shutdownManager.Done()
		rideAcceptedConsumer.Run(workerCtx, "ride.accepted")
	}()

	// ride.cancelled consumer: flips the driver's profile status back
	// OnRide -> Online via ProcessRideCancelledHandler (no-op if the ride
	// was cancelled before a match existed).
	rideCancelledConsumer := consumers.NewRideCancelledConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	go func() {
		defer shutdownManager.Done()
		rideCancelledConsumer.Run(workerCtx, "ride.cancelled")
	}()

	// ride.completed consumer: flips the driver's profile status back
	// OnRide -> Online and increments total_rides_completed via
	// ProcessRideCompletedHandler.
	rideCompletedConsumer := consumers.NewRideCompletedConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	go func() {
		defer shutdownManager.Done()
		rideCompletedConsumer.Run(workerCtx, "ride.completed")
	}()

	// Start server in a goroutine
	go func() {
		log.Println("driver-service listening on :8003")
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
