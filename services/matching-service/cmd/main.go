package main

import (
	"context"
	"log"
	"net/http"
	"os"
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
	"matching-service/internal/interfaces/http/middleware"
	"matching-service/internal/workers"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

func main() {
	redisUrl := getenv("REDIS_URL", "redis:6379")
	redisDb := redis.NewClient(&redis.Options{
		Addr:     redisUrl,
		Password: "",
		DB:       0,
	})

	kafkaBroker := getenv("KAFKA_BROKER", "kafka:29092")
	port := getenv("SERVICE_PORT", "8002")

	driverRepo := cache.NewDriverRepository(redisDb)
	rideRepo := cache.NewRideRepository(redisDb)
	offerRepo := cache.NewOfferRepository(redisDb)

	// create logger and metrics client used by decorators
	logger := logrus.NewEntry(logrus.New())
	metricsClient := metrics.NewLoggingMetricsClient(logger)

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
		Handler:      middleware.RequestID(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create shutdown manager with 30s timeout
	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// Cancellable context for background goroutines (Kafka consumers + retry worker)
	bgCtx, cancelBg := context.WithCancel(context.Background())
	shutdownManager.OnStop(cancelBg)

	rideConsumer := consumers.NewRideRequestedConsumer(application, kafkaBroker)
	driverConsumer := consumers.NewShiftUpdatedConsumer(application, kafkaBroker)
	rideCancelledConsumer := consumers.NewRideCancelledConsumer(application, kafkaBroker)

	shutdownManager.Add(3)
	go func() {
		defer shutdownManager.Done()
		rideConsumer.Run(bgCtx, "ride.requested")
	}()
	go func() {
		defer shutdownManager.Done()
		driverConsumer.Run(bgCtx, "shift.updated")
	}()
	go func() {
		defer shutdownManager.Done()
		rideCancelledConsumer.Run(bgCtx, "ride.cancelled")
	}()

	// Match retry worker: sweeps pending_ride:* and re-broadcasts offers for
	// rides whose deadline lapsed without an accept.
	retryWorker := workers.NewMatchRetryWorker(offerRepo, application.Commands.BroadcastOffers, logger, 5*time.Second)
	shutdownManager.Add(1)
	go func() {
		defer shutdownManager.Done()
		retryWorker.Run(bgCtx)
	}()

	// Start server in a goroutine
	go func() {
		log.Println("matching-service listening on :" + port)
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
