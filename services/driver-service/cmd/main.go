package main

import (
	"context"
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
	"strconv"
	"time"

	"github.com/oxf/MyUber/observability/obsdb"
	"github.com/oxf/MyUber/observability/obshttp"
	"github.com/oxf/MyUber/observability/obslog"
	"github.com/oxf/MyUber/observability/otelinit"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

const serviceName = "driver-service"

const defaultPgDsn = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"

func main() {
	// `app healthcheck` is invoked by Docker's HEALTHCHECK (see
	// docker-compose.yml) — distroless has no shell/curl for a CMD-SHELL
	// check, so the binary probes its own /health/ready and exits 0/1.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		healthcheckSelf("http://localhost:8003/health/live")
	}

	// otelinit.Setup reads the standard OTEL_* env vars (see
	// docker-compose.yml) and installs the global tracer/meter/logger
	// providers. It never fails the boot on a down Collector — OTLP/gRPC
	// exporters dial lazily and retry in the background.
	ctx := context.Background()
	providers, err := otelinit.Setup(ctx, serviceName)
	if err != nil {
		log.Fatal(err)
	}

	logger := obslog.NewLogger(serviceName)

	dsn := getenv("PG_DSN", defaultPgDsn)
	if os.Getenv("APP_ENV") == "production" && dsn == defaultPgDsn {
		log.Fatal("refusing to start in production with the default PG_DSN — set a real PG_DSN")
	}
	db, err := obsdb.Open("postgres", dsn, serviceName, "postgresql")
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(atoi(getenv("DB_MAX_OPEN_CONNS", "20")))
	db.SetMaxIdleConns(atoi(getenv("DB_MAX_IDLE_CONNS", "10")))
	db.SetConnMaxLifetime(time.Duration(atoi(getenv("DB_CONN_MAX_LIFETIME_MIN", "5"))) * time.Minute)

	kafkaBroker := getenv("KAFKA_BROKER", "kafka:29092")

	profileRepo := persistence.NewPostgresDriverRepository(db)
	shiftRepo := persistence.NewPostgresShiftRepository(db)
	outboxRepo := persistence.NewPostgresOutboxRepository(db)
	transactionManager := persistence.NewPostgresTransactionManager(db)

	metricsClient := metrics.NewOtelMetricsClient(serviceName)

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
		Handler:      middleware.BodyLimit(middleware.RequestID(obshttp.Handler(middleware.Recover(logger)(mux), serviceName))),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create shutdown manager with 30s timeout
	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// GET /health/ready should stop reporting healthy the moment shutdown
	// begins, not up to checkInterval later — the ticker-based DB-ping check
	// alone wouldn't catch this promptly.
	shutdownManager.OnStop(healthChecker.MarkNotReady)

	// Flush/close the trace/metric/log providers during graceful shutdown,
	// after the HTTP server stops accepting new requests but before the
	// process exits, so in-flight batched telemetry isn't dropped on exit.
	shutdownManager.OnStop(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := providers.Shutdown(shutdownCtx); err != nil {
			log.Printf("observability shutdown error: %v\n", err)
		}
	})

	// Outbox worker: publishes driver.outbox_message rows (e.g. shift.updated) to Kafka
	publisher := kafkainfra.NewPublisher(kafkaBroker)
	defer publisher.Close()

	outboxWorker := workers.NewOutboxWorker(outboxRepo, publisher, transactionManager, logger, 2*time.Second)
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	shutdownManager.OnStop(cancelWorker)

	shutdownManager.Add(1)
	goSafe(logger, healthChecker, workerCtx, "outbox-worker", func() {
		defer shutdownManager.Done()
		outboxWorker.Run(workerCtx)
	})

	// ride.accepted consumer: flips the matched driver's profile status
	// Online -> OnRide via ProcessRideAcceptedHandler.
	rideAcceptedConsumer := consumers.NewRideAcceptedConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	goSafe(logger, healthChecker, workerCtx, "ride-accepted-consumer", func() {
		defer shutdownManager.Done()
		rideAcceptedConsumer.Run(workerCtx, "ride.accepted")
	})

	// ride.cancelled consumer: flips the driver's profile status back
	// OnRide -> Online via ProcessRideCancelledHandler (no-op if the ride
	// was cancelled before a match existed).
	rideCancelledConsumer := consumers.NewRideCancelledConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	goSafe(logger, healthChecker, workerCtx, "ride-cancelled-consumer", func() {
		defer shutdownManager.Done()
		rideCancelledConsumer.Run(workerCtx, "ride.cancelled")
	})

	// ride.completed consumer: flips the driver's profile status back
	// OnRide -> Online and increments total_rides_completed via
	// ProcessRideCompletedHandler.
	rideCompletedConsumer := consumers.NewRideCompletedConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	goSafe(logger, healthChecker, workerCtx, "ride-completed-consumer", func() {
		defer shutdownManager.Done()
		rideCompletedConsumer.Run(workerCtx, "ride.completed")
	})

	// Start server in a goroutine
	goSafe(logger, healthChecker, nil, "http-server", func() {
		log.Println("driver-service listening on :8003")
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
// message/query doesn't take the whole process down silently. If workerCtx
// is non-nil and fn returns while workerCtx.Err() is still nil — i.e. fn
// exited (crashed or returned early) without being told to stop — that's a
// genuine liveness failure: a background worker this service depends on
// (an outbox worker, a Kafka consumer) is gone and won't come back, and
// GET /health/live should say so. Pass a nil workerCtx for goroutines with
// no associated cancellation context, like the HTTP server itself, which
// exits cleanly via http.ErrServerClosed on a normal shutdown.
func goSafe(logger *logrus.Entry, healthChecker *health.Checker, workerCtx context.Context, name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.WithField("goroutine", name).WithField("panic", r).Error("recovered from panic")
			}
			if workerCtx != nil && workerCtx.Err() == nil {
				healthChecker.MarkNotLive(name + " exited unexpectedly")
			}
		}()
		fn()
	}()
}
