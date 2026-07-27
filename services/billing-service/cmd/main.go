package main

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/application/query"
	"billing-service/internal/consumers"
	"billing-service/internal/infrastructure/health"
	kafkainfra "billing-service/internal/infrastructure/kafka"
	"billing-service/internal/infrastructure/metrics"
	"billing-service/internal/infrastructure/payment/stub"
	"billing-service/internal/infrastructure/shutdown"
	"billing-service/internal/interfaces/http/handler"
	"billing-service/internal/interfaces/http/middleware"
	"billing-service/internal/persistence"
	"billing-service/internal/workers"
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
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
	commissionBps := int64(atoi(getenv("PLATFORM_COMMISSION_BPS", "2000")))
	maxAttempts := atoi(getenv("MAX_PAYMENT_ATTEMPTS", "3"))
	backoff := parseDurations(getenv("PAYMENT_BACKOFF", "1m,5m,30m"))

	customerRepo := persistence.NewPostgresCustomerRepository(db)
	paymentMethodRepo := persistence.NewPostgresPaymentMethodRepository(db)
	invoiceRepo := persistence.NewPostgresInvoiceRepository(db)
	paymentRepo := persistence.NewPostgresPaymentRepository(db)
	ledgerRepo := persistence.NewPostgresLedgerRepository(db)
	outboxRepo := persistence.NewPostgresOutboxRepository(db)
	transactionManager := persistence.NewPostgresTransactionManager(db)
	paymentProvider := stub.NewStubProvider()

	logger := logrus.NewEntry(logrus.New())
	metricsClient := metrics.NewLoggingMetricsClient(logger)

	application := app.Application{
		Commands: app.Commands{
			AddPaymentMethod:      command.NewAddPaymentMethodHandler(customerRepo, paymentMethodRepo, transactionManager, logger, metricsClient),
			RemovePaymentMethod:   command.NewRemovePaymentMethodHandler(paymentMethodRepo, invoiceRepo, transactionManager, logger, metricsClient),
			CreateInvoiceFromRide: command.NewCreateInvoiceFromRideHandler(invoiceRepo, ledgerRepo, transactionManager, commissionBps, logger, metricsClient),
		},
		Queries: app.Queries{
			GetInvoice:         query.NewGetInvoiceHandler(invoiceRepo, logger, metricsClient),
			GetInvoiceByRide:   query.NewGetInvoiceByRideHandler(invoiceRepo, logger, metricsClient),
			ListInvoices:       query.NewListInvoicesHandler(invoiceRepo, logger, metricsClient),
			ListPaymentMethods: query.NewListPaymentMethodsHandler(paymentMethodRepo, logger, metricsClient),
			GetLedgerBalance:   query.NewGetLedgerBalanceHandler(ledgerRepo, logger, metricsClient),
		},
	}

	paymentMethodHandler := handler.NewPaymentMethodHandler(application)
	invoiceHandler := handler.NewInvoiceHandler(application)
	ledgerHandler := handler.NewLedgerHandler(application)

	healthChecker := health.NewChecker(db, 5*time.Second)
	healthChecker.Start()
	defer healthChecker.Stop()

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health/live", healthChecker.LiveHandler)
	mux.HandleFunc("GET /health/ready", healthChecker.ReadyHandler)

	mux.HandleFunc("POST /payment-methods", paymentMethodHandler.Create)
	mux.HandleFunc("GET /payment-methods", paymentMethodHandler.List)
	mux.HandleFunc("DELETE /payment-methods/{id}", paymentMethodHandler.Delete)
	mux.HandleFunc("GET /invoices", invoiceHandler.GetList)
	mux.HandleFunc("GET /invoices/{id}", invoiceHandler.GetByID)
	mux.HandleFunc("GET /rides/{rideId}/invoice", invoiceHandler.GetByRideID)
	mux.HandleFunc("GET /ledger/balance", ledgerHandler.GetBalance)

	server := &http.Server{
		Addr:         ":8005",
		Handler:      middleware.RequestID(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// Outbox worker: publishes billing.outbox_message rows (payment.completed,
	// payment.failed) to Kafka.
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

	// ChargeWorker: sweeps open invoices due for a collection attempt.
	chargeWorker := workers.NewChargeWorker(
		invoiceRepo, paymentRepo, paymentMethodRepo, ledgerRepo, outboxRepo,
		paymentProvider, transactionManager, logger,
		time.Duration(atoi(getenv("CHARGE_WORKER_INTERVAL_SEC", "5")))*time.Second,
		maxAttempts, backoff, commissionBps,
	)
	shutdownManager.Add(1)
	go func() {
		defer shutdownManager.Done()
		chargeWorker.Run(workerCtx)
	}()

	// ride.completed consumer: creates a ride_fare invoice + posts T1.
	rideCompletedConsumer := consumers.NewRideCompletedConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	go func() {
		defer shutdownManager.Done()
		rideCompletedConsumer.Run(workerCtx, "ride.completed")
	}()

	// ride.cancelled consumer: creates a cancellation_fee invoice + posts T1
	// only when a nonzero fee was charged.
	rideCancelledConsumer := consumers.NewRideCancelledConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	go func() {
		defer shutdownManager.Done()
		rideCancelledConsumer.Run(workerCtx, "ride.cancelled")
	}()

	go func() {
		log.Println("billing-service listening on :8005")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v\n", err)
		}
	}()

	shutdownManager.WaitForShutdown()
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func atoi(s string) int { v, _ := strconv.Atoi(s); return v }

func parseDurations(s string) []time.Duration {
	parts := strings.Split(s, ",")
	durations := make([]time.Duration, 0, len(parts))
	for _, p := range parts {
		d, err := time.ParseDuration(strings.TrimSpace(p))
		if err != nil {
			continue
		}
		durations = append(durations, d)
	}
	if len(durations) == 0 {
		durations = []time.Duration{time.Minute}
	}
	return durations
}
