package main

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/application/query"
	"billing-service/internal/application/services"
	"billing-service/internal/consumers"
	"billing-service/internal/domain"
	"billing-service/internal/infrastructure/health"
	kafkainfra "billing-service/internal/infrastructure/kafka"
	"billing-service/internal/infrastructure/metrics"
	"billing-service/internal/infrastructure/payment/stripe"
	"billing-service/internal/infrastructure/payment/stub"
	"billing-service/internal/infrastructure/shutdown"
	"billing-service/internal/interfaces/http/handler"
	"billing-service/internal/interfaces/http/middleware"
	"billing-service/internal/persistence"
	"billing-service/internal/workers"
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/oxf/MyUber/observability/obsdb"
	"github.com/oxf/MyUber/observability/obshttp"
	"github.com/oxf/MyUber/observability/obslog"
	"github.com/oxf/MyUber/observability/otelinit"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

const serviceName = "billing-service"

const defaultPgDsn = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"

func main() {
	// `app healthcheck` is invoked by Docker's HEALTHCHECK (see
	// docker-compose.yml) — distroless has no shell/curl for a CMD-SHELL
	// check, so the binary probes its own /health/ready and exits 0/1.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		healthcheckSelf("http://localhost:8005/health/live")
	}

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
	commissionBps := int64(atoi(getenv("PLATFORM_COMMISSION_BPS", "2000")))
	maxAttempts := atoi(getenv("MAX_PAYMENT_ATTEMPTS", "3"))
	backoff := parseDurations(getenv("PAYMENT_BACKOFF", "1m,5m,30m"))
	chargeLease := parseDuration(getenv("CHARGE_LEASE", "2m"), 2*time.Minute)

	customerRepo := persistence.NewPostgresCustomerRepository(db)
	paymentMethodRepo := persistence.NewPostgresPaymentMethodRepository(db)
	invoiceRepo := persistence.NewPostgresInvoiceRepository(db)
	paymentRepo := persistence.NewPostgresPaymentRepository(db)
	ledgerRepo := persistence.NewPostgresLedgerRepository(db)
	outboxRepo := persistence.NewPostgresOutboxRepository(db)
	pspEventRepo := persistence.NewPostgresPspEventRepository(db)
	transactionManager := persistence.NewPostgresTransactionManager(db)

	// PAYMENT_PROVIDER selects which adapter implements PaymentProvider,
	// CustomerVault, and (stripe only) ProviderEventParser — stub keeps the
	// stack and the continuously-running e2e simulator offline and
	// deterministic; stripe is sandbox/test-mode only, enforced by
	// NewStripeProvider refusing any key that isn't sk_test_*. eventParser
	// stays nil for stub: there is no real webhook source to verify, so the
	// webhook route itself is never registered below.
	providerName := getenv("PAYMENT_PROVIDER", domain.ProviderStub)
	var paymentProvider services.PaymentProvider
	var customerVault services.CustomerVault
	var eventParser services.ProviderEventParser
	switch providerName {
	case domain.ProviderStripe:
		stripeTimeout := time.Duration(atoi(getenv("STRIPE_TIMEOUT_SEC", "20"))) * time.Second
		stripeProvider, err := stripe.NewStripeProvider(os.Getenv("STRIPE_SECRET_KEY"), os.Getenv("STRIPE_WEBHOOK_SECRET"), stripeTimeout)
		if err != nil {
			log.Fatal(err)
		}
		paymentProvider = stripeProvider
		customerVault = stripeProvider
		eventParser = stripeProvider
	case domain.ProviderStub:
		stubProvider := stub.NewStubProvider()
		paymentProvider = stubProvider
		customerVault = stubProvider
	default:
		log.Fatalf("unknown PAYMENT_PROVIDER %q (want %q or %q)", providerName, domain.ProviderStub, domain.ProviderStripe)
	}

	metricsClient := metrics.NewOtelMetricsClient(serviceName)

	application := app.Application{
		Commands: app.Commands{
			AddPaymentMethod:      command.NewAddPaymentMethodHandler(customerRepo, paymentMethodRepo, customerVault, transactionManager, providerName, logger, metricsClient),
			RemovePaymentMethod:   command.NewRemovePaymentMethodHandler(paymentMethodRepo, invoiceRepo, transactionManager, logger, metricsClient),
			CreateInvoiceFromRide: command.NewCreateInvoiceFromRideHandler(invoiceRepo, ledgerRepo, transactionManager, commissionBps, logger, metricsClient),
			FinalizeChargeSucceeded: command.NewFinalizeChargeSucceededHandler(
				invoiceRepo, paymentRepo, ledgerRepo, outboxRepo, transactionManager, commissionBps, logger, metricsClient,
			),
			FinalizeChargeFailed: command.NewFinalizeChargeFailedHandler(
				invoiceRepo, paymentRepo, ledgerRepo, outboxRepo, transactionManager, maxAttempts, backoff, logger, metricsClient,
			),
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
	var webhookHandler *handler.WebhookHandler
	if eventParser != nil {
		webhookHandler = handler.NewWebhookHandler(application, eventParser, pspEventRepo, paymentRepo, invoiceRepo, logger)
	}

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
	if webhookHandler != nil {
		// Unauthenticated at Kong (see gateway/kong.yml) — the signature
		// check inside WebhookHandler is the actual auth boundary here.
		mux.HandleFunc("POST /webhooks/stripe", webhookHandler.StripeWebhook)
	}

	server := &http.Server{
		Addr:         ":8005",
		Handler:      middleware.BodyLimit(middleware.RequestID(obshttp.Handler(middleware.Recover(logger)(mux), serviceName))),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

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

	// Outbox worker: publishes billing.outbox_message rows (payment.completed,
	// payment.failed) to Kafka.
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

	// ChargeWorker: sweeps open invoices due for a collection attempt. Its
	// own transactions never hold the provider call — see
	// internal/workers/charge_worker.go for why.
	chargeWorker := workers.NewChargeWorker(
		invoiceRepo, paymentRepo, paymentMethodRepo, customerRepo,
		paymentProvider, transactionManager, application, providerName, logger,
		time.Duration(atoi(getenv("CHARGE_WORKER_INTERVAL_SEC", "5")))*time.Second,
		chargeLease,
	)
	shutdownManager.Add(1)
	goSafe(logger, healthChecker, workerCtx, "charge-worker", func() {
		defer shutdownManager.Done()
		chargeWorker.Run(workerCtx)
	})

	// ride.completed consumer: creates a ride_fare invoice + posts T1.
	rideCompletedConsumer := consumers.NewRideCompletedConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	goSafe(logger, healthChecker, workerCtx, "ride-completed-consumer", func() {
		defer shutdownManager.Done()
		rideCompletedConsumer.Run(workerCtx, "ride.completed")
	})

	// ride.cancelled consumer: creates a cancellation_fee invoice + posts T1
	// only when a nonzero fee was charged.
	rideCancelledConsumer := consumers.NewRideCancelledConsumer(application, kafkaBroker)
	shutdownManager.Add(1)
	goSafe(logger, healthChecker, workerCtx, "ride-cancelled-consumer", func() {
		defer shutdownManager.Done()
		rideCancelledConsumer.Run(workerCtx, "ride.cancelled")
	})

	goSafe(logger, healthChecker, nil, "http-server", func() {
		log.Println("billing-service listening on :8005")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v\n", err)
		}
	})

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
// (an outbox/charge worker, a Kafka consumer) is gone and won't come back,
// and GET /health/live should say so. Pass a nil workerCtx for goroutines
// with no associated cancellation context, like the HTTP server itself,
// which exits cleanly via http.ErrServerClosed on a normal shutdown.
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

func parseDuration(s string, def time.Duration) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

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
