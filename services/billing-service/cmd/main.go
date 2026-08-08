package main

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/application/query"
	"billing-service/internal/application/services"
	"billing-service/internal/consumers"
	"billing-service/internal/domain"
	"billing-service/internal/infrastructure/health"
	"billing-service/internal/infrastructure/metrics"
	"billing-service/internal/infrastructure/payment/stripe"
	"billing-service/internal/infrastructure/payment/stub"
	"billing-service/internal/infrastructure/shutdown"
	"billing-service/internal/interfaces/http/handler"
	"billing-service/internal/persistence"
	"billing-service/internal/workers"
	"context"
	"log"
	"net/http"
	"os"
	"strings"
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

const serviceName = "billing-service"

const defaultPgDsn = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"

func main() {
	// `app healthcheck` backs Docker's HEALTHCHECK (see docker-compose.yml): distroless has no shell/curl for
	// CMD-SHELL, so the binary probes its own /health/ready and exits 0/1.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		health.HealthcheckSelf("http://localhost:8005/health/live")
	}

	// otelinit.Setup reads standard OTEL_* env vars (see docker-compose.yml) and installs the global providers.
	// It never fails boot on a down Collector — OTLP/gRPC exporters dial lazily and retry in the background.
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
	commissionBps := int64(envconfig.Int("PLATFORM_COMMISSION_BPS", 2000))
	maxAttempts := envconfig.Int("MAX_PAYMENT_ATTEMPTS", 3)
	backoff := parseDurations(envconfig.String("PAYMENT_BACKOFF", "1m,5m,30m"))
	chargeLease := parseDuration(envconfig.String("CHARGE_LEASE", "2m"), 2*time.Minute)

	customerRepo := persistence.NewPostgresCustomerRepository(db)
	paymentMethodRepo := persistence.NewPostgresPaymentMethodRepository(db)
	invoiceRepo := persistence.NewPostgresInvoiceRepository(db)
	paymentRepo := persistence.NewPostgresPaymentRepository(db)
	ledgerRepo := persistence.NewPostgresLedgerRepository(db)
	outboxRepo := persistence.NewPostgresOutboxRepository(db)
	pspEventRepo := persistence.NewPostgresPspEventRepository(db)
	transactionManager := persistence.NewPostgresTransactionManager(db)

	// PAYMENT_PROVIDER selects the adapter: stub keeps the stack and e2e simulator offline and deterministic;
	// stripe is sandbox-only (NewStripeProvider rejects non-sk_test_* keys). eventParser stays nil for stub, so the webhook route is never registered below.
	providerName := envconfig.String("PAYMENT_PROVIDER", domain.ProviderStub)
	var paymentProvider services.PaymentProvider
	var customerVault services.CustomerVault
	var eventParser services.ProviderEventParser
	switch providerName {
	case domain.ProviderStripe:
		stripeTimeout := time.Duration(envconfig.Int("STRIPE_TIMEOUT_SEC", 20)) * time.Second
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

	// Outbox worker: publishes billing.outbox_message rows (payment.completed,
	// payment.failed) to Kafka.
	publisher := kafkapublisher.New(kafkaBroker)
	defer publisher.Close()
	outboxWorker := outbox.New(serviceName, outboxRepo, publisher, transactionManager, logger, 2*time.Second)

	metricsClient := metrics.NewOtelMetricsClient(serviceName)

	// Outbox backlog gauges: "pending" will still be retried; "parked" exceeded outboxWorker.MaxRetries() and needs
	// manual triage (see PLAN.md's SQL) — previously this backlog had no signal at all short of running that query by hand.
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

	paymentMethodHandler := handler.NewPaymentMethodHandler(application, logger)
	invoiceHandler := handler.NewInvoiceHandler(application, logger)
	ledgerHandler := handler.NewLedgerHandler(application, logger)
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
		Handler:      httpmw.BodyLimit(httpmw.RequestID(obshttp.Handler(httpmw.Recover(logger)(mux), serviceName))),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// GET /health/ready must stop reporting healthy the moment shutdown begins, not up to checkInterval later —
	// the ticker-based DB-ping check alone wouldn't catch this promptly.
	shutdownManager.OnStop(healthChecker.MarkNotReady)

	// Flush providers only after every worker below has actually drained, not merely told to stop — see
	// shutdown.Manager.OnDrained's doc comment for why OnStop would be too early and drop the drain period's telemetry.
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

	// ChargeWorker: sweeps open invoices due for a collection attempt. Its own transactions never hold the
	// provider call — see internal/workers/charge_worker.go for why.
	chargeWorker := workers.NewChargeWorker(
		invoiceRepo, paymentRepo, paymentMethodRepo, customerRepo,
		paymentProvider, transactionManager, application, providerName, logger,
		time.Duration(envconfig.Int("CHARGE_WORKER_INTERVAL_SEC", 5))*time.Second,
		chargeLease,
	)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, workerCtx, "charge-worker", func() {
		defer shutdownManager.Done()
		chargeWorker.Run(workerCtx)
	})

	// ride.completed consumer: creates a ride_fare invoice + posts T1.
	rideCompletedConsumer := consumers.NewRideCompletedConsumer(application, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, workerCtx, "ride-completed-consumer", func() {
		defer shutdownManager.Done()
		rideCompletedConsumer.Run(workerCtx, "ride.completed")
	})

	// ride.cancelled consumer: creates a cancellation_fee invoice + posts T1
	// only when a nonzero fee was charged.
	rideCancelledConsumer := consumers.NewRideCancelledConsumer(application, kafkaBroker, logger)
	shutdownManager.Add(1)
	health.GoSafe(logger, healthChecker, workerCtx, "ride-cancelled-consumer", func() {
		defer shutdownManager.Done()
		rideCancelledConsumer.Run(workerCtx, "ride.cancelled")
	})

	health.GoSafe(logger, healthChecker, nil, "http-server", func() {
		logger.Info("billing-service listening on :8005")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Error("server error")
		}
	})

	shutdownManager.WaitForShutdown()
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
