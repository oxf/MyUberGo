package workers

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/application/services"
	"billing-service/internal/domain"
	"billing-service/internal/infrastructure/metrics"
	"context"
	"io"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

const testCommissionBps = int64(2000)

func testLogger() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return logrus.NewEntry(l)
}

// testHarness wires one ChargeWorker plus its finalize commands against
// fully in-memory fakes — no DB, no network, no Postgres transaction
// semantics (fakeTransactionManager just calls the closure directly).
type testHarness struct {
	invoiceRepo       *fakeInvoiceRepo
	paymentRepo       *fakePaymentRepo
	paymentMethodRepo *fakePaymentMethodRepo
	customerRepo      *fakeCustomerRepo
	ledgerRepo        *fakeLedgerRepo
	outboxRepo        *fakeOutboxRepo
	provider          *fakeProvider
	worker            *ChargeWorker
	application       app.Application
}

func newTestHarness(inv *domain.Invoice, provider *fakeProvider, maxAttempts int, backoff []time.Duration, hasPaymentMethod bool) *testHarness {
	paymentRepo := newFakePaymentRepo()
	invoiceRepo := newFakeInvoiceRepo(paymentRepo, inv)
	paymentMethodRepo := newFakePaymentMethodRepo()
	customerRepo := newFakeCustomerRepo()
	ledgerRepo := newFakeLedgerRepo()
	outboxRepo := newFakeOutboxRepo()
	transaction := fakeTransactionManager{}
	logger := testLogger()
	metricsClient := metrics.NewNoopMetricsClient()

	if hasPaymentMethod {
		paymentMethodRepo.addDefault(&domain.PaymentMethod{
			ID: "pm-1", ClientID: inv.ClientID, ProviderPaymentMethodID: "pm_test_token", Status: domain.PaymentMethodStatusActive,
		})
		customerRepo.add(&domain.Customer{ClientID: inv.ClientID, Provider: domain.ProviderStub, ProviderCustomerID: "cus_test_" + inv.ClientID})
	}

	application := app.Application{
		Commands: app.Commands{
			FinalizeChargeSucceeded: command.NewFinalizeChargeSucceededHandler(
				invoiceRepo, paymentRepo, ledgerRepo, outboxRepo, transaction, testCommissionBps, logger, metricsClient,
			),
			FinalizeChargeFailed: command.NewFinalizeChargeFailedHandler(
				invoiceRepo, paymentRepo, ledgerRepo, outboxRepo, transaction, maxAttempts, backoff, logger, metricsClient,
			),
		},
	}

	worker := NewChargeWorker(
		invoiceRepo, paymentRepo, paymentMethodRepo, customerRepo,
		provider, transaction, application, domain.ProviderStub, logger,
		time.Second, time.Hour, // interval is irrelevant — tests call processBatch directly; a 1h lease is the default "don't re-sweep mid-charge" case
	)

	return &testHarness{
		invoiceRepo: invoiceRepo, paymentRepo: paymentRepo, paymentMethodRepo: paymentMethodRepo,
		customerRepo: customerRepo, ledgerRepo: ledgerRepo, outboxRepo: outboxRepo,
		provider: provider, worker: worker, application: application,
	}
}

func openInvoice(id string) *domain.Invoice {
	past := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	return &domain.Invoice{
		ID: id, RideID: "ride-1", ClientID: "client-1", Type: domain.InvoiceTypeRideFare,
		Status: domain.InvoiceStatusOpen, AmountMinor: 2000, Currency: "EUR",
		NextAttemptAt: &past,
	}
}

func TestChargeWorker_HappyPath_Succeeded(t *testing.T) {
	inv := openInvoice("inv-1")
	driverID := "driver-1"
	inv.DriverID = &driverID
	provider := newFakeProvider(services.ChargeResult{Status: services.ChargeSucceeded, ProviderIntentID: "pi_ok"})
	h := newTestHarness(inv, provider, 3, []time.Duration{time.Minute}, true)

	if err := h.worker.processBatch(context.Background()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	got := h.invoiceRepo.get("inv-1")
	if got.Status != domain.InvoiceStatusPaid {
		t.Fatalf("invoice status = %q, want paid", got.Status)
	}
	if got.PaidAt == nil {
		t.Fatal("expected PaidAt to be set")
	}
	if h.ledgerRepo.countByType(domain.LedgerTxPaymentSucceeded) != 1 {
		t.Fatalf("expected exactly 1 payment_succeeded posting, got %d", h.ledgerRepo.countByType(domain.LedgerTxPaymentSucceeded))
	}
	if h.outboxRepo.countByTopic("payment.completed") != 1 {
		t.Fatalf("expected exactly 1 payment.completed outbox row, got %d", h.outboxRepo.countByTopic("payment.completed"))
	}
	if h.paymentRepo.count() != 1 {
		t.Fatalf("expected exactly 1 payment row, got %d", h.paymentRepo.count())
	}
}

// TestChargeWorker_ResumeAfterCrash_ReusesIdempotencyKey simulates a worker
// that claimed an invoice (created a pending payment row with a lease) and
// then crashed before charging or finalizing. A later tick, once the lease
// has expired, must resume that exact payment — same attempt number, same
// idempotency key — rather than starting a fresh attempt, which is what
// makes a real provider's idempotency guarantee actually prevent a double
// charge.
func TestChargeWorker_ResumeAfterCrash_ReusesIdempotencyKey(t *testing.T) {
	inv := openInvoice("inv-2")
	provider := newFakeProvider(services.ChargeResult{Status: services.ChargeSucceeded, ProviderIntentID: "pi_resumed"})
	h := newTestHarness(inv, provider, 3, []time.Duration{time.Minute}, true)

	const idempotencyKey = "invoice:inv-2:attempt:1"
	expiredLease := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	seededPaymentID, err := h.paymentRepo.Create(context.Background(), &domain.Payment{
		InvoiceID: "inv-2", AttemptNo: 1, Provider: domain.ProviderStub, PaymentMethodID: strPtr("pm-1"),
		AmountMinor: inv.AmountMinor, Currency: inv.Currency, Status: domain.PaymentStatusPending,
		IdempotencyKey: idempotencyKey, ClaimedUntil: &expiredLease,
	})
	if err != nil {
		t.Fatalf("seed payment Create: %v", err)
	}

	if err := h.worker.processBatch(context.Background()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	if h.paymentRepo.count() != 1 {
		t.Fatalf("expected the resumed claim to reuse the existing payment row, not create a new one; got %d rows", h.paymentRepo.count())
	}
	if h.provider.callCount() != 1 {
		t.Fatalf("expected exactly 1 provider call, got %d", h.provider.callCount())
	}
	if got := h.provider.lastCall().IdempotencyKey; got != idempotencyKey {
		t.Fatalf("Charge called with IdempotencyKey %q, want %q (the resumed key)", got, idempotencyKey)
	}

	gotInv := h.invoiceRepo.get("inv-2")
	if gotInv.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1 (resume must not double-count)", gotInv.AttemptCount)
	}
	if gotInv.Status != domain.InvoiceStatusPaid {
		t.Fatalf("invoice status = %q, want paid", gotInv.Status)
	}

	gotPayment, err := h.paymentRepo.GetNonTerminalByInvoiceID(context.Background(), "inv-2")
	if err == nil {
		t.Fatalf("expected the resumed payment to be terminal (succeeded), still found non-terminal: %+v", gotPayment)
	}
	_ = seededPaymentID
}

// TestChargeWorker_StillLeased_SkipsWithoutCharging covers the other half
// of the lease: an in-flight attempt whose claimed_until is still in the
// future must be left alone — no reclaim, no provider call, no change to
// either row. This is what makes it safe for GetDueForCharge to keep
// returning an invoice on every tick while a charge is genuinely in flight.
func TestChargeWorker_StillLeased_SkipsWithoutCharging(t *testing.T) {
	inv := openInvoice("inv-leased")
	provider := newFakeProvider(services.ChargeResult{Status: services.ChargeSucceeded, ProviderIntentID: "pi_should_not_happen"})
	h := newTestHarness(inv, provider, 3, []time.Duration{time.Minute}, true)

	futureLease := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	if _, err := h.paymentRepo.Create(context.Background(), &domain.Payment{
		InvoiceID: "inv-leased", AttemptNo: 1, Provider: domain.ProviderStub, PaymentMethodID: strPtr("pm-1"),
		AmountMinor: inv.AmountMinor, Currency: inv.Currency, Status: domain.PaymentStatusPending,
		IdempotencyKey: "invoice:inv-leased:attempt:1", ClaimedUntil: &futureLease,
	}); err != nil {
		t.Fatalf("seed payment Create: %v", err)
	}

	if err := h.worker.processBatch(context.Background()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	if provider.callCount() != 0 {
		t.Fatalf("expected no provider call while still leased, got %d", provider.callCount())
	}
	if h.paymentRepo.count() != 1 {
		t.Fatalf("expected still exactly 1 payment row, got %d", h.paymentRepo.count())
	}
	got := h.invoiceRepo.get("inv-leased")
	if got.Status != domain.InvoiceStatusOpen {
		t.Fatalf("invoice status = %q, want still open (untouched while leased)", got.Status)
	}
}

// TestChargeWorker_ExhaustedAttempts_MarksUncollectible drives a
// consistently-declining client through maxAttempts, asserting the invoice
// ends uncollectible with a T3 posting and driver_payable untouched (it was
// posted at invoice-creation time, not here).
func TestChargeWorker_ExhaustedAttempts_MarksUncollectible(t *testing.T) {
	inv := openInvoice("inv-3")
	provider := newFakeProvider(services.ChargeResult{Status: services.ChargeFailed, FailureCode: "card_declined", FailureMessage: "declined"})
	const maxAttempts = 2
	// Negative backoff means the next attempt is immediately due, so the
	// test can drive multiple retries without a real sleep.
	h := newTestHarness(inv, provider, maxAttempts, []time.Duration{-time.Hour}, true)

	ctx := context.Background()
	for i := 0; i < maxAttempts; i++ {
		if err := h.worker.processBatch(ctx); err != nil {
			t.Fatalf("processBatch iteration %d: %v", i, err)
		}
	}

	got := h.invoiceRepo.get("inv-3")
	if got.Status != domain.InvoiceStatusUncollectible {
		t.Fatalf("invoice status = %q, want uncollectible after %d attempts", got.Status, maxAttempts)
	}
	if got.AttemptCount != maxAttempts {
		t.Fatalf("attempt_count = %d, want %d", got.AttemptCount, maxAttempts)
	}
	if h.ledgerRepo.countByType(domain.LedgerTxInvoiceUncollectible) != 1 {
		t.Fatalf("expected exactly 1 invoice_uncollectible posting, got %d", h.ledgerRepo.countByType(domain.LedgerTxInvoiceUncollectible))
	}
	if h.outboxRepo.countByTopic("payment.failed") != 1 {
		t.Fatalf("expected exactly 1 payment.failed outbox row, got %d", h.outboxRepo.countByTopic("payment.failed"))
	}
	if h.paymentRepo.count() != maxAttempts {
		t.Fatalf("expected %d payment attempt rows, got %d", maxAttempts, h.paymentRepo.count())
	}
}

// TestChargeWorker_NoPaymentMethod_FailsImmediatelyNoProviderCall covers
// the pre-charge branch: a client with no default active payment method
// never reaches the provider at all, but still counts as a normal failed
// attempt toward the retry budget.
func TestChargeWorker_NoPaymentMethod_FailsImmediatelyNoProviderCall(t *testing.T) {
	inv := openInvoice("inv-4")
	inv.ClientID = "client-without-pm"
	provider := newFakeProvider(services.ChargeResult{Status: services.ChargeSucceeded})
	h := newTestHarness(inv, provider, 3, []time.Duration{time.Minute}, false)

	if err := h.worker.processBatch(context.Background()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	if provider.callCount() != 0 {
		t.Fatalf("expected no provider call for a client with no payment method, got %d", provider.callCount())
	}
	got := h.invoiceRepo.get("inv-4")
	if got.Status != domain.InvoiceStatusOpen {
		t.Fatalf("invoice status = %q, want still open (1 of 3 attempts)", got.Status)
	}
	if got.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1", got.AttemptCount)
	}
}

// TestChargeWorker_NoPaymentMethod_DoesNotRetryEveryTick is the regression
// test for the bug where the no-payment-method payment row was created
// already terminal (Failed): FinalizeChargeFailed's guarded MarkFailed would
// then always no-op ("payment already resolved, skipping"), so
// SetNextAttemptAt never ran and the same invoice was re-claimed and
// re-failed on every single tick forever. With the fix (row created Pending,
// so MarkFailed's guard actually fires), a second tick before the backoff
// window elapses must leave the invoice untouched — same as any other
// declined attempt.
func TestChargeWorker_NoPaymentMethod_DoesNotRetryEveryTick(t *testing.T) {
	inv := openInvoice("inv-6")
	inv.ClientID = "client-without-pm"
	provider := newFakeProvider(services.ChargeResult{Status: services.ChargeSucceeded})
	h := newTestHarness(inv, provider, 3, []time.Duration{time.Minute}, false)

	ctx := context.Background()
	if err := h.worker.processBatch(ctx); err != nil {
		t.Fatalf("processBatch (attempt 1): %v", err)
	}
	if h.paymentRepo.count() != 1 {
		t.Fatalf("after attempt 1: expected 1 payment row, got %d", h.paymentRepo.count())
	}

	// Immediately sweep again, as ChargeWorker's ticker would 5s later. Before
	// the fix this created a second payment row and logged the same
	// "already resolved, skipping" no-op; after the fix, next_attempt_at was
	// pushed a minute out, so GetDueForCharge must not re-select this invoice.
	if err := h.worker.processBatch(ctx); err != nil {
		t.Fatalf("processBatch (attempt 2): %v", err)
	}

	if h.paymentRepo.count() != 1 {
		t.Fatalf("expected still exactly 1 payment row (no same-tick re-attempt), got %d", h.paymentRepo.count())
	}
	got := h.invoiceRepo.get("inv-6")
	if got.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1 (must not grow every tick)", got.AttemptCount)
	}
	if got.Status != domain.InvoiceStatusOpen {
		t.Fatalf("invoice status = %q, want still open (1 of 3 attempts, backing off)", got.Status)
	}
	if got.NextAttemptAt == nil {
		t.Fatal("expected next_attempt_at to be scheduled into the future, got nil")
	}
	if provider.callCount() != 0 {
		t.Fatalf("expected no provider call for a client with no payment method, got %d", provider.callCount())
	}
}

// TestChargeWorker_NoPaymentMethod_ExhaustedAttempts_MarksUncollectible
// mirrors TestChargeWorker_ExhaustedAttempts_MarksUncollectible but drives
// the no-payment-method branch instead of a provider decline — confirming
// FinalizeChargeFailed's backoff/uncollectible/ledger/outbox effects, which
// the pre-fix bug skipped entirely, now actually run for this path too.
func TestChargeWorker_NoPaymentMethod_ExhaustedAttempts_MarksUncollectible(t *testing.T) {
	inv := openInvoice("inv-7")
	inv.ClientID = "client-without-pm"
	provider := newFakeProvider(services.ChargeResult{Status: services.ChargeSucceeded})
	const maxAttempts = 2
	// Negative backoff means the next attempt is immediately due, so the
	// test can drive multiple retries without a real sleep.
	h := newTestHarness(inv, provider, maxAttempts, []time.Duration{-time.Hour}, false)

	ctx := context.Background()
	for i := 0; i < maxAttempts; i++ {
		if err := h.worker.processBatch(ctx); err != nil {
			t.Fatalf("processBatch iteration %d: %v", i, err)
		}
	}

	got := h.invoiceRepo.get("inv-7")
	if got.Status != domain.InvoiceStatusUncollectible {
		t.Fatalf("invoice status = %q, want uncollectible after %d attempts", got.Status, maxAttempts)
	}
	if got.AttemptCount != maxAttempts {
		t.Fatalf("attempt_count = %d, want %d", got.AttemptCount, maxAttempts)
	}
	if h.ledgerRepo.countByType(domain.LedgerTxInvoiceUncollectible) != 1 {
		t.Fatalf("expected exactly 1 invoice_uncollectible posting, got %d", h.ledgerRepo.countByType(domain.LedgerTxInvoiceUncollectible))
	}
	if h.outboxRepo.countByTopic("payment.failed") != 1 {
		t.Fatalf("expected exactly 1 payment.failed outbox row, got %d", h.outboxRepo.countByTopic("payment.failed"))
	}
	if provider.callCount() != 0 {
		t.Fatalf("expected no provider call for a client with no payment method, got %d", provider.callCount())
	}
}

// TestFinalizeChargeSucceeded_DuplicateCall_PostsLedgerOnce exercises the
// guard directly: two resolutions of the same payment (e.g. ChargeWorker
// and a later webhook both trying to finalize) must only post T2 once.
func TestFinalizeChargeSucceeded_DuplicateCall_PostsLedgerOnce(t *testing.T) {
	inv := openInvoice("inv-5")
	provider := newFakeProvider(services.ChargeResult{Status: services.ChargeSucceeded, ProviderIntentID: "pi_dup"})
	h := newTestHarness(inv, provider, 3, []time.Duration{time.Minute}, true)

	if err := h.worker.processBatch(context.Background()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if h.ledgerRepo.countByType(domain.LedgerTxPaymentSucceeded) != 1 {
		t.Fatalf("expected 1 posting after first finalize, got %d", h.ledgerRepo.countByType(domain.LedgerTxPaymentSucceeded))
	}

	nonTerminal, err := h.paymentRepo.GetNonTerminalByInvoiceID(context.Background(), "inv-5")
	if err == nil {
		t.Fatalf("expected the payment to already be terminal, found non-terminal: %+v", nonTerminal)
	}

	var paymentID string
	for id, p := range h.paymentRepo.payments {
		if p.InvoiceID == "inv-5" {
			paymentID = id
		}
	}
	if paymentID == "" {
		t.Fatal("could not find the finalized payment row")
	}

	err = h.application.Commands.FinalizeChargeSucceeded.Handle(context.Background(), command.FinalizeChargeSucceeded{
		PaymentID: paymentID, InvoiceID: "inv-5", ProviderIntentID: "pi_dup_second",
	})
	if err != nil {
		t.Fatalf("second finalize call returned an error (should be a no-op): %v", err)
	}

	if h.ledgerRepo.countByType(domain.LedgerTxPaymentSucceeded) != 1 {
		t.Fatalf("expected still exactly 1 payment_succeeded posting after duplicate finalize, got %d", h.ledgerRepo.countByType(domain.LedgerTxPaymentSucceeded))
	}
	if h.outboxRepo.countByTopic("payment.completed") != 1 {
		t.Fatalf("expected still exactly 1 payment.completed outbox row after duplicate finalize, got %d", h.outboxRepo.countByTopic("payment.completed"))
	}
}

func strPtr(s string) *string { return &s }
