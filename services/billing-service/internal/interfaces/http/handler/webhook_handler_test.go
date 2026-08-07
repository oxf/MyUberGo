package handler

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/application/services"
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"billing-service/internal/infrastructure/metrics"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
)

// --- minimal fakes, scoped to this test's needs only ----------------------

type whInvoiceRepo struct {
	mu       sync.Mutex
	invoices map[string]*domain.Invoice
}

func newWhInvoiceRepo(inv *domain.Invoice) *whInvoiceRepo {
	return &whInvoiceRepo{invoices: map[string]*domain.Invoice{inv.ID: inv}}
}
func (r *whInvoiceRepo) Create(ctx context.Context, inv *domain.Invoice) (string, error) {
	panic("not used")
}
func (r *whInvoiceRepo) GetByID(ctx context.Context, id string) (*domain.Invoice, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inv, ok := r.invoices[id]
	if !ok {
		return nil, commonerrors.ErrNotFound
	}
	cp := *inv
	return &cp, nil
}
func (r *whInvoiceRepo) GetByRideID(ctx context.Context, rideID, invoiceType string) (*domain.Invoice, error) {
	panic("not used")
}
func (r *whInvoiceRepo) GetList(ctx context.Context, req domain.PageRequest) ([]*domain.Invoice, error) {
	panic("not used")
}
func (r *whInvoiceRepo) CountInvoices(ctx context.Context) (int, error) { panic("not used") }
func (r *whInvoiceRepo) CountOpenByClientID(ctx context.Context, clientID string) (int, error) {
	panic("not used")
}
func (r *whInvoiceRepo) GetDueForCharge(ctx context.Context, limit int) ([]*domain.Invoice, error) {
	panic("not used")
}
func (r *whInvoiceRepo) MarkPaid(ctx context.Context, id, paidAt string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inv, ok := r.invoices[id]
	if !ok || inv.Status != domain.InvoiceStatusOpen {
		return false, nil
	}
	inv.Status = domain.InvoiceStatusPaid
	inv.PaidAt = &paidAt
	inv.NextAttemptAt = nil
	return true, nil
}
func (r *whInvoiceRepo) SetNextAttemptAt(ctx context.Context, id string, nextAttemptAt *string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	inv, ok := r.invoices[id]
	if !ok {
		return commonerrors.ErrNotFound
	}
	inv.NextAttemptAt = nextAttemptAt
	return nil
}
func (r *whInvoiceRepo) MarkUncollectible(ctx context.Context, id string) (bool, error) {
	panic("not used")
}

type whPaymentRepo struct {
	mu       sync.Mutex
	payments map[string]*domain.Payment
}

func newWhPaymentRepo(p *domain.Payment) *whPaymentRepo {
	return &whPaymentRepo{payments: map[string]*domain.Payment{p.ID: p}}
}
func (r *whPaymentRepo) Create(ctx context.Context, p *domain.Payment) (string, error) {
	panic("not used")
}
func (r *whPaymentRepo) GetNonTerminalByInvoiceID(ctx context.Context, invoiceID string) (*domain.Payment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.payments {
		if p.InvoiceID == invoiceID && (p.Status == domain.PaymentStatusPending || p.Status == domain.PaymentStatusProcessing) {
			cp := *p
			return &cp, nil
		}
	}
	return nil, commonerrors.ErrNotFound
}
func (r *whPaymentRepo) GetByProviderIntentID(ctx context.Context, providerIntentID string) (*domain.Payment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.payments {
		if p.ProviderPaymentIntentID != nil && *p.ProviderPaymentIntentID == providerIntentID {
			cp := *p
			return &cp, nil
		}
	}
	return nil, commonerrors.ErrNotFound
}
func (r *whPaymentRepo) SetClaimedUntil(ctx context.Context, id string, claimedUntil string) error {
	panic("not used")
}
func (r *whPaymentRepo) MarkProcessing(ctx context.Context, id string, providerPaymentIntentID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.payments[id]
	if !ok || p.Status != domain.PaymentStatusPending {
		return false, nil
	}
	p.Status = domain.PaymentStatusProcessing
	p.ProviderPaymentIntentID = &providerPaymentIntentID
	return true, nil
}
func (r *whPaymentRepo) MarkSucceeded(ctx context.Context, id, providerPaymentIntentID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.payments[id]
	if !ok || (p.Status != domain.PaymentStatusPending && p.Status != domain.PaymentStatusProcessing) {
		return false, nil
	}
	p.Status = domain.PaymentStatusSucceeded
	p.ProviderPaymentIntentID = &providerPaymentIntentID
	return true, nil
}
func (r *whPaymentRepo) MarkFailed(ctx context.Context, id, failureCode, failureMessage string) (bool, error) {
	panic("not used")
}

type whLedgerRepo struct {
	mu    sync.Mutex
	count int
}

func (r *whLedgerRepo) PostTransaction(ctx context.Context, txType, refType, refID, currency string, legs []domain.LedgerLeg) error {
	if err := domain.ValidateLegs(currency, legs); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	return nil
}
func (r *whLedgerRepo) GetBalance(ctx context.Context, accountType, ownerID, currency string) (int64, error) {
	panic("not used")
}
func (r *whLedgerRepo) postings() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

type whOutboxRepo struct {
	mu    sync.Mutex
	count int
}

func (r *whOutboxRepo) Insert(ctx context.Context, m *domain.OutboxMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	return nil
}
func (r *whOutboxRepo) GetUnprocessedBatch(ctx context.Context, limit int) ([]*domain.OutboxMessage, error) {
	panic("not used")
}
func (r *whOutboxRepo) MarkProcessed(ctx context.Context, id string) error    { panic("not used") }
func (r *whOutboxRepo) IncrementRetries(ctx context.Context, id string) error { panic("not used") }
func (r *whOutboxRepo) CountByRetries(ctx context.Context, maxRetries int) (int64, int64, error) {
	panic("not used")
}
func (r *whOutboxRepo) messages() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

type whPspEventRepo struct {
	mu     sync.Mutex
	events map[string]*domain.PspEvent
}

func newWhPspEventRepo() *whPspEventRepo {
	return &whPspEventRepo{events: make(map[string]*domain.PspEvent)}
}
func (r *whPspEventRepo) Insert(ctx context.Context, e *domain.PspEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.events[e.ID]; exists {
		return domain.ErrDuplicatePspEvent
	}
	cp := *e
	r.events[e.ID] = &cp
	return nil
}
func (r *whPspEventRepo) GetByID(ctx context.Context, id string) (*domain.PspEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.events[id]
	if !ok {
		return nil, commonerrors.ErrNotFound
	}
	cp := *e
	return &cp, nil
}
func (r *whPspEventRepo) MarkProcessed(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.events[id]
	if !ok {
		return commonerrors.ErrNotFound
	}
	processedAt := "2026-01-01T00:00:00Z"
	e.ProcessedAt = &processedAt
	return nil
}

type whTransactionManager struct{}

func (whTransactionManager) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func testHandlerLogger() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return logrus.NewEntry(l)
}

// TestWebhookHandler_Apply_DuplicateEvent_PostsLedgerOnce is the core idempotency guarantee of the
// psp_event inbox: two deliveries of the same Stripe event id resolve the payment/ledger exactly once.
func TestWebhookHandler_Apply_DuplicateEvent_PostsLedgerOnce(t *testing.T) {
	inv := &domain.Invoice{ID: "inv-1", RideID: "ride-1", ClientID: "client-1", Type: domain.InvoiceTypeRideFare,
		Status: domain.InvoiceStatusOpen, AmountMinor: 1500, Currency: "EUR"}
	payment := &domain.Payment{ID: "payment-1", InvoiceID: "inv-1", AttemptNo: 1, Provider: domain.ProviderStripe,
		AmountMinor: 1500, Currency: "EUR", Status: domain.PaymentStatusProcessing, IdempotencyKey: "invoice:inv-1:attempt:1"}
	intentID := "pi_webhook_test"
	payment.ProviderPaymentIntentID = &intentID

	invoiceRepo := newWhInvoiceRepo(inv)
	paymentRepo := newWhPaymentRepo(payment)
	ledgerRepo := &whLedgerRepo{}
	outboxRepo := &whOutboxRepo{}
	pspEventRepo := newWhPspEventRepo()
	transaction := whTransactionManager{}
	logger := testHandlerLogger()
	metricsClient := metrics.NewNoopMetricsClient()

	application := app.Application{
		Commands: app.Commands{
			FinalizeChargeSucceeded: command.NewFinalizeChargeSucceededHandler(
				invoiceRepo, paymentRepo, ledgerRepo, outboxRepo, transaction, 2000, logger, metricsClient,
			),
			FinalizeChargeFailed: command.NewFinalizeChargeFailedHandler(
				invoiceRepo, paymentRepo, ledgerRepo, outboxRepo, transaction, 3, nil, logger, metricsClient,
			),
		},
	}

	h := NewWebhookHandler(application, nil, pspEventRepo, paymentRepo, invoiceRepo, logger)

	event := services.ProviderEvent{
		EventID: "evt_dup_1", EventType: "payment_intent.succeeded",
		Result: services.ChargeResult{Status: services.ChargeSucceeded, ProviderIntentID: intentID},
	}

	if err := h.apply(context.Background(), event, []byte(`{}`)); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if got := ledgerRepo.postings(); got != 1 {
		t.Fatalf("postings after first apply = %d, want 1", got)
	}
	if got := outboxRepo.messages(); got != 1 {
		t.Fatalf("outbox messages after first apply = %d, want 1", got)
	}

	// Redelivery: same event id, already fully processed.
	if err := h.apply(context.Background(), event, []byte(`{}`)); err != nil {
		t.Fatalf("second (redelivered) apply: %v", err)
	}
	if got := ledgerRepo.postings(); got != 1 {
		t.Fatalf("postings after redelivered apply = %d, want still 1", got)
	}
	if got := outboxRepo.messages(); got != 1 {
		t.Fatalf("outbox messages after redelivered apply = %d, want still 1", got)
	}

	gotInv, _ := invoiceRepo.GetByID(context.Background(), "inv-1")
	if gotInv.Status != domain.InvoiceStatusPaid {
		t.Fatalf("invoice status = %q, want paid", gotInv.Status)
	}
}

// TestWebhookHandler_Apply_InterruptedDelivery_RetriesDispatch covers an event recorded but never
// marked processed (a crash between insert and dispatch): its effect must be retried on redelivery.
func TestWebhookHandler_Apply_InterruptedDelivery_RetriesDispatch(t *testing.T) {
	inv := &domain.Invoice{ID: "inv-2", RideID: "ride-2", ClientID: "client-2", Type: domain.InvoiceTypeRideFare,
		Status: domain.InvoiceStatusOpen, AmountMinor: 1000, Currency: "EUR"}
	payment := &domain.Payment{ID: "payment-2", InvoiceID: "inv-2", AttemptNo: 1, Provider: domain.ProviderStripe,
		AmountMinor: 1000, Currency: "EUR", Status: domain.PaymentStatusProcessing, IdempotencyKey: "invoice:inv-2:attempt:1"}
	intentID := "pi_interrupted"
	payment.ProviderPaymentIntentID = &intentID

	invoiceRepo := newWhInvoiceRepo(inv)
	paymentRepo := newWhPaymentRepo(payment)
	ledgerRepo := &whLedgerRepo{}
	outboxRepo := &whOutboxRepo{}
	pspEventRepo := newWhPspEventRepo()
	logger := testHandlerLogger()

	// Simulate a delivery that got as far as recording the event, but
	// crashed before dispatch/mark-processed.
	if err := pspEventRepo.Insert(context.Background(), &domain.PspEvent{ID: "evt_interrupted_1", Type: "payment_intent.succeeded"}); err != nil {
		t.Fatalf("seed Insert: %v", err)
	}

	application := app.Application{
		Commands: app.Commands{
			FinalizeChargeSucceeded: command.NewFinalizeChargeSucceededHandler(
				invoiceRepo, paymentRepo, ledgerRepo, outboxRepo, whTransactionManager{}, 2000, logger, metrics.NewNoopMetricsClient(),
			),
			FinalizeChargeFailed: command.NewFinalizeChargeFailedHandler(
				invoiceRepo, paymentRepo, ledgerRepo, outboxRepo, whTransactionManager{}, 3, nil, logger, metrics.NewNoopMetricsClient(),
			),
		},
	}
	h := NewWebhookHandler(application, nil, pspEventRepo, paymentRepo, invoiceRepo, logger)

	event := services.ProviderEvent{
		EventID: "evt_interrupted_1", EventType: "payment_intent.succeeded",
		Result: services.ChargeResult{Status: services.ChargeSucceeded, ProviderIntentID: intentID},
	}

	if err := h.apply(context.Background(), event, []byte(`{}`)); err != nil {
		t.Fatalf("apply after interrupted insert: %v", err)
	}
	if got := ledgerRepo.postings(); got != 1 {
		t.Fatalf("postings = %d, want 1 (the interrupted delivery's effect must still apply)", got)
	}

	stored, err := pspEventRepo.GetByID(context.Background(), "evt_interrupted_1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if stored.ProcessedAt == nil {
		t.Fatal("expected ProcessedAt to be set after a successful retry-dispatch")
	}
}
