package workers

import (
	"billing-service/internal/application/services"
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"fmt"
	"sync"
	"time"
)

// --- fakeInvoiceRepo -------------------------------------------------------

type fakeInvoiceRepo struct {
	mu          sync.Mutex
	invoices    map[string]*domain.Invoice
	paymentRepo *fakePaymentRepo // for the derived attempt_count — mirrors invoiceSelectCols' correlated subquery
}

func newFakeInvoiceRepo(paymentRepo *fakePaymentRepo, invoices ...*domain.Invoice) *fakeInvoiceRepo {
	m := make(map[string]*domain.Invoice)
	for _, inv := range invoices {
		cp := *inv
		m[inv.ID] = &cp
	}
	return &fakeInvoiceRepo{invoices: m, paymentRepo: paymentRepo}
}

// withDerivedAttemptCount returns a copy with AttemptCount populated by counting payment rows,
// matching the real invoiceSelectCols subquery so the fake can't silently drift from reality.
func (r *fakeInvoiceRepo) withDerivedAttemptCount(inv *domain.Invoice) *domain.Invoice {
	cp := *inv
	cp.AttemptCount = r.paymentRepo.countByInvoiceID(inv.ID)
	return &cp
}

func (r *fakeInvoiceRepo) get(id string) *domain.Invoice {
	r.mu.Lock()
	inv := r.invoices[id]
	r.mu.Unlock()
	if inv == nil {
		return nil
	}
	return r.withDerivedAttemptCount(inv)
}

func (r *fakeInvoiceRepo) Create(ctx context.Context, inv *domain.Invoice) (string, error) {
	panic("not used by ChargeWorker tests")
}

func (r *fakeInvoiceRepo) GetByID(ctx context.Context, id string) (*domain.Invoice, error) {
	r.mu.Lock()
	inv, ok := r.invoices[id]
	r.mu.Unlock()
	if !ok {
		return nil, commonerrors.ErrNotFound
	}
	return r.withDerivedAttemptCount(inv), nil
}

func (r *fakeInvoiceRepo) GetByRideID(ctx context.Context, rideID, invoiceType string) (*domain.Invoice, error) {
	panic("not used by ChargeWorker tests")
}

func (r *fakeInvoiceRepo) GetList(ctx context.Context, req domain.PageRequest) ([]*domain.Invoice, error) {
	panic("not used by ChargeWorker tests")
}

func (r *fakeInvoiceRepo) CountInvoices(ctx context.Context) (int, error) {
	panic("not used by ChargeWorker tests")
}

func (r *fakeInvoiceRepo) CountOpenByClientID(ctx context.Context, clientID string) (int, error) {
	panic("not used by ChargeWorker tests")
}

// GetDueForCharge mirrors the real query's semantics closely enough to
// exercise scheduling: open, next_attempt_at set and <= now.
func (r *fakeInvoiceRepo) GetDueForCharge(ctx context.Context, limit int) ([]*domain.Invoice, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	var result []*domain.Invoice
	for _, inv := range r.invoices {
		if inv.Status != domain.InvoiceStatusOpen || inv.NextAttemptAt == nil {
			continue
		}
		t, err := time.Parse(time.RFC3339, *inv.NextAttemptAt)
		if err != nil || t.After(now) {
			continue
		}
		result = append(result, r.withDerivedAttemptCount(inv))
		if len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (r *fakeInvoiceRepo) MarkPaid(ctx context.Context, id, paidAt string) (bool, error) {
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

func (r *fakeInvoiceRepo) SetNextAttemptAt(ctx context.Context, id string, nextAttemptAt *string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	inv, ok := r.invoices[id]
	if !ok {
		return commonerrors.ErrNotFound
	}
	inv.NextAttemptAt = nextAttemptAt
	return nil
}

func (r *fakeInvoiceRepo) MarkUncollectible(ctx context.Context, id string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inv, ok := r.invoices[id]
	if !ok || inv.Status != domain.InvoiceStatusOpen {
		return false, nil
	}
	inv.Status = domain.InvoiceStatusUncollectible
	inv.NextAttemptAt = nil
	return true, nil
}

// --- fakePaymentRepo ---------------------------------------------------

type fakePaymentRepo struct {
	mu       sync.Mutex
	payments map[string]*domain.Payment
	nextID   int
}

func newFakePaymentRepo() *fakePaymentRepo {
	return &fakePaymentRepo{payments: make(map[string]*domain.Payment)}
}

func (r *fakePaymentRepo) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.payments)
}

func (r *fakePaymentRepo) countByInvoiceID(invoiceID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, p := range r.payments {
		if p.InvoiceID == invoiceID {
			n++
		}
	}
	return n
}

func (r *fakePaymentRepo) Create(ctx context.Context, p *domain.Payment) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	id := fmt.Sprintf("payment-%d", r.nextID)
	cp := *p
	cp.ID = id
	r.payments[id] = &cp
	return id, nil
}

func (r *fakePaymentRepo) GetNonTerminalByInvoiceID(ctx context.Context, invoiceID string) (*domain.Payment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var best *domain.Payment
	for _, p := range r.payments {
		if p.InvoiceID != invoiceID {
			continue
		}
		if p.Status != domain.PaymentStatusPending && p.Status != domain.PaymentStatusProcessing {
			continue
		}
		if best == nil || p.AttemptNo > best.AttemptNo {
			best = p
		}
	}
	if best == nil {
		return nil, commonerrors.ErrNotFound
	}
	cp := *best
	return &cp, nil
}

func (r *fakePaymentRepo) GetByProviderIntentID(ctx context.Context, providerIntentID string) (*domain.Payment, error) {
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

func (r *fakePaymentRepo) SetClaimedUntil(ctx context.Context, id string, claimedUntil string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.payments[id]
	if !ok {
		return commonerrors.ErrNotFound
	}
	p.ClaimedUntil = &claimedUntil
	return nil
}

func (r *fakePaymentRepo) MarkProcessing(ctx context.Context, id string, providerPaymentIntentID string) (bool, error) {
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

func (r *fakePaymentRepo) MarkSucceeded(ctx context.Context, id string, providerPaymentIntentID string) (bool, error) {
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

func (r *fakePaymentRepo) MarkFailed(ctx context.Context, id, failureCode, failureMessage string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.payments[id]
	if !ok || (p.Status != domain.PaymentStatusPending && p.Status != domain.PaymentStatusProcessing) {
		return false, nil
	}
	p.Status = domain.PaymentStatusFailed
	p.FailureCode = &failureCode
	p.FailureMessage = &failureMessage
	return true, nil
}

// --- fakePaymentMethodRepo ---------------------------------------------

type fakePaymentMethodRepo struct {
	mu              sync.Mutex
	methods         map[string]*domain.PaymentMethod
	byClientDefault map[string]string
}

func newFakePaymentMethodRepo() *fakePaymentMethodRepo {
	return &fakePaymentMethodRepo{
		methods:         make(map[string]*domain.PaymentMethod),
		byClientDefault: make(map[string]string),
	}
}

func (r *fakePaymentMethodRepo) addDefault(pm *domain.PaymentMethod) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *pm
	r.methods[pm.ID] = &cp
	r.byClientDefault[pm.ClientID] = pm.ID
}

func (r *fakePaymentMethodRepo) Create(ctx context.Context, m *domain.PaymentMethod) (string, error) {
	panic("not used by ChargeWorker tests")
}

func (r *fakePaymentMethodRepo) ClearDefault(ctx context.Context, clientID string) error {
	panic("not used by ChargeWorker tests")
}

func (r *fakePaymentMethodRepo) ListByClientID(ctx context.Context, clientID string) ([]*domain.PaymentMethod, error) {
	panic("not used by ChargeWorker tests")
}

func (r *fakePaymentMethodRepo) GetByID(ctx context.Context, id string) (*domain.PaymentMethod, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	pm, ok := r.methods[id]
	if !ok {
		return nil, commonerrors.ErrNotFound
	}
	cp := *pm
	return &cp, nil
}

func (r *fakePaymentMethodRepo) GetDefaultActive(ctx context.Context, clientID string) (*domain.PaymentMethod, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok := r.byClientDefault[clientID]
	if !ok {
		return nil, commonerrors.ErrNotFound
	}
	cp := *r.methods[id]
	return &cp, nil
}

func (r *fakePaymentMethodRepo) GetActiveByProviderID(ctx context.Context, clientID, provider, providerPaymentMethodID string) (*domain.PaymentMethod, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.methods {
		if m.ClientID == clientID && m.Provider == provider && m.ProviderPaymentMethodID == providerPaymentMethodID && m.Status == domain.PaymentMethodStatusActive {
			cp := *m
			return &cp, nil
		}
	}
	return nil, commonerrors.ErrNotFound
}

func (r *fakePaymentMethodRepo) MarkRemoved(ctx context.Context, id string) error {
	panic("not used by ChargeWorker tests")
}

// --- fakeCustomerRepo ---------------------------------------------------

type fakeCustomerRepo struct {
	mu        sync.Mutex
	customers map[string]*domain.Customer
}

func newFakeCustomerRepo() *fakeCustomerRepo {
	return &fakeCustomerRepo{customers: make(map[string]*domain.Customer)}
}

func (r *fakeCustomerRepo) add(c *domain.Customer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *c
	r.customers[c.ClientID+":"+c.Provider] = &cp
}

func (r *fakeCustomerRepo) GetByClientID(ctx context.Context, clientID, provider string) (*domain.Customer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.customers[clientID+":"+provider]
	if !ok {
		return nil, commonerrors.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (r *fakeCustomerRepo) Create(ctx context.Context, c *domain.Customer) (string, error) {
	panic("not used by ChargeWorker tests")
}

// --- fakeLedgerRepo ------------------------------------------------------

type ledgerPosting struct {
	txType string
	legs   []domain.LedgerLeg
}

type fakeLedgerRepo struct {
	mu       sync.Mutex
	postings []ledgerPosting
}

func newFakeLedgerRepo() *fakeLedgerRepo { return &fakeLedgerRepo{} }

func (r *fakeLedgerRepo) PostTransaction(ctx context.Context, txType, refType, refID, currency string, legs []domain.LedgerLeg) error {
	if err := domain.ValidateLegs(currency, legs); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.postings = append(r.postings, ledgerPosting{txType: txType, legs: legs})
	return nil
}

func (r *fakeLedgerRepo) GetBalance(ctx context.Context, accountType, ownerID, currency string) (int64, error) {
	panic("not used by ChargeWorker tests")
}

func (r *fakeLedgerRepo) countByType(txType string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, p := range r.postings {
		if p.txType == txType {
			n++
		}
	}
	return n
}

// --- fakeOutboxRepo ------------------------------------------------------

type fakeOutboxRepo struct {
	mu       sync.Mutex
	messages []*domain.OutboxMessage
}

func newFakeOutboxRepo() *fakeOutboxRepo { return &fakeOutboxRepo{} }

func (r *fakeOutboxRepo) Insert(ctx context.Context, m *domain.OutboxMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, m)
	return nil
}

func (r *fakeOutboxRepo) GetUnprocessedBatch(ctx context.Context, limit int) ([]*domain.OutboxMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []*domain.OutboxMessage
	for _, m := range r.messages {
		if m.Processed {
			continue
		}
		if m.ClaimedUntil != nil {
			claimedUntil, err := time.Parse(time.RFC3339, *m.ClaimedUntil)
			if err == nil && claimedUntil.After(time.Now().UTC()) {
				continue
			}
		}
		result = append(result, m)
		if len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (r *fakeOutboxRepo) SetClaimedUntil(ctx context.Context, id string, claimedUntil string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.messages {
		if m.ID == id {
			m.ClaimedUntil = &claimedUntil
			return nil
		}
	}
	return commonerrors.ErrNotFound
}

func (r *fakeOutboxRepo) MarkProcessed(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.messages {
		if m.ID == id {
			m.Processed = true
			return nil
		}
	}
	return commonerrors.ErrNotFound
}

func (r *fakeOutboxRepo) IncrementRetries(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.messages {
		if m.ID == id {
			m.Retries++
			return nil
		}
	}
	return commonerrors.ErrNotFound
}

func (r *fakeOutboxRepo) CountByRetries(ctx context.Context, maxRetries int) (pending int64, parked int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.messages {
		if m.Processed {
			continue
		}
		if m.Retries < maxRetries {
			pending++
		} else {
			parked++
		}
	}
	return pending, parked, nil
}

func (r *fakeOutboxRepo) countByTopic(topic string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, m := range r.messages {
		if m.Topic == topic {
			n++
		}
	}
	return n
}

// --- fakeTransactionManager ----------------------------------------------

// fakeTransactionManager has no real transactional semantics — the fakes above aren't SQL-backed,
// so there's nothing to roll back; it exists only to satisfy services.TransactionManager.
type fakeTransactionManager struct{}

func (fakeTransactionManager) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

// --- fakeProvider ----------------------------------------------------

// fakeProvider mimics StubProvider's idempotency-key caching (sticky first result per key) so
// tests can assert a resumed attempt reuses the same key instead of charging twice.
type fakeProvider struct {
	mu      sync.Mutex
	calls   []services.ChargeRequest
	results map[string]services.ChargeResult
	next    services.ChargeResult
}

func newFakeProvider(next services.ChargeResult) *fakeProvider {
	return &fakeProvider{results: make(map[string]services.ChargeResult), next: next}
}

func (p *fakeProvider) Charge(ctx context.Context, req services.ChargeRequest) (services.ChargeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, req)
	if r, ok := p.results[req.IdempotencyKey]; ok {
		return r, nil
	}
	p.results[req.IdempotencyKey] = p.next
	return p.next, nil
}

func (p *fakeProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

func (p *fakeProvider) lastCall() services.ChargeRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[len(p.calls)-1]
}
