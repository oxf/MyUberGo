package persistence

import (
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lib/pq"
)

func newInvoiceForPayments(t *testing.T) string {
	t.Helper()
	invRepo := NewPostgresInvoiceRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "")
	id, err := invRepo.Create(context.Background(), newOpenInvoice(rideID, clientID))
	if err != nil {
		t.Fatalf("create invoice fixture: %v", err)
	}
	return id
}

func newPayment(invoiceID string, attemptNo int, status string) *domain.Payment {
	return &domain.Payment{
		InvoiceID:      invoiceID,
		AttemptNo:      attemptNo,
		Provider:       domain.ProviderStub,
		AmountMinor:    1000,
		Currency:       "EUR",
		Status:         status,
		IdempotencyKey: fmt.Sprintf("invoice:%s:attempt:%d", invoiceID, attemptNo),
	}
}

func paymentStatus(t *testing.T, id string) string {
	t.Helper()
	var status string
	if err := testDB.QueryRow(`SELECT status FROM billing.payment WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("read payment status: %v", err)
	}
	return status
}

func TestMarkProcessing_GuardOnlyFromPending(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentRepository(testDB)
	invoiceID := newInvoiceForPayments(t)

	id, err := repo.Create(ctx, newPayment(invoiceID, 1, domain.PaymentStatusPending))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	won, err := repo.MarkProcessing(ctx, id, "pi_123")
	if err != nil {
		t.Fatalf("first MarkProcessing: %v", err)
	}
	if !won {
		t.Fatal("expected first MarkProcessing on a pending payment to win the guard")
	}
	if got := paymentStatus(t, id); got != domain.PaymentStatusProcessing {
		t.Fatalf("expected status=processing, got %s", got)
	}

	won, err = repo.MarkProcessing(ctx, id, "pi_456")
	if err != nil {
		t.Fatalf("second MarkProcessing: %v", err)
	}
	if won {
		t.Fatal("expected the guard to reject a second MarkProcessing on an already-processing payment")
	}
}

func TestMarkSucceeded_GuardFromPendingOrProcessing(t *testing.T) {
	cases := []struct {
		name        string
		startStatus string
		wantWon     bool
	}{
		{"fromPending", domain.PaymentStatusPending, true},
		{"fromProcessing", domain.PaymentStatusProcessing, true},
		{"fromSucceeded", domain.PaymentStatusSucceeded, false},
		{"fromFailed", domain.PaymentStatusFailed, false},
	}

	ctx := context.Background()
	repo := NewPostgresPaymentRepository(testDB)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			invoiceID := newInvoiceForPayments(t)
			id, err := repo.Create(ctx, newPayment(invoiceID, 1, tc.startStatus))
			if err != nil {
				t.Fatalf("Create: %v", err)
			}

			// A distinct provider intent id per subtest: idx_payment_provider_intent
			// is a real unique index, so reusing one literal across subtests
			// would collide regardless of which row the guard actually targets.
			intentID := fmt.Sprintf("pi_test_%d", nextSeq())
			won, err := repo.MarkSucceeded(ctx, id, intentID)
			if err != nil {
				t.Fatalf("MarkSucceeded: %v", err)
			}
			if won != tc.wantWon {
				t.Fatalf("expected won=%v from status=%s, got %v", tc.wantWon, tc.startStatus, won)
			}

			wantStatus := tc.startStatus
			if tc.wantWon {
				wantStatus = domain.PaymentStatusSucceeded
			}
			if got := paymentStatus(t, id); got != wantStatus {
				t.Fatalf("expected status=%s, got %s", wantStatus, got)
			}
		})
	}
}

func TestMarkSucceeded_ThenMarkFailed_SecondIsNoop(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentRepository(testDB)
	invoiceID := newInvoiceForPayments(t)

	id, err := repo.Create(ctx, newPayment(invoiceID, 1, domain.PaymentStatusPending))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if won, err := repo.MarkSucceeded(ctx, id, "pi_race"); err != nil || !won {
		t.Fatalf("MarkSucceeded should win on a pending payment: won=%v err=%v", won, err)
	}

	won, err := repo.MarkFailed(ctx, id, "card_declined", "the card was declined")
	if err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	if won {
		t.Fatal("expected MarkFailed to lose the guard against an already-succeeded payment")
	}
	if got := paymentStatus(t, id); got != domain.PaymentStatusSucceeded {
		t.Fatalf("expected status to remain succeeded, got %s", got)
	}
}

func TestGetNonTerminalByInvoiceID_OrderedDescExcludesTerminal(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentRepository(testDB)
	invoiceID := newInvoiceForPayments(t)

	if _, err := repo.Create(ctx, newPayment(invoiceID, 1, domain.PaymentStatusFailed)); err != nil {
		t.Fatalf("Create attempt 1: %v", err)
	}
	attempt2ID, err := repo.Create(ctx, newPayment(invoiceID, 2, domain.PaymentStatusPending))
	if err != nil {
		t.Fatalf("Create attempt 2: %v", err)
	}

	got, err := repo.GetNonTerminalByInvoiceID(ctx, invoiceID)
	if err != nil {
		t.Fatalf("GetNonTerminalByInvoiceID: %v", err)
	}
	if got.ID != attempt2ID {
		t.Fatalf("expected the non-terminal attempt 2 (id=%s), got id=%s attemptNo=%d", attempt2ID, got.ID, got.AttemptNo)
	}

	if won, err := repo.MarkSucceeded(ctx, attempt2ID, "pi_final"); err != nil || !won {
		t.Fatalf("MarkSucceeded on attempt 2: won=%v err=%v", won, err)
	}

	if _, err := repo.GetNonTerminalByInvoiceID(ctx, invoiceID); !errors.Is(err, commonerrors.ErrNotFound) {
		t.Fatalf("expected ErrNotFound once every attempt is terminal, got %v", err)
	}
}

// TestCreate_UniqueIdempotencyKeyEnforced documents current behavior rather
// than asserting a translated domain error: unlike every other
// unique-violation in this service (invoice, customer, payment_method,
// psp_event), billing.payment's UNIQUE(idempotency_key) has no
// isUniqueViolation translation today. This is a real (if currently
// harmless, since callers always derive a fresh deterministic key)
// asymmetry — flagged here rather than silently assumed away.
func TestCreate_UniqueIdempotencyKeyEnforced(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentRepository(testDB)
	invoice1 := newInvoiceForPayments(t)
	invoice2 := newInvoiceForPayments(t)

	sharedKey := "invoice:shared:attempt:1"

	first := newPayment(invoice1, 1, domain.PaymentStatusPending)
	first.IdempotencyKey = sharedKey
	if _, err := repo.Create(ctx, first); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	second := newPayment(invoice2, 1, domain.PaymentStatusPending)
	second.IdempotencyKey = sharedKey
	_, err := repo.Create(ctx, second)
	if err == nil {
		t.Fatal("expected a unique-violation error on a duplicate idempotency_key")
	}
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) || pqErr.Code != "23505" {
		t.Fatalf("expected a raw *pq.Error with code 23505 (untranslated, unlike other unique-violations in this service), got %v", err)
	}
}

func TestGetByProviderIntentID(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentRepository(testDB)
	invoiceID := newInvoiceForPayments(t)

	id, err := repo.Create(ctx, newPayment(invoiceID, 1, domain.PaymentStatusPending))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if won, err := repo.MarkProcessing(ctx, id, "pi_lookup"); err != nil || !won {
		t.Fatalf("MarkProcessing: won=%v err=%v", won, err)
	}

	got, err := repo.GetByProviderIntentID(ctx, "pi_lookup")
	if err != nil {
		t.Fatalf("GetByProviderIntentID: %v", err)
	}
	if got.ID != id {
		t.Fatalf("expected id %s, got %s", id, got.ID)
	}

	if _, err := repo.GetByProviderIntentID(ctx, "pi_does_not_exist"); !errors.Is(err, commonerrors.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestSetClaimedUntilRoundTrip(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPaymentRepository(testDB)
	invoiceID := newInvoiceForPayments(t)

	id, err := repo.Create(ctx, newPayment(invoiceID, 1, domain.PaymentStatusPending))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	var claimedUntil string
	if err := testDB.QueryRow(`SELECT (NOW() + interval '30 seconds')::text`).Scan(&claimedUntil); err != nil {
		t.Fatalf("compute claimed_until fixture: %v", err)
	}

	if err := repo.SetClaimedUntil(ctx, id, claimedUntil); err != nil {
		t.Fatalf("SetClaimedUntil: %v", err)
	}

	got, err := repo.GetNonTerminalByInvoiceID(ctx, invoiceID)
	if err != nil {
		t.Fatalf("GetNonTerminalByInvoiceID: %v", err)
	}
	if got.ClaimedUntil == nil {
		t.Fatal("expected ClaimedUntil to be set")
	}
}
