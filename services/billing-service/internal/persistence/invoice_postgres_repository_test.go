package persistence

import (
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func newOpenInvoice(rideID, clientID string) *domain.Invoice {
	return &domain.Invoice{
		RideID:      rideID,
		ClientID:    clientID,
		Type:        domain.InvoiceTypeRideFare,
		Status:      domain.InvoiceStatusOpen,
		AmountMinor: 1000,
		Currency:    "EUR",
	}
}

func TestCreate_PopulatesDriverIDWhenPresent(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresInvoiceRepository(testDB)
	clientID := seedClient(t, testDB)
	driverID := seedDriver(t, testDB)
	rideID := seedRide(t, testDB, clientID, driverID)

	inv := newOpenInvoice(rideID, clientID)
	inv.DriverID = &driverID
	id, err := repo.Create(ctx, inv)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.DriverID == nil || *got.DriverID != driverID {
		t.Fatalf("expected DriverID=%s, got %v", driverID, got.DriverID)
	}
}

func TestCreate_DuplicateRideAndType_ReturnsErrDuplicateInvoice(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresInvoiceRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "")

	if _, err := repo.Create(ctx, newOpenInvoice(rideID, clientID)); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	_, err := repo.Create(ctx, newOpenInvoice(rideID, clientID))
	if !errors.Is(err, domain.ErrDuplicateInvoice) {
		t.Fatalf("expected ErrDuplicateInvoice for a redelivered (ride_id, type), got %v", err)
	}

	// A different type for the same ride is a different composite key and
	// must succeed.
	feeInvoice := newOpenInvoice(rideID, clientID)
	feeInvoice.Type = domain.InvoiceTypeCancellationFee
	if _, err := repo.Create(ctx, feeInvoice); err != nil {
		t.Fatalf("Create with a different type should succeed, got %v", err)
	}
}

func TestMarkPaid_GuardOnlyFromOpen(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresInvoiceRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "")

	id, err := repo.Create(ctx, newOpenInvoice(rideID, clientID))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	firstPaidAt := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
	won, err := repo.MarkPaid(ctx, id, firstPaidAt)
	if err != nil {
		t.Fatalf("first MarkPaid: %v", err)
	}
	if !won {
		t.Fatal("expected first MarkPaid on an open invoice to win the guard")
	}

	inv, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if inv.Status != domain.InvoiceStatusPaid || inv.PaidAt == nil || *inv.PaidAt != firstPaidAt {
		t.Fatalf("expected status=paid paidAt=%s, got status=%s paidAt=%v", firstPaidAt, inv.Status, inv.PaidAt)
	}
	if inv.NextAttemptAt != nil {
		t.Fatalf("expected next_attempt_at cleared on MarkPaid, got %v", *inv.NextAttemptAt)
	}

	// A redelivered payment.completed must be a no-op, not an error and not
	// a second write.
	secondPaidAt := time.Now().Add(time.Hour).UTC().Truncate(time.Second).Format(time.RFC3339)
	won, err = repo.MarkPaid(ctx, id, secondPaidAt)
	if err != nil {
		t.Fatalf("second (redelivered) MarkPaid: %v", err)
	}
	if won {
		t.Fatal("expected the guard to reject a redelivered MarkPaid on an already-paid invoice")
	}

	inv, err = repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID after redelivery: %v", err)
	}
	if inv.PaidAt == nil || *inv.PaidAt != firstPaidAt {
		t.Fatalf("guard failed: redelivery changed paid_at to %v (want unchanged %s)", inv.PaidAt, firstPaidAt)
	}
}

func TestMarkUncollectible_GuardOnlyFromOpen(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresInvoiceRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "")

	id, err := repo.Create(ctx, newOpenInvoice(rideID, clientID))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	won, err := repo.MarkUncollectible(ctx, id)
	if err != nil {
		t.Fatalf("first MarkUncollectible: %v", err)
	}
	if !won {
		t.Fatal("expected first MarkUncollectible on an open invoice to win the guard")
	}

	won, err = repo.MarkUncollectible(ctx, id)
	if err != nil {
		t.Fatalf("second MarkUncollectible: %v", err)
	}
	if won {
		t.Fatal("expected the guard to reject a second MarkUncollectible on an already-uncollectible invoice")
	}
}

// TestMarkPaid_MarkUncollectible_CrossGuardRace is the webhook-vs-ChargeWorker
// race the guard exists for: once an invoice is paid, a later
// MarkUncollectible call (e.g. from a worker that hadn't yet seen the
// payment) must not win.
func TestMarkPaid_MarkUncollectible_CrossGuardRace(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresInvoiceRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "")

	id, err := repo.Create(ctx, newOpenInvoice(rideID, clientID))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	paidAt := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
	if won, err := repo.MarkPaid(ctx, id, paidAt); err != nil || !won {
		t.Fatalf("MarkPaid should win on an open invoice: won=%v err=%v", won, err)
	}

	won, err := repo.MarkUncollectible(ctx, id)
	if err != nil {
		t.Fatalf("MarkUncollectible: %v", err)
	}
	if won {
		t.Fatal("expected MarkUncollectible to lose the guard against an already-paid invoice")
	}

	inv, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if inv.Status != domain.InvoiceStatusPaid {
		t.Fatalf("expected status to remain paid, got %s", inv.Status)
	}
}

// TestGetDueForCharge_SkipLockedNoDoubleClaim exercises the real
// "FOR UPDATE OF i SKIP LOCKED" guarantee under two concurrent claims — no
// fake/mock repository can verify this.
func TestGetDueForCharge_SkipLockedNoDoubleClaim(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresInvoiceRepository(testDB)
	clientID := seedClient(t, testDB)

	past := time.Now().Add(-time.Hour).UTC().Truncate(time.Second).Format(time.RFC3339)

	for range 6 {
		rideID := seedRide(t, testDB, clientID, "")
		inv := newOpenInvoice(rideID, clientID)
		inv.NextAttemptAt = &past
		if _, err := repo.Create(ctx, inv); err != nil {
			t.Fatalf("Create due invoice: %v", err)
		}
	}

	tx1, err := testDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer func() { _ = tx1.Rollback() }()

	batch1, err := repo.GetDueForCharge(WithTx(ctx, tx1), 3)
	if err != nil {
		t.Fatalf("tx1 GetDueForCharge: %v", err)
	}
	if len(batch1) != 3 {
		t.Fatalf("expected 3 invoices in batch1, got %d", len(batch1))
	}

	tx2, err := testDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer func() { _ = tx2.Rollback() }()

	batch2, err := repo.GetDueForCharge(WithTx(ctx, tx2), 3)
	if err != nil {
		t.Fatalf("tx2 GetDueForCharge: %v", err)
	}
	if len(batch2) != 3 {
		t.Fatalf("expected 3 invoices in batch2 (skipping tx1's locked rows), got %d", len(batch2))
	}

	seen := map[string]bool{}
	for _, inv := range batch1 {
		seen[inv.ID] = true
	}
	for _, inv := range batch2 {
		if seen[inv.ID] {
			t.Fatalf("batch2 claimed invoice %s already claimed by batch1 — SKIP LOCKED failed to exclude it", inv.ID)
		}
	}
}

func TestGetDueForCharge_ExcludesFutureAndNonOpen(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresInvoiceRepository(testDB)
	clientID := seedClient(t, testDB)

	past := time.Now().Add(-time.Hour).UTC().Truncate(time.Second).Format(time.RFC3339)
	future := time.Now().Add(time.Hour).UTC().Truncate(time.Second).Format(time.RFC3339)

	dueRideID := seedRide(t, testDB, clientID, "")
	dueInvoice := newOpenInvoice(dueRideID, clientID)
	dueInvoice.NextAttemptAt = &past
	dueID, err := repo.Create(ctx, dueInvoice)
	if err != nil {
		t.Fatalf("Create due invoice: %v", err)
	}

	futureRideID := seedRide(t, testDB, clientID, "")
	futureInvoice := newOpenInvoice(futureRideID, clientID)
	futureInvoice.NextAttemptAt = &future
	futureID, err := repo.Create(ctx, futureInvoice)
	if err != nil {
		t.Fatalf("Create future invoice: %v", err)
	}

	paidRideID := seedRide(t, testDB, clientID, "")
	paidInvoice := newOpenInvoice(paidRideID, clientID)
	paidInvoice.NextAttemptAt = &past
	paidID, err := repo.Create(ctx, paidInvoice)
	if err != nil {
		t.Fatalf("Create soon-to-be-paid invoice: %v", err)
	}
	if won, err := repo.MarkPaid(ctx, paidID, past); err != nil || !won {
		t.Fatalf("MarkPaid on paidInvoice: won=%v err=%v", won, err)
	}

	batch, err := repo.GetDueForCharge(ctx, 50)
	if err != nil {
		t.Fatalf("GetDueForCharge: %v", err)
	}

	seen := map[string]bool{}
	for _, inv := range batch {
		seen[inv.ID] = true
	}
	if !seen[dueID] {
		t.Fatalf("expected due invoice %s to be included", dueID)
	}
	if seen[futureID] {
		t.Fatalf("expected future-dated invoice %s to be excluded", futureID)
	}
	if seen[paidID] {
		t.Fatalf("expected already-paid invoice %s to be excluded", paidID)
	}
}

// TestAttemptCount_DerivedSubqueryMatchesPaymentRowCount seeds
// billing.payment rows directly and confirms the correlated subquery in
// invoiceSelectCols derives the right count — a computed expression a
// fake/mock repository would have to hand-reimplement rather than exercise.
func TestAttemptCount_DerivedSubqueryMatchesPaymentRowCount(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresInvoiceRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "")

	id, err := repo.Create(ctx, newOpenInvoice(rideID, clientID))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if got, err := repo.GetByID(ctx, id); err != nil {
		t.Fatalf("GetByID before any payment: %v", err)
	} else if got.AttemptCount != 0 {
		t.Fatalf("expected AttemptCount=0 before any payment row, got %d", got.AttemptCount)
	}

	const paymentCount = 3
	for i := 1; i <= paymentCount; i++ {
		idempotencyKey := fmt.Sprintf("invoice:%s:attempt:%d", id, i)
		if _, err := testDB.ExecContext(ctx, `
			INSERT INTO billing.payment (invoice_id, attempt_no, provider, amount_minor, currency, status, idempotency_key)
			VALUES ($1, $2, 'stub', 1000, 'EUR', 'failed', $3)
		`, id, i, idempotencyKey); err != nil {
			t.Fatalf("seed billing.payment attempt %d: %v", i, err)
		}
	}

	got, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID after seeding payments: %v", err)
	}
	if got.AttemptCount != paymentCount {
		t.Fatalf("expected AttemptCount=%d, got %d", paymentCount, got.AttemptCount)
	}
}

func TestSetNextAttemptAt_RoundTripAndClear(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresInvoiceRepository(testDB)
	clientID := seedClient(t, testDB)
	rideID := seedRide(t, testDB, clientID, "")

	id, err := repo.Create(ctx, newOpenInvoice(rideID, clientID))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	when := time.Now().Add(30 * time.Minute).UTC().Truncate(time.Second).Format(time.RFC3339)
	if err := repo.SetNextAttemptAt(ctx, id, &when); err != nil {
		t.Fatalf("SetNextAttemptAt(set): %v", err)
	}
	got, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.NextAttemptAt == nil || *got.NextAttemptAt != when {
		t.Fatalf("expected next_attempt_at=%s, got %v", when, got.NextAttemptAt)
	}

	if err := repo.SetNextAttemptAt(ctx, id, nil); err != nil {
		t.Fatalf("SetNextAttemptAt(clear): %v", err)
	}
	got, err = repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID after clear: %v", err)
	}
	if got.NextAttemptAt != nil {
		t.Fatalf("expected next_attempt_at cleared, got %v", *got.NextAttemptAt)
	}
}

func TestGetByID_GetByRideID_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresInvoiceRepository(testDB)

	if _, err := repo.GetByID(ctx, "00000000-0000-0000-0000-000000000099"); !errors.Is(err, commonerrors.ErrNotFound) {
		t.Fatalf("GetByID: expected ErrNotFound, got %v", err)
	}
	if _, err := repo.GetByRideID(ctx, "00000000-0000-0000-0000-000000000099", domain.InvoiceTypeRideFare); !errors.Is(err, commonerrors.ErrNotFound) {
		t.Fatalf("GetByRideID: expected ErrNotFound, got %v", err)
	}
}

func TestGetList_Pagination(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresInvoiceRepository(testDB)
	clientID := seedClient(t, testDB)

	before, err := repo.CountInvoices(ctx)
	if err != nil {
		t.Fatalf("CountInvoices before: %v", err)
	}

	for range 3 {
		rideID := seedRide(t, testDB, clientID, "")
		if _, err := repo.Create(ctx, newOpenInvoice(rideID, clientID)); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	after, err := repo.CountInvoices(ctx)
	if err != nil {
		t.Fatalf("CountInvoices after: %v", err)
	}
	if after-before != 3 {
		t.Fatalf("expected count delta of 3, got %d", after-before)
	}

	list, err := repo.GetList(ctx, domain.PageRequest{Page: 1, PageSize: after, SortBy: "createdAt", SortDir: "DESC"})
	if err != nil {
		t.Fatalf("GetList: %v", err)
	}
	if len(list) != after {
		t.Fatalf("expected %d invoices, got %d", after, len(list))
	}
}

func TestCountOpenByClientID_Scoped(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresInvoiceRepository(testDB)
	clientID := seedClient(t, testDB)
	otherClientID := seedClient(t, testDB)

	rideID := seedRide(t, testDB, clientID, "")
	if _, err := repo.Create(ctx, newOpenInvoice(rideID, clientID)); err != nil {
		t.Fatalf("Create for clientID: %v", err)
	}
	otherRideID := seedRide(t, testDB, otherClientID, "")
	if _, err := repo.Create(ctx, newOpenInvoice(otherRideID, otherClientID)); err != nil {
		t.Fatalf("Create for otherClientID: %v", err)
	}

	n, err := repo.CountOpenByClientID(ctx, clientID)
	if err != nil {
		t.Fatalf("CountOpenByClientID: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 open invoice scoped to clientID, got %d", n)
	}
}
