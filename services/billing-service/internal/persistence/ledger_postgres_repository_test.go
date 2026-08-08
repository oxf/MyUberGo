package persistence

import (
	"billing-service/internal/domain"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// fakeRefID produces a syntactically valid UUID for ledger_transaction's
// ref_id column (UUID NOT NULL) — these tests don't need a real
// referenced entity, just a unique value.
func fakeRefID(n int64) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", n)
}

func ledgerAccountCount(t *testing.T, accountType, ownerID, currency string) int {
	t.Helper()
	var ownerArg any
	if ownerID != "" {
		ownerArg = ownerID
	}
	var n int
	if err := testDB.QueryRow(`
		SELECT COUNT(*) FROM billing.ledger_account
		WHERE type = $1
		  AND COALESCE(owner_id, $4::uuid) = COALESCE($2::uuid, $4::uuid)
		  AND currency = $3
	`, accountType, ownerArg, currency, sentinelOwnerID).Scan(&n); err != nil {
		t.Fatalf("count ledger_account: %v", err)
	}
	return n
}

func ledgerTransactionCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := testDB.QueryRow(`SELECT COUNT(*) FROM billing.ledger_transaction`).Scan(&n); err != nil {
		t.Fatalf("count ledger_transaction: %v", err)
	}
	return n
}

func TestPostTransaction_GetOrCreateAccount_Idempotent(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresLedgerRepository(testDB)
	ownerID := seedClient(t, testDB)

	legs := []domain.LedgerLeg{
		{AccountType: domain.LedgerAccountClientReceivable, OwnerID: ownerID, Currency: "EUR", Direction: domain.LedgerDirectionDebit, AmountMinor: 100},
		{AccountType: domain.LedgerAccountPlatformRevenue, OwnerID: "", Currency: "EUR", Direction: domain.LedgerDirectionCredit, AmountMinor: 100},
	}

	if err := repo.PostTransaction(ctx, domain.LedgerTxInvoiceOpened, "invoice", fakeRefID(nextSeq()), "EUR", legs); err != nil {
		t.Fatalf("first PostTransaction: %v", err)
	}
	if err := repo.PostTransaction(ctx, domain.LedgerTxInvoiceOpened, "invoice", fakeRefID(nextSeq()), "EUR", legs); err != nil {
		t.Fatalf("second PostTransaction: %v", err)
	}

	if n := ledgerAccountCount(t, domain.LedgerAccountClientReceivable, ownerID, "EUR"); n != 1 {
		t.Fatalf("expected exactly 1 ledger_account row for repeated postings, got %d", n)
	}
}

// TestGetOrCreateAccount_ConcurrentRaceResolvesToOneAccount fires N
// concurrent PostTransaction calls all targeting a brand-new account triple,
// exercising the real INSERT-then-catch-23505-then-re-SELECT retry loop
// under genuine contention — no fake/mock repository can express this.
func TestGetOrCreateAccount_ConcurrentRaceResolvesToOneAccount(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresLedgerRepository(testDB)
	ownerID := seedClient(t, testDB)

	const n = 8
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			legs := []domain.LedgerLeg{
				{AccountType: domain.LedgerAccountClientReceivable, OwnerID: ownerID, Currency: "EUR", Direction: domain.LedgerDirectionDebit, AmountMinor: 100},
				{AccountType: domain.LedgerAccountPlatformRevenue, OwnerID: "", Currency: "EUR", Direction: domain.LedgerDirectionCredit, AmountMinor: 100},
			}
			errCh <- repo.PostTransaction(ctx, domain.LedgerTxInvoiceOpened, "invoice", fakeRefID(nextSeq()), "EUR", legs)
		})
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent PostTransaction: %v", err)
		}
	}

	if got := ledgerAccountCount(t, domain.LedgerAccountClientReceivable, ownerID, "EUR"); got != 1 {
		t.Fatalf("expected exactly 1 ledger_account row after %d concurrent postings racing to create it, got %d", n, got)
	}
}

func TestPostTransaction_PlatformAccount_NullOwnerCollapsesToSentinel(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresLedgerRepository(testDB)
	ownerID := seedClient(t, testDB)
	const currency = "ZZZ" // distinct from EUR/USD used elsewhere, for test isolation

	legs1 := []domain.LedgerLeg{
		{AccountType: domain.LedgerAccountClientReceivable, OwnerID: ownerID, Currency: currency, Direction: domain.LedgerDirectionDebit, AmountMinor: 100},
		{AccountType: domain.LedgerAccountPlatformRevenue, OwnerID: "", Currency: currency, Direction: domain.LedgerDirectionCredit, AmountMinor: 100},
	}
	if err := repo.PostTransaction(ctx, domain.LedgerTxInvoiceOpened, "invoice", fakeRefID(nextSeq()), currency, legs1); err != nil {
		t.Fatalf("first PostTransaction: %v", err)
	}

	legs2 := []domain.LedgerLeg{
		{AccountType: domain.LedgerAccountPlatformRevenue, OwnerID: "", Currency: currency, Direction: domain.LedgerDirectionDebit, AmountMinor: 50},
		{AccountType: domain.LedgerAccountBadDebt, OwnerID: "", Currency: currency, Direction: domain.LedgerDirectionCredit, AmountMinor: 50},
	}
	if err := repo.PostTransaction(ctx, domain.LedgerTxInvoiceUncollectible, "invoice", fakeRefID(nextSeq()), currency, legs2); err != nil {
		t.Fatalf("second PostTransaction: %v", err)
	}

	if n := ledgerAccountCount(t, domain.LedgerAccountPlatformRevenue, "", currency); n != 1 {
		t.Fatalf("expected two platform-level (null-owner) postings to collapse to exactly 1 account, got %d", n)
	}
}

func TestGetBalance_ExactInt64(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresLedgerRepository(testDB)
	ownerID := seedClient(t, testDB)

	legs1 := []domain.LedgerLeg{
		{AccountType: domain.LedgerAccountClientReceivable, OwnerID: ownerID, Currency: "EUR", Direction: domain.LedgerDirectionDebit, AmountMinor: 1234},
		{AccountType: domain.LedgerAccountPlatformRevenue, OwnerID: "", Currency: "EUR", Direction: domain.LedgerDirectionCredit, AmountMinor: 1234},
	}
	if err := repo.PostTransaction(ctx, domain.LedgerTxInvoiceOpened, "invoice", fakeRefID(nextSeq()), "EUR", legs1); err != nil {
		t.Fatalf("PostTransaction 1: %v", err)
	}

	legs2 := []domain.LedgerLeg{
		{AccountType: domain.LedgerAccountPlatformRevenue, OwnerID: "", Currency: "EUR", Direction: domain.LedgerDirectionDebit, AmountMinor: 400},
		{AccountType: domain.LedgerAccountClientReceivable, OwnerID: ownerID, Currency: "EUR", Direction: domain.LedgerDirectionCredit, AmountMinor: 400},
	}
	if err := repo.PostTransaction(ctx, domain.LedgerTxPaymentSucceeded, "invoice", fakeRefID(nextSeq()), "EUR", legs2); err != nil {
		t.Fatalf("PostTransaction 2: %v", err)
	}

	balance, err := repo.GetBalance(ctx, domain.LedgerAccountClientReceivable, ownerID, "EUR")
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	// debit 1234, then credit 400 => balance = credits - debits = 400 - 1234.
	if balance != -834 {
		t.Fatalf("expected exact balance -834, got %d", balance)
	}
}

func TestGetBalance_ScopedPerCurrency(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresLedgerRepository(testDB)
	ownerID := seedClient(t, testDB)

	eurLegs := []domain.LedgerLeg{
		{AccountType: domain.LedgerAccountClientReceivable, OwnerID: ownerID, Currency: "EUR", Direction: domain.LedgerDirectionDebit, AmountMinor: 100},
		{AccountType: domain.LedgerAccountPlatformRevenue, OwnerID: "", Currency: "EUR", Direction: domain.LedgerDirectionCredit, AmountMinor: 100},
	}
	if err := repo.PostTransaction(ctx, domain.LedgerTxInvoiceOpened, "invoice", fakeRefID(nextSeq()), "EUR", eurLegs); err != nil {
		t.Fatalf("PostTransaction EUR: %v", err)
	}

	usdLegs := []domain.LedgerLeg{
		{AccountType: domain.LedgerAccountClientReceivable, OwnerID: ownerID, Currency: "USD", Direction: domain.LedgerDirectionDebit, AmountMinor: 500},
		{AccountType: domain.LedgerAccountPlatformRevenue, OwnerID: "", Currency: "USD", Direction: domain.LedgerDirectionCredit, AmountMinor: 500},
	}
	if err := repo.PostTransaction(ctx, domain.LedgerTxInvoiceOpened, "invoice", fakeRefID(nextSeq()), "USD", usdLegs); err != nil {
		t.Fatalf("PostTransaction USD: %v", err)
	}

	eurBalance, err := repo.GetBalance(ctx, domain.LedgerAccountClientReceivable, ownerID, "EUR")
	if err != nil {
		t.Fatalf("GetBalance EUR: %v", err)
	}
	if eurBalance != -100 {
		t.Fatalf("expected EUR balance -100 (unaffected by the USD posting), got %d", eurBalance)
	}

	usdBalance, err := repo.GetBalance(ctx, domain.LedgerAccountClientReceivable, ownerID, "USD")
	if err != nil {
		t.Fatalf("GetBalance USD: %v", err)
	}
	if usdBalance != -500 {
		t.Fatalf("expected USD balance -500 (unaffected by the EUR posting), got %d", usdBalance)
	}
}

func TestPostTransaction_ValidationFailureWritesNothing(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresLedgerRepository(testDB)
	ownerID := seedClient(t, testDB)

	before := ledgerTransactionCount(t)

	unbalanced := []domain.LedgerLeg{
		{AccountType: domain.LedgerAccountClientReceivable, OwnerID: ownerID, Currency: "EUR", Direction: domain.LedgerDirectionDebit, AmountMinor: 100},
		{AccountType: domain.LedgerAccountPlatformRevenue, OwnerID: "", Currency: "EUR", Direction: domain.LedgerDirectionCredit, AmountMinor: 50},
	}
	err := repo.PostTransaction(ctx, domain.LedgerTxInvoiceOpened, "invoice", fakeRefID(nextSeq()), "EUR", unbalanced)
	if !errors.Is(err, domain.ErrUnbalancedLedgerTransaction) {
		t.Fatalf("expected ErrUnbalancedLedgerTransaction, got %v", err)
	}

	if after := ledgerTransactionCount(t); after != before {
		t.Fatalf("expected zero rows written on validation failure, ledger_transaction count changed from %d to %d", before, after)
	}
}
