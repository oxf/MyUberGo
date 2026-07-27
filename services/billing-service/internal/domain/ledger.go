package domain

import (
	"context"
	"errors"
)

const (
	LedgerAccountClientReceivable = "client_receivable"
	LedgerAccountDriverPayable    = "driver_payable"
	LedgerAccountPlatformRevenue  = "platform_revenue"
	LedgerAccountPSPClearing      = "psp_clearing"
	LedgerAccountPSPFees          = "psp_fees"
	LedgerAccountBadDebt          = "bad_debt"
)

const (
	LedgerDirectionDebit  = "debit"
	LedgerDirectionCredit = "credit"
)

const (
	LedgerTxInvoiceOpened        = "invoice_opened"
	LedgerTxPaymentSucceeded     = "payment_succeeded"
	LedgerTxInvoiceUncollectible = "invoice_uncollectible"
)

// LedgerLeg is one posting instruction. The account is identified by
// (AccountType, OwnerID, Currency) rather than an account ID — the
// repository get-or-creates the account row inside the posting transaction
// (BILLING_SPEC.md §4.4: "created lazily on first use"). OwnerID is empty
// for platform-level accounts (platform_revenue, psp_clearing, bad_debt).
type LedgerLeg struct {
	AccountType string
	OwnerID     string
	Currency    string
	Direction   string
	AmountMinor int64
}

var (
	// ErrUnbalancedLedgerTransaction guards invariant 1 (BILLING_SPEC.md
	// §4.4): every transaction's debits must equal its credits.
	ErrUnbalancedLedgerTransaction = errors.New("ledger transaction: debits do not equal credits")
	// ErrMixedCurrencyLedgerTransaction guards invariant 2: a transaction
	// never mixes currencies (v1 has no FX).
	ErrMixedCurrencyLedgerTransaction = errors.New("ledger transaction: legs must share one currency")
)

// ValidateLegs enforces the double-entry invariants before any leg reaches
// persistence. Called by every command that posts a ledger transaction
// (T1/T2/T3), so an unbalanced or mixed-currency posting never reaches the
// database in the first place.
func ValidateLegs(currency string, legs []LedgerLeg) error {
	var debits, credits int64
	for _, l := range legs {
		if l.Currency != currency {
			return ErrMixedCurrencyLedgerTransaction
		}
		switch l.Direction {
		case LedgerDirectionDebit:
			debits += l.AmountMinor
		case LedgerDirectionCredit:
			credits += l.AmountMinor
		}
	}
	if debits != credits {
		return ErrUnbalancedLedgerTransaction
	}
	return nil
}

// LedgerRepository posts a validated set of legs as one atomic transaction
// and reads back computed balances. There is deliberately no update/delete
// method — entries are append-only; corrections are new compensating
// transactions (BILLING_SPEC.md §4.4).
type LedgerRepository interface {
	// PostTransaction get-or-creates each leg's account, inserts one
	// ledger_transaction row, and inserts one ledger_entry row per leg, all
	// in the caller's ambient DB transaction (via Executor(ctx, db)).
	PostTransaction(ctx context.Context, txType, refType, refID, currency string, legs []LedgerLeg) error
	// GetBalance computes SUM(credits) - SUM(debits) for one account,
	// scoped to a single currency — never summed across currencies
	// (BILLING_SPEC.md §4.4 invariant 4).
	GetBalance(ctx context.Context, accountType, ownerID, currency string) (int64, error)
}
