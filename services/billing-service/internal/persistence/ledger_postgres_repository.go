package persistence

import (
	"billing-service/internal/domain"
	"context"
	"database/sql"
)

type PostgresLedgerRepository struct {
	db *sql.DB
}

func NewPostgresLedgerRepository(db *sql.DB) *PostgresLedgerRepository {
	return &PostgresLedgerRepository{db: db}
}

// sentinelOwnerID collapses NULL owner_id (platform-level accounts) to a
// fixed value for uniqueness lookups — see the idx_ledger_account_unique
// comment in init.sql for why a plain UNIQUE(type, owner_id, currency)
// wouldn't actually prevent duplicate platform accounts.
const sentinelOwnerID = "00000000-0000-0000-0000-000000000000"

// PostTransaction validates the legs (BILLING_SPEC.md §4.4 invariants),
// get-or-creates each leg's account, and inserts one ledger_transaction row
// plus one ledger_entry row per leg — all via Executor(ctx, db), so it
// participates in the caller's ambient transaction (e.g. alongside the
// invoice INSERT for T1).
func (r *PostgresLedgerRepository) PostTransaction(ctx context.Context, txType, refType, refID, currency string, legs []domain.LedgerLeg) error {
	if err := domain.ValidateLegs(currency, legs); err != nil {
		return err
	}

	executor := Executor(ctx, r.db)

	var txID string
	if err := executor.QueryRowContext(ctx, `
		INSERT INTO billing.ledger_transaction (type, ref_type, ref_id, currency)
		VALUES ($1,$2,$3,$4)
		RETURNING id
	`, txType, refType, refID, currency).Scan(&txID); err != nil {
		return err
	}

	for _, leg := range legs {
		accountID, err := r.getOrCreateAccount(ctx, executor, leg.AccountType, leg.OwnerID, leg.Currency)
		if err != nil {
			return err
		}

		if _, err := executor.ExecContext(ctx, `
			INSERT INTO billing.ledger_entry (transaction_id, account_id, direction, amount_minor, currency)
			VALUES ($1,$2,$3,$4,$5)
		`, txID, accountID, leg.Direction, leg.AmountMinor, leg.Currency); err != nil {
			return err
		}
	}

	return nil
}

func (r *PostgresLedgerRepository) getOrCreateAccount(ctx context.Context, executor DBExecutor, accountType, ownerID, currency string) (string, error) {
	var ownerArg any
	if ownerID != "" {
		ownerArg = ownerID
	}

	const selectSQL = `
		SELECT id FROM billing.ledger_account
		WHERE type = $1
		  AND COALESCE(owner_id, $4::uuid) = COALESCE($2::uuid, $4::uuid)
		  AND currency = $3
	`

	var id string
	err := executor.QueryRowContext(ctx, selectSQL, accountType, ownerArg, currency, sentinelOwnerID).Scan(&id)
	if err == nil {
		return id, nil
	}
	if err != sql.ErrNoRows {
		return "", err
	}

	err = executor.QueryRowContext(ctx, `
		INSERT INTO billing.ledger_account (type, owner_id, currency)
		VALUES ($1,$2,$3)
		RETURNING id
	`, accountType, ownerArg, currency).Scan(&id)
	if err != nil {
		if isUniqueViolation(err) {
			// Lost a race against a concurrent transaction creating the
			// same account — read back what it created.
			if err2 := executor.QueryRowContext(ctx, selectSQL, accountType, ownerArg, currency, sentinelOwnerID).Scan(&id); err2 == nil {
				return id, nil
			} else {
				return "", err2
			}
		}
		return "", err
	}

	return id, nil
}

// GetBalance computes SUM(credits) - SUM(debits) for one account, scoped to
// a single currency (BILLING_SPEC.md §4.4 invariant 3: balance is always
// computed, never a stored column).
func (r *PostgresLedgerRepository) GetBalance(ctx context.Context, accountType, ownerID, currency string) (int64, error) {
	var ownerArg any
	if ownerID != "" {
		ownerArg = ownerID
	}

	var balance sql.NullInt64
	err := r.db.QueryRowContext(ctx, `
		SELECT SUM(CASE WHEN e.direction = 'credit' THEN e.amount_minor ELSE -e.amount_minor END)
		FROM billing.ledger_entry e
		JOIN billing.ledger_account a ON a.id = e.account_id
		WHERE a.type = $1
		  AND COALESCE(a.owner_id, $4::uuid) = COALESCE($2::uuid, $4::uuid)
		  AND a.currency = $3
	`, accountType, ownerArg, currency, sentinelOwnerID).Scan(&balance)
	if err != nil {
		return 0, err
	}
	return balance.Int64, nil
}
