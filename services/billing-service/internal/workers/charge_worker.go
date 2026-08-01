package workers

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/application/services"
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// ChargeWorker sweeps open invoices whose retry deadline has elapsed and
// attempts collection through the PaymentProvider port. Each tick runs in
// three phases:
//
//  1. claim  — a single short DB transaction that locks due invoices
//     (FOR UPDATE SKIP LOCKED), then for each one decides: skip (an
//     in-flight attempt is still within its claimed_until lease), resume
//     (the lease expired — reuse the same idempotency key), or start fresh.
//  2. charge — the provider call itself, with NO transaction open and no
//     row locks held. This is the entire reason claim/charge/finalize are
//     split: a real provider is a network round-trip, which must never
//     happen inside a Postgres transaction holding FOR UPDATE locks.
//  3. finalize — FinalizeChargeSucceeded/FinalizeChargeFailed (their own
//     short transactions), shared with the webhook handler so the ledger
//     is posted exactly once no matter which caller resolves first.
type ChargeWorker struct {
	invoiceRepo       domain.InvoiceRepository
	paymentRepo       domain.PaymentRepository
	paymentMethodRepo domain.PaymentMethodRepository
	customerRepo      domain.CustomerRepository
	provider          services.PaymentProvider
	transaction       services.TransactionManager
	application       app.Application
	providerName      string
	logger            *logrus.Entry
	interval          time.Duration
	batchSize         int
	leaseDuration     time.Duration
}

func NewChargeWorker(
	invoiceRepo domain.InvoiceRepository,
	paymentRepo domain.PaymentRepository,
	paymentMethodRepo domain.PaymentMethodRepository,
	customerRepo domain.CustomerRepository,
	provider services.PaymentProvider,
	transaction services.TransactionManager,
	application app.Application,
	providerName string,
	logger *logrus.Entry,
	interval time.Duration,
	leaseDuration time.Duration,
) *ChargeWorker {

	return &ChargeWorker{
		invoiceRepo:       invoiceRepo,
		paymentRepo:       paymentRepo,
		paymentMethodRepo: paymentMethodRepo,
		customerRepo:      customerRepo,
		provider:          provider,
		transaction:       transaction,
		application:       application,
		providerName:      providerName,
		logger:            logger,
		interval:          interval,
		batchSize:         defaultBatchSize,
		leaseDuration:     leaseDuration,
	}
}

func (w *ChargeWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.processBatch(ctx); err != nil {
				w.logger.WithError(err).Error("charge worker: batch processing failed")
			}
		}
	}
}

func (w *ChargeWorker) processBatch(ctx context.Context) error {
	claims, err := w.claimBatch(ctx)
	if err != nil {
		return err
	}

	for _, claim := range claims {
		w.resolve(ctx, claim)
	}
	return nil
}

// chargeClaim is everything the charge/finalize phases need, entirely
// derived from data already committed by claimBatch — neither phase reads
// the DB again except through the finalize commands' own transactions.
type chargeClaim struct {
	PaymentID               string
	InvoiceID               string
	ImmediateFailure        bool
	FailureCode             string
	FailureMessage          string
	IdempotencyKey          string
	ProviderCustomerID      string
	ProviderPaymentMethodID string
	AmountMinor             int64
	Currency                string
}

// claimBatch locks and claims the whole due batch in one transaction. A
// genuine error from claimOne (a failed SQL statement) aborts the Postgres
// transaction server-side regardless of Go-level handling — so on error the
// whole batch is abandoned for this tick rather than continuing to touch an
// already-aborted transaction (the same lesson CreateInvoiceFromRideHandler
// learned the hard way). FOR UPDATE SKIP LOCKED plus the resume/skip logic
// make retrying the whole batch next tick safe.
func (w *ChargeWorker) claimBatch(ctx context.Context) ([]chargeClaim, error) {
	var claims []chargeClaim
	err := w.transaction.WithinTransaction(ctx, func(txCtx context.Context) error {
		invoices, err := w.invoiceRepo.GetDueForCharge(txCtx, w.batchSize)
		if err != nil {
			return err
		}

		lease := time.Now().UTC().Add(w.leaseDuration).Format(time.RFC3339)

		for _, inv := range invoices {
			claim, err := w.claimOne(txCtx, inv, lease)
			if err != nil {
				if errors.Is(err, commonerrors.ErrNotFound) {
					// A missing customer/payment-method row for this one
					// invoice (e.g. its client's billing data predates a
					// PAYMENT_PROVIDER switch) is a per-invoice data
					// inconsistency, not a transient SQL failure — unlike a
					// real statement error, a SELECT finding zero rows never
					// aborts the surrounding Postgres transaction, so it's
					// safe to skip just this one invoice rather than
					// abandoning every other, unrelated invoice behind it
					// for the rest of this batch (and every batch after,
					// since nothing here ever resolves it).
					w.logger.WithField("invoice_id", inv.ID).WithField("client_id", inv.ClientID).
						Warn("charge worker: skipping invoice with missing customer/payment-method data")
					continue
				}
				return err
			}
			if claim != nil {
				claims = append(claims, *claim)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claims, nil
}

// claimOne returns nil (no error) when the invoice should be skipped this
// tick — an in-flight attempt is still within its claimed_until lease, so
// neither reclaiming nor re-charging is appropriate.
func (w *ChargeWorker) claimOne(ctx context.Context, inv *domain.Invoice, lease string) (*chargeClaim, error) {
	existing, err := w.paymentRepo.GetNonTerminalByInvoiceID(ctx, inv.ID)
	if err != nil && !errors.Is(err, commonerrors.ErrNotFound) {
		return nil, err
	}

	if err == nil {
		leased, err := leaseActive(existing.ClaimedUntil)
		if err != nil {
			return nil, err
		}
		if leased {
			return nil, nil
		}
		// Lease expired: resume. Reuse the same idempotency key/attempt
		// number untouched — the unchanged key is what lets the provider
		// (real or stub) recognize a retried request instead of charging
		// twice. Extend the lease so a subsequent tick doesn't also try to
		// resume while THIS resume is charging.
		if err := w.paymentRepo.SetClaimedUntil(ctx, existing.ID, lease); err != nil {
			return nil, err
		}
		return w.buildClaim(ctx, inv, existing)
	}

	// No non-terminal payment row: a fresh attempt. inv.AttemptCount is
	// already derived (a correlated subquery over billing.payment) by the
	// same query that returned this invoice via GetDueForCharge — no
	// separate count call needed.
	attemptNo := inv.AttemptCount + 1
	idempotencyKey := fmt.Sprintf("invoice:%s:attempt:%d", inv.ID, attemptNo)

	pm, err := w.paymentMethodRepo.GetDefaultActive(ctx, inv.ClientID)
	if err != nil {
		if !errors.Is(err, commonerrors.ErrNotFound) {
			return nil, err
		}
		// Not in BILLING_SPEC.md, but needed: a client with no active
		// payment method is a distinguishable failure, not a crash — it
		// counts toward attempt_count like any other decline. Created as
		// Pending (not Failed) even though no provider call happens: resolve()
		// below still routes this through FinalizeChargeFailed to schedule the
		// backoff/uncollectible transition, and MarkFailed's guarded UPDATE
		// only matches a pending/processing row — inserting this already-Failed
		// would make that guard a permanent no-op, so the invoice's
		// next_attempt_at would never advance and GetDueForCharge would
		// re-select (and re-fail) it every single tick forever.
		code, message := "no_payment_method", "client has no active default payment method"
		paymentID, err := w.paymentRepo.Create(ctx, &domain.Payment{
			InvoiceID: inv.ID, AttemptNo: attemptNo, Provider: w.providerName,
			AmountMinor: inv.AmountMinor, Currency: inv.Currency,
			Status: domain.PaymentStatusPending, IdempotencyKey: idempotencyKey,
		})
		if err != nil {
			return nil, err
		}
		return &chargeClaim{
			PaymentID: paymentID, InvoiceID: inv.ID, ImmediateFailure: true,
			FailureCode: code, FailureMessage: message,
		}, nil
	}

	paymentID, err := w.paymentRepo.Create(ctx, &domain.Payment{
		InvoiceID: inv.ID, AttemptNo: attemptNo, Provider: w.providerName,
		PaymentMethodID: &pm.ID, AmountMinor: inv.AmountMinor, Currency: inv.Currency,
		Status: domain.PaymentStatusPending, IdempotencyKey: idempotencyKey,
		ClaimedUntil: &lease,
	})
	if err != nil {
		return nil, err
	}

	// A payment method can only exist for a client that already has a
	// billing.customer row for this provider (AddPaymentMethodHandler
	// always creates the customer first) — so this lookup failing would be
	// a genuine data inconsistency, surfaced as an error rather than
	// defended against.
	customer, err := w.customerRepo.GetByClientID(ctx, inv.ClientID, w.providerName)
	if err != nil {
		return nil, err
	}

	return &chargeClaim{
		PaymentID: paymentID, InvoiceID: inv.ID,
		IdempotencyKey: idempotencyKey, ProviderCustomerID: customer.ProviderCustomerID,
		ProviderPaymentMethodID: pm.ProviderPaymentMethodID,
		AmountMinor:             inv.AmountMinor, Currency: inv.Currency,
	}, nil
}

func (w *ChargeWorker) buildClaim(ctx context.Context, inv *domain.Invoice, existing *domain.Payment) (*chargeClaim, error) {
	if existing.PaymentMethodID == nil {
		// A no-payment-method claim (see claimOne) whose FinalizeChargeFailed
		// call didn't complete on a prior tick (e.g. a transient DB error) —
		// still Pending, with no payment method to resume charging against.
		// Retry the same immediate-failure finalization rather than erroring
		// out the whole batch.
		return &chargeClaim{
			PaymentID: existing.ID, InvoiceID: inv.ID, ImmediateFailure: true,
			FailureCode: "no_payment_method", FailureMessage: "client has no active default payment method",
		}, nil
	}
	pm, err := w.paymentMethodRepo.GetByID(ctx, *existing.PaymentMethodID)
	if err != nil {
		return nil, err
	}
	customer, err := w.customerRepo.GetByClientID(ctx, inv.ClientID, w.providerName)
	if err != nil {
		return nil, err
	}
	return &chargeClaim{
		PaymentID: existing.ID, InvoiceID: inv.ID,
		IdempotencyKey: existing.IdempotencyKey, ProviderCustomerID: customer.ProviderCustomerID,
		ProviderPaymentMethodID: pm.ProviderPaymentMethodID,
		AmountMinor:             existing.AmountMinor, Currency: existing.Currency,
	}, nil
}

// leaseActive reports whether a claimed_until timestamp is still in the
// future. A nil timestamp (shouldn't happen for a non-terminal row) is
// treated as not-leased, so a data inconsistency degrades to "resumable"
// rather than a permanently stuck claim.
func leaseActive(claimedUntil *string) (bool, error) {
	if claimedUntil == nil {
		return false, nil
	}
	t, err := time.Parse(time.RFC3339, *claimedUntil)
	if err != nil {
		return false, err
	}
	return t.After(time.Now().UTC()), nil
}

func (w *ChargeWorker) resolve(ctx context.Context, claim chargeClaim) {
	if claim.ImmediateFailure {
		if err := w.application.Commands.FinalizeChargeFailed.Handle(ctx, command.FinalizeChargeFailed{
			PaymentID: claim.PaymentID, InvoiceID: claim.InvoiceID,
			FailureCode: claim.FailureCode, FailureMessage: claim.FailureMessage,
		}); err != nil {
			w.logger.WithError(err).WithField("invoice_id", claim.InvoiceID).Error("charge worker: finalize (no payment method) failed")
		}
		return
	}

	result, err := w.provider.Charge(ctx, services.ChargeRequest{
		IdempotencyKey:          claim.IdempotencyKey,
		ProviderCustomerID:      claim.ProviderCustomerID,
		ProviderPaymentMethodID: claim.ProviderPaymentMethodID,
		AmountMinor:             claim.AmountMinor,
		Currency:                claim.Currency,
	})
	if err != nil {
		// A transport/API error, not a card decline — treat it as a failed
		// attempt so it counts toward the retry budget instead of spinning
		// forever; a transient outage still recovers via the next scheduled
		// retry.
		result = services.ChargeResult{Status: services.ChargeFailed, FailureCode: "provider_error", FailureMessage: err.Error()}
	}

	switch result.Status {
	case services.ChargeSucceeded:
		if err := w.application.Commands.FinalizeChargeSucceeded.Handle(ctx, command.FinalizeChargeSucceeded{
			PaymentID: claim.PaymentID, InvoiceID: claim.InvoiceID, ProviderIntentID: result.ProviderIntentID,
		}); err != nil {
			w.logger.WithError(err).WithField("invoice_id", claim.InvoiceID).Error("charge worker: finalize (succeeded) failed")
		}
	case services.ChargeProcessing:
		// A genuinely async outcome — not produced by the stub, and not
		// expected for a card charge even with real Stripe (D6), but
		// modeled from day one. Mark the payment processing (guarded — a
		// second call from a later resume or the webhook handler is a safe
		// no-op) and stop the routine sweep: only the webhook or Phase 3's
		// reconciliation drift-detector should resolve this from here.
		if won, err := w.paymentRepo.MarkProcessing(ctx, claim.PaymentID, result.ProviderIntentID); err != nil || !won {
			if err != nil {
				w.logger.WithError(err).WithField("invoice_id", claim.InvoiceID).Error("charge worker: mark processing failed")
			}
			return
		}
		if err := w.invoiceRepo.SetNextAttemptAt(ctx, claim.InvoiceID, nil); err != nil {
			w.logger.WithError(err).WithField("invoice_id", claim.InvoiceID).Error("charge worker: clearing next_attempt_at failed")
		}
	default: // Failed / RequiresAction
		if err := w.application.Commands.FinalizeChargeFailed.Handle(ctx, command.FinalizeChargeFailed{
			PaymentID: claim.PaymentID, InvoiceID: claim.InvoiceID,
			FailureCode: result.FailureCode, FailureMessage: result.FailureMessage,
		}); err != nil {
			w.logger.WithError(err).WithField("invoice_id", claim.InvoiceID).Error("charge worker: finalize (failed) failed")
		}
	}
}
