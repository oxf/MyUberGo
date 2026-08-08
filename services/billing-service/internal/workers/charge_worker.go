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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tracer names ChargeWorker's own spans (w.provider.Charge is the riskiest network call in the
// platform) — same instrumentation-scope name the outbox worker used before it moved to common/outbox.
var tracer = otel.Tracer("billing-service/outbox")

const defaultBatchSize = 10

// ChargeWorker sweeps open invoices whose retry deadline has elapsed and collects via PaymentProvider.
// Each tick: claim (lock+lease due invoices), charge (provider call, no txn/locks held), finalize (post the ledger).
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
	ctx, span := tracer.Start(ctx, "charge worker sweep")
	defer span.End()

	claims, err := w.claimBatch(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetAttributes(attribute.Int("billing.claimed_count", len(claims)))

	for _, claim := range claims {
		w.resolve(ctx, claim)
	}
	return nil
}

// chargeClaim is everything the charge/finalize phases need, derived from data claimBatch already
// committed — neither phase reads the DB again except through the finalize commands' own transactions.
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

// claimBatch locks and claims the whole due batch in one transaction. A genuine claimOne error aborts
// the Postgres transaction server-side, so the whole batch is abandoned this tick and safely retried next.
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
					// A missing customer/payment-method row is a per-invoice data inconsistency, not a
					// transient SQL failure, so it's safe to skip just this invoice rather than the whole batch.
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

// claimOne returns nil (no error) when the invoice should be skipped this tick: an in-flight attempt
// is still within its claimed_until lease.
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
		// Lease expired: resume with the same idempotency key so the provider recognizes a retry
		// instead of double-charging, and extend the lease so no other tick also tries to resume it.
		if err := w.paymentRepo.SetClaimedUntil(ctx, existing.ID, lease); err != nil {
			return nil, err
		}
		return w.buildClaim(ctx, inv, existing)
	}

	// No non-terminal payment row: a fresh attempt. inv.AttemptCount is already derived by the same
	// GetDueForCharge query that returned this invoice, so no separate count call is needed.
	attemptNo := inv.AttemptCount + 1
	idempotencyKey := fmt.Sprintf("invoice:%s:attempt:%d", inv.ID, attemptNo)

	pm, err := w.paymentMethodRepo.GetDefaultActive(ctx, inv.ClientID)
	if err != nil {
		if !errors.Is(err, commonerrors.ErrNotFound) {
			return nil, err
		}
		// No active payment method: created Pending, not Failed, even though no provider call happens —
		// MarkFailed's guarded UPDATE only matches pending/processing, so inserting Failed directly would make it a permanent no-op.
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

	// A payment method can only exist for a client with a billing.customer row already (created first by
	// AddPaymentMethodHandler), so a failed lookup here is a genuine inconsistency, surfaced as an error.
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
		// A no-payment-method claim whose FinalizeChargeFailed didn't complete on a prior tick — retry
		// the same immediate-failure finalization rather than erroring out the whole batch.
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

// leaseActive reports whether a claimed_until timestamp is still in the future. A nil timestamp
// (shouldn't happen for a non-terminal row) is treated as not-leased rather than a permanently stuck claim.
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
	ctx, span := tracer.Start(ctx, "charge invoice", trace.WithAttributes(
		attribute.String("billing.invoice_id", claim.InvoiceID),
		attribute.String("billing.payment_id", claim.PaymentID),
	))
	defer span.End()

	if claim.ImmediateFailure {
		span.SetAttributes(attribute.String("billing.charge_outcome", "no_payment_method"))
		if err := w.application.Commands.FinalizeChargeFailed.Handle(ctx, command.FinalizeChargeFailed{
			PaymentID: claim.PaymentID, InvoiceID: claim.InvoiceID,
			FailureCode: claim.FailureCode, FailureMessage: claim.FailureMessage, Provider: w.providerName,
		}); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			w.logger.WithError(err).WithField("invoice_id", claim.InvoiceID).Error("charge worker: finalize (no payment method) failed")
		}
		return
	}

	result := w.charge(ctx, claim)
	span.SetAttributes(attribute.String("billing.charge_outcome", string(result.Status)))

	switch result.Status {
	case services.ChargeSucceeded:
		if err := w.application.Commands.FinalizeChargeSucceeded.Handle(ctx, command.FinalizeChargeSucceeded{
			PaymentID: claim.PaymentID, InvoiceID: claim.InvoiceID, ProviderIntentID: result.ProviderIntentID, Provider: w.providerName,
		}); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			w.logger.WithError(err).WithField("invoice_id", claim.InvoiceID).Error("charge worker: finalize (succeeded) failed")
		}
	case services.ChargeProcessing:
		// A genuinely async outcome, modeled from day one though not produced by the stub. Mark
		// processing (guarded, safe to double-call) and stop the sweep: only the webhook resolves it further.
		if won, err := w.paymentRepo.MarkProcessing(ctx, claim.PaymentID, result.ProviderIntentID); err != nil || !won {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				w.logger.WithError(err).WithField("invoice_id", claim.InvoiceID).Error("charge worker: mark processing failed")
			}
			return
		}
		if err := w.invoiceRepo.SetNextAttemptAt(ctx, claim.InvoiceID, nil); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			w.logger.WithError(err).WithField("invoice_id", claim.InvoiceID).Error("charge worker: clearing next_attempt_at failed")
		}
	default: // Failed / RequiresAction
		if err := w.application.Commands.FinalizeChargeFailed.Handle(ctx, command.FinalizeChargeFailed{
			PaymentID: claim.PaymentID, InvoiceID: claim.InvoiceID,
			FailureCode: result.FailureCode, FailureMessage: result.FailureMessage, Provider: w.providerName,
		}); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			w.logger.WithError(err).WithField("invoice_id", claim.InvoiceID).Error("charge worker: finalize (failed) failed")
		}
	}
}

// charge is the one network round-trip to a real payment processor; its span carries `provider` so
// latency/errors split by stub vs. Stripe. A transport error is a span error; a card decline is not.
func (w *ChargeWorker) charge(ctx context.Context, claim chargeClaim) services.ChargeResult {
	ctx, span := tracer.Start(ctx, "provider.Charge",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("payment.provider", w.providerName),
			attribute.Int64("billing.amount_minor", claim.AmountMinor),
			attribute.String("billing.currency", claim.Currency),
		),
	)
	defer span.End()

	result, err := w.provider.Charge(ctx, services.ChargeRequest{
		IdempotencyKey:          claim.IdempotencyKey,
		ProviderCustomerID:      claim.ProviderCustomerID,
		ProviderPaymentMethodID: claim.ProviderPaymentMethodID,
		AmountMinor:             claim.AmountMinor,
		Currency:                claim.Currency,
	})
	if err != nil {
		// A transport/API error, not a card decline — treat as a failed attempt so it counts
		// toward the retry budget instead of spinning forever.
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return services.ChargeResult{Status: services.ChargeFailed, FailureCode: "provider_error", FailureMessage: err.Error()}
	}
	span.SetAttributes(attribute.String("billing.charge_status", string(result.Status)))
	return result
}
