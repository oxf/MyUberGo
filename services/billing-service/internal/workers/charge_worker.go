package workers

import (
	"billing-service/internal/application/services"
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

// ChargeWorker sweeps open invoices whose retry deadline has elapsed,
// attempts collection through the PaymentProvider port, and posts the
// resulting ledger transaction (T2 on success, T3 once attempts are
// exhausted) — same ticker+select shape as OutboxWorker.
type ChargeWorker struct {
	invoiceRepo       domain.InvoiceRepository
	paymentRepo       domain.PaymentRepository
	paymentMethodRepo domain.PaymentMethodRepository
	ledgerRepo        domain.LedgerRepository
	outboxRepo        domain.OutboxRepository
	provider          services.PaymentProvider
	transaction       services.TransactionManager
	logger            *logrus.Entry
	interval          time.Duration
	batchSize         int
	maxAttempts       int
	backoff           []time.Duration
	commissionBps     int64
}

func NewChargeWorker(
	invoiceRepo domain.InvoiceRepository,
	paymentRepo domain.PaymentRepository,
	paymentMethodRepo domain.PaymentMethodRepository,
	ledgerRepo domain.LedgerRepository,
	outboxRepo domain.OutboxRepository,
	provider services.PaymentProvider,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	interval time.Duration,
	maxAttempts int,
	backoff []time.Duration,
	commissionBps int64,
) *ChargeWorker {

	return &ChargeWorker{
		invoiceRepo:       invoiceRepo,
		paymentRepo:       paymentRepo,
		paymentMethodRepo: paymentMethodRepo,
		ledgerRepo:        ledgerRepo,
		outboxRepo:        outboxRepo,
		provider:          provider,
		transaction:       transaction,
		logger:            logger,
		interval:          interval,
		batchSize:         defaultBatchSize,
		maxAttempts:       maxAttempts,
		backoff:           backoff,
		commissionBps:     commissionBps,
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
	return w.transaction.WithinTransaction(ctx, func(txCtx context.Context) error {
		invoices, err := w.invoiceRepo.GetDueForCharge(txCtx, w.batchSize)
		if err != nil {
			return err
		}

		for _, inv := range invoices {
			if err := w.attempt(txCtx, inv); err != nil {
				w.logger.WithError(err).WithField("invoice_id", inv.ID).Error("charge worker: attempt failed")
			}
		}

		return nil
	})
}

func (w *ChargeWorker) attempt(ctx context.Context, inv *domain.Invoice) error {
	attemptNo := inv.AttemptCount + 1
	idempotencyKey := fmt.Sprintf("invoice:%s:attempt:%d", inv.ID, attemptNo)

	pm, err := w.paymentMethodRepo.GetDefaultActive(ctx, inv.ClientID)
	if err != nil {
		if errors.Is(err, commonerrors.ErrNotFound) {
			// Not in BILLING_SPEC.md, but needed: a client with no active
			// payment method is a distinguishable failure, not a crash —
			// it counts toward attempt_count like any other decline.
			code, message := "no_payment_method", "client has no active default payment method"
			if _, err := w.paymentRepo.Create(ctx, &domain.Payment{
				InvoiceID: inv.ID, AttemptNo: attemptNo, Provider: domain.ProviderStub,
				AmountMinor: inv.AmountMinor, Currency: inv.Currency,
				Status: domain.PaymentStatusFailed, FailureCode: &code, FailureMessage: &message,
				IdempotencyKey: idempotencyKey,
			}); err != nil {
				return err
			}
			return w.afterFailedAttempt(ctx, inv, attemptNo, code)
		}
		return err
	}

	paymentID, err := w.paymentRepo.Create(ctx, &domain.Payment{
		InvoiceID: inv.ID, AttemptNo: attemptNo, Provider: domain.ProviderStub,
		PaymentMethodID: &pm.ID, AmountMinor: inv.AmountMinor, Currency: inv.Currency,
		Status: domain.PaymentStatusPending, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return err
	}

	result, err := w.provider.Charge(ctx, services.ChargeRequest{
		IdempotencyKey:          idempotencyKey,
		ProviderPaymentMethodID: pm.ProviderPaymentMethodID,
		AmountMinor:             inv.AmountMinor,
		Currency:                inv.Currency,
	})
	if err != nil {
		return err
	}

	switch result.Status {
	case services.ChargeSucceeded:
		return w.onSucceeded(ctx, inv, paymentID, result)
	case services.ChargeProcessing:
		// Unreachable with the v1 stub (D6): leave the payment row pending
		// and next_attempt_at unset until a future webhook/poller resolves
		// it — do not record a failed attempt.
		return nil
	default: // Failed / RequiresAction
		if err := w.paymentRepo.MarkFailed(ctx, paymentID, result.FailureCode, result.FailureMessage); err != nil {
			return err
		}
		return w.afterFailedAttempt(ctx, inv, attemptNo, result.FailureCode)
	}
}

// onSucceeded posts T2 (BILLING_SPEC.md §5): psp_clearing debited,
// client_receivable credited, then marks the invoice paid and queues
// payment.completed on the outbox.
func (w *ChargeWorker) onSucceeded(ctx context.Context, inv *domain.Invoice, paymentID string, result services.ChargeResult) error {
	if err := w.paymentRepo.MarkSucceeded(ctx, paymentID, result.ProviderIntentID); err != nil {
		return err
	}

	paidAt := time.Now().UTC().Format(time.RFC3339)
	if err := w.invoiceRepo.MarkPaid(ctx, inv.ID, paidAt); err != nil {
		return err
	}

	legs := []domain.LedgerLeg{
		{AccountType: domain.LedgerAccountPSPClearing, Currency: inv.Currency, Direction: domain.LedgerDirectionDebit, AmountMinor: inv.AmountMinor},
		{AccountType: domain.LedgerAccountClientReceivable, OwnerID: inv.ClientID, Currency: inv.Currency, Direction: domain.LedgerDirectionCredit, AmountMinor: inv.AmountMinor},
	}
	if err := w.ledgerRepo.PostTransaction(ctx, domain.LedgerTxPaymentSucceeded, "invoice", inv.ID, inv.Currency, legs); err != nil {
		return err
	}

	driverID := ""
	platformFeeMinor := inv.AmountMinor
	driverPayableMinor := int64(0)
	if inv.Type == domain.InvoiceTypeRideFare && inv.DriverID != nil {
		driverID = *inv.DriverID
		platformFeeMinor = inv.AmountMinor * w.commissionBps / 10000
		driverPayableMinor = inv.AmountMinor - platformFeeMinor
	}

	event := contractsKafka.PaymentCompletedEvent{
		RideID: inv.RideID, InvoiceID: inv.ID, ClientID: inv.ClientID, DriverID: driverID,
		AmountMinor: inv.AmountMinor, PlatformFeeMinor: platformFeeMinor, DriverPayableMinor: driverPayableMinor,
		Currency: inv.Currency, PaidAt: paidAt,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return w.outboxRepo.Insert(ctx, &domain.OutboxMessage{
		Topic: "payment.completed", EventType: "PaymentCompleted", Payload: payload,
	})
}

// afterFailedAttempt either schedules the next retry with backoff, or —
// once maxAttempts is exhausted — marks the invoice uncollectible, posts T3
// (Dr bad_debt, Cr client_receivable — driver_payable deliberately stays
// posted from T1, since the driver is still owed money the platform never
// collected), and queues payment.failed.
func (w *ChargeWorker) afterFailedAttempt(ctx context.Context, inv *domain.Invoice, attemptNo int, failureCode string) error {
	if attemptNo < w.maxAttempts {
		next := time.Now().UTC().Add(w.backoffFor(attemptNo)).Format(time.RFC3339)
		return w.invoiceRepo.RecordFailedAttempt(ctx, inv.ID, &next)
	}

	if err := w.invoiceRepo.RecordFailedAttempt(ctx, inv.ID, nil); err != nil {
		return err
	}
	if err := w.invoiceRepo.MarkUncollectible(ctx, inv.ID); err != nil {
		return err
	}

	legs := []domain.LedgerLeg{
		{AccountType: domain.LedgerAccountBadDebt, Currency: inv.Currency, Direction: domain.LedgerDirectionDebit, AmountMinor: inv.AmountMinor},
		{AccountType: domain.LedgerAccountClientReceivable, OwnerID: inv.ClientID, Currency: inv.Currency, Direction: domain.LedgerDirectionCredit, AmountMinor: inv.AmountMinor},
	}
	if err := w.ledgerRepo.PostTransaction(ctx, domain.LedgerTxInvoiceUncollectible, "invoice", inv.ID, inv.Currency, legs); err != nil {
		return err
	}

	event := contractsKafka.PaymentFailedEvent{
		RideID: inv.RideID, InvoiceID: inv.ID, ClientID: inv.ClientID,
		AmountMinor: inv.AmountMinor, Currency: inv.Currency, FailureCode: failureCode,
		FailedAt: time.Now().UTC().Format(time.RFC3339),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return w.outboxRepo.Insert(ctx, &domain.OutboxMessage{
		Topic: "payment.failed", EventType: "PaymentFailed", Payload: payload,
	})
}

func (w *ChargeWorker) backoffFor(attemptNo int) time.Duration {
	idx := attemptNo - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(w.backoff) {
		idx = len(w.backoff) - 1
	}
	if idx < 0 {
		return time.Minute
	}
	return w.backoff[idx]
}
