package command

import (
	"billing-service/internal/application/services"
	"billing-service/internal/common/decorator"
	"billing-service/internal/domain"
	"context"
	"encoding/json"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
)

// FinalizeChargeFailed resolves a payment attempt that the provider (or a
// pre-charge check like "no active payment method") reported as failed.
// Shared with a later webhook handler for the same reason as
// FinalizeChargeSucceeded — see that file's comment.
type FinalizeChargeFailed struct {
	PaymentID      string
	InvoiceID      string
	FailureCode    string
	FailureMessage string
}

type FinalizeChargeFailedHandler struct {
	invoiceRepo domain.InvoiceRepository
	paymentRepo domain.PaymentRepository
	ledgerRepo  domain.LedgerRepository
	outboxRepo  domain.OutboxRepository
	transaction services.TransactionManager
	maxAttempts int
	backoff     []time.Duration
	logger      *logrus.Entry
}

func NewFinalizeChargeFailedHandler(
	invoiceRepo domain.InvoiceRepository,
	paymentRepo domain.PaymentRepository,
	ledgerRepo domain.LedgerRepository,
	outboxRepo domain.OutboxRepository,
	transaction services.TransactionManager,
	maxAttempts int,
	backoff []time.Duration,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[FinalizeChargeFailed] {

	handler := &FinalizeChargeFailedHandler{
		invoiceRepo: invoiceRepo,
		paymentRepo: paymentRepo,
		ledgerRepo:  ledgerRepo,
		outboxRepo:  outboxRepo,
		transaction: transaction,
		maxAttempts: maxAttempts,
		backoff:     backoff,
		logger:      logger,
	}

	return decorator.ApplyCommandDecoratorsNoResult[FinalizeChargeFailed](
		handler,
		logger,
		metricsClient,
	)
}

func (h *FinalizeChargeFailedHandler) Handle(ctx context.Context, cmd FinalizeChargeFailed) error {
	return h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		won, err := h.paymentRepo.MarkFailed(ctx, cmd.PaymentID, cmd.FailureCode, cmd.FailureMessage)
		if err != nil {
			return err
		}
		if !won {
			h.logger.WithField("payment_id", cmd.PaymentID).Info(
				"finalize charge failed: payment already resolved, skipping")
			return nil
		}

		inv, err := h.invoiceRepo.GetByID(ctx, cmd.InvoiceID)
		if err != nil {
			return err
		}
		if inv.Status != domain.InvoiceStatusOpen {
			// Resolved through some other path (e.g. a concurrent success)
			// between the payment guard above and this read — the payment
			// row is already correctly marked failed; there's nothing left
			// to do to the invoice.
			h.logger.WithField("invoice_id", inv.ID).Info(
				"finalize charge failed: invoice no longer open, skipping invoice-level update")
			return nil
		}

		// inv.AttemptCount already reflects this attempt — the claim step
		// (ChargeWorker) increments it before charging, not here.
		if inv.AttemptCount < h.maxAttempts {
			next := time.Now().UTC().Add(h.backoffFor(inv.AttemptCount)).Format(time.RFC3339)
			return h.invoiceRepo.SetNextAttemptAt(ctx, inv.ID, &next)
		}

		if err := h.invoiceRepo.SetNextAttemptAt(ctx, inv.ID, nil); err != nil {
			return err
		}
		uncWon, err := h.invoiceRepo.MarkUncollectible(ctx, inv.ID)
		if err != nil {
			return err
		}
		if !uncWon {
			return nil
		}

		// T3 (BILLING_SPEC.md §5): bad_debt debited, client_receivable
		// credited. driver_payable deliberately stays posted from T1 — the
		// driver is still owed money the platform never collected.
		legs := []domain.LedgerLeg{
			{AccountType: domain.LedgerAccountBadDebt, Currency: inv.Currency, Direction: domain.LedgerDirectionDebit, AmountMinor: inv.AmountMinor},
			{AccountType: domain.LedgerAccountClientReceivable, OwnerID: inv.ClientID, Currency: inv.Currency, Direction: domain.LedgerDirectionCredit, AmountMinor: inv.AmountMinor},
		}
		if err := h.ledgerRepo.PostTransaction(ctx, domain.LedgerTxInvoiceUncollectible, "invoice", inv.ID, inv.Currency, legs); err != nil {
			return err
		}

		event := contractsKafka.PaymentFailedEvent{
			RideID: inv.RideID, InvoiceID: inv.ID, ClientID: inv.ClientID,
			AmountMinor: inv.AmountMinor, Currency: inv.Currency, FailureCode: cmd.FailureCode,
			FailedAt: time.Now().UTC().Format(time.RFC3339),
		}
		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}
		return h.outboxRepo.Insert(ctx, &domain.OutboxMessage{
			Topic: "payment.failed", EventType: "PaymentFailed", Payload: payload,
		})
	})
}

func (h *FinalizeChargeFailedHandler) backoffFor(attemptNo int) time.Duration {
	idx := attemptNo - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(h.backoff) {
		idx = len(h.backoff) - 1
	}
	if idx < 0 {
		return time.Minute
	}
	return h.backoff[idx]
}
