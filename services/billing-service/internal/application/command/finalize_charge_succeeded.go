package command

import (
	"billing-service/internal/application/services"
	"billing-service/internal/common/decorator"
	"billing-service/internal/domain"
	"billing-service/internal/infrastructure/metrics"
	"context"
	"encoding/json"
	"time"

	contractsKafka "github.com/oxf/MyUber/contracts/kafka"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

// FinalizeChargeSucceeded resolves a payment attempt the provider reported as succeeded. Deliberately shared
// between ChargeWorker's synchronous path and a Stripe webhook's async path, so the ledger posts exactly once.
type FinalizeChargeSucceeded struct {
	PaymentID        string
	InvoiceID        string
	ProviderIntentID string
	// Provider is the payment.provider value the caller already has in hand — see
	// FinalizeChargeFailed.Provider's doc comment for why it's threaded through rather than looked up again.
	Provider string
}

type FinalizeChargeSucceededHandler struct {
	invoiceRepo   domain.InvoiceRepository
	paymentRepo   domain.PaymentRepository
	ledgerRepo    domain.LedgerRepository
	outboxRepo    domain.OutboxRepository
	transaction   services.TransactionManager
	commissionBps int64
	logger        *logrus.Entry
	metrics       decorator.MetricsClient
}

func NewFinalizeChargeSucceededHandler(
	invoiceRepo domain.InvoiceRepository,
	paymentRepo domain.PaymentRepository,
	ledgerRepo domain.LedgerRepository,
	outboxRepo domain.OutboxRepository,
	transaction services.TransactionManager,
	commissionBps int64,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[FinalizeChargeSucceeded] {
	if metricsClient == nil {
		metricsClient = metrics.NewNoopMetricsClient()
	}

	handler := &FinalizeChargeSucceededHandler{
		invoiceRepo:   invoiceRepo,
		paymentRepo:   paymentRepo,
		ledgerRepo:    ledgerRepo,
		outboxRepo:    outboxRepo,
		transaction:   transaction,
		commissionBps: commissionBps,
		logger:        logger,
		metrics:       metricsClient,
	}

	return decorator.ApplyCommandDecoratorsNoResult[FinalizeChargeSucceeded](
		handler,
		logger,
		metricsClient,
	)
}

func (h *FinalizeChargeSucceededHandler) Handle(ctx context.Context, cmd FinalizeChargeSucceeded) error {
	// See FinalizeChargeFailedHandler.Handle's identical comment: the metric fires only after WithinTransaction
	// returns successfully, so a later rollback can't leave a phantom "attempted" count for an uncommitted payment.
	attempted := false
	err := h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		won, err := h.paymentRepo.MarkSucceeded(ctx, cmd.PaymentID, cmd.ProviderIntentID)
		if err != nil {
			return err
		}
		if !won {
			// Already resolved by a concurrent finalize (e.g. worker vs. webhook racing the same payment) —
			// a no-op, not a failure, is exactly the point of the guarded update.
			h.logger.WithField("payment_id", cmd.PaymentID).Info(
				"finalize charge succeeded: payment already resolved, skipping")
			return nil
		}
		attempted = true

		inv, err := h.invoiceRepo.GetByID(ctx, cmd.InvoiceID)
		if err != nil {
			return err
		}

		paidAt := time.Now().UTC().Format(time.RFC3339)
		paidWon, err := h.invoiceRepo.MarkPaid(ctx, inv.ID, paidAt)
		if err != nil {
			return err
		}
		if !paidWon {
			// The payment-row guard above already ensures single-finalize per payment; a non-open invoice
			// here means it moved through some other path (e.g. voided) — a genuine inconsistency, not a normal race.
			h.logger.WithField("invoice_id", inv.ID).Warn(
				"finalize charge succeeded: payment succeeded but invoice was not open")
			return nil
		}

		// T2 (BILLING_SPEC.md §5): psp_clearing debited, client_receivable
		// credited.
		legs := []domain.LedgerLeg{
			{AccountType: domain.LedgerAccountPSPClearing, Currency: inv.Currency, Direction: domain.LedgerDirectionDebit, AmountMinor: inv.AmountMinor},
			{AccountType: domain.LedgerAccountClientReceivable, OwnerID: inv.ClientID, Currency: inv.Currency, Direction: domain.LedgerDirectionCredit, AmountMinor: inv.AmountMinor},
		}
		if err := h.ledgerRepo.PostTransaction(ctx, domain.LedgerTxPaymentSucceeded, "invoice", inv.ID, inv.Currency, legs); err != nil {
			return err
		}

		driverID := ""
		platformFeeMinor := inv.AmountMinor
		driverPayableMinor := int64(0)
		if inv.Type == domain.InvoiceTypeRideFare && inv.DriverID != nil {
			driverID = *inv.DriverID
			platformFeeMinor = inv.AmountMinor * h.commissionBps / 10000
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
		return h.outboxRepo.Insert(ctx, &domain.OutboxMessage{
			Topic: "payment.completed", EventType: "PaymentCompleted", Payload: payload,
		})
	})
	if err != nil {
		return err
	}
	if attempted {
		h.metrics.IncCounter(ctx, "myubergo.payments.attempted",
			attribute.String("outcome", "success"),
			attribute.String("provider", cmd.Provider),
		)
	}
	return nil
}
