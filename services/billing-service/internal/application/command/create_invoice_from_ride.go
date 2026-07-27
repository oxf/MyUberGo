package command

import (
	"billing-service/internal/application/services"
	"billing-service/internal/common/decorator"
	"billing-service/internal/domain"
	"context"
	"errors"
	"time"

	"github.com/sirupsen/logrus"
)

// CreateInvoiceFromRide is shared by both the ride.completed and
// ride.cancelled(with fee) pipelines (BILLING_SPEC.md §8: "same pipeline")
// — only Type/DriverID/AmountMinor differ between the two call sites.
type CreateInvoiceFromRide struct {
	RideID      string
	ClientID    string
	DriverID    *string // nil for pre-match cancellations
	Type        string  // domain.InvoiceTypeRideFare | InvoiceTypeCancellationFee
	AmountMinor int64
	Currency    string
}

type CreateInvoiceFromRideHandler struct {
	invoiceRepo   domain.InvoiceRepository
	ledgerRepo    domain.LedgerRepository
	transaction   services.TransactionManager
	commissionBps int64
	logger        *logrus.Entry
}

func NewCreateInvoiceFromRideHandler(
	invoiceRepo domain.InvoiceRepository,
	ledgerRepo domain.LedgerRepository,
	transaction services.TransactionManager,
	commissionBps int64,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[CreateInvoiceFromRide] {

	handler := &CreateInvoiceFromRideHandler{
		invoiceRepo:   invoiceRepo,
		ledgerRepo:    ledgerRepo,
		transaction:   transaction,
		commissionBps: commissionBps,
		logger:        logger,
	}

	return decorator.ApplyCommandDecoratorsNoResult[CreateInvoiceFromRide](
		handler,
		logger,
		metricsClient,
	)
}

// Handle inserts the invoice (+ one line) and posts T1 (invoice_opened) in
// one transaction. A (ride_id, type) unique-violation — a redelivered
// Kafka event — is treated as a no-op success: the message still gets
// acked, but no second invoice/ledger transaction is created. Do not
// pre-check with a SELECT first; that would race against a concurrent
// redelivery instead of relying on the DB constraint.
//
// The duplicate check must happen OUTSIDE WithinTransaction, after it
// returns. A unique-violation aborts the Postgres transaction server-side;
// returning nil from the inner closure would make WithinTransaction call
// tx.Commit() on that already-aborted transaction, which itself errors
// ("current transaction is aborted") and turns the intended no-op into a
// real failure. Returning the error instead makes WithinTransaction roll
// back (always safe, even on an aborted tx) — only after that do we
// translate ErrDuplicateInvoice into a nil, successful result.
func (h *CreateInvoiceFromRideHandler) Handle(ctx context.Context, cmd CreateInvoiceFromRide) error {
	err := h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		lineKind := domain.InvoiceLineKindBaseFare
		if cmd.Type == domain.InvoiceTypeCancellationFee {
			lineKind = domain.InvoiceLineKindCancellationFee
		}

		nextAttemptAt := time.Now().UTC().Format(time.RFC3339)
		inv := &domain.Invoice{
			RideID:        cmd.RideID,
			ClientID:      cmd.ClientID,
			DriverID:      cmd.DriverID,
			Type:          cmd.Type,
			Status:        domain.InvoiceStatusOpen,
			AmountMinor:   cmd.AmountMinor,
			Currency:      cmd.Currency,
			NextAttemptAt: &nextAttemptAt,
			Lines: []domain.InvoiceLine{{
				Kind:        lineKind,
				AmountMinor: cmd.AmountMinor,
				Currency:    cmd.Currency,
			}},
		}

		id, err := h.invoiceRepo.Create(ctx, inv)
		if err != nil {
			return err
		}

		legs := h.buildT1Legs(cmd)
		return h.ledgerRepo.PostTransaction(ctx, domain.LedgerTxInvoiceOpened, "invoice", id, cmd.Currency, legs)
	})

	if errors.Is(err, domain.ErrDuplicateInvoice) {
		h.logger.WithField("ride_id", cmd.RideID).Info("invoice already exists for this ride/type, treating as no-op")
		return nil
	}
	return err
}

// buildT1Legs implements the T1 posting from BILLING_SPEC.md §5. A
// cancellation fee is 100% platform_revenue with no driver_payable leg
// (D7); a ride fare splits into platform_revenue (commission) and
// driver_payable (remainder) — truncating integer division on the fee,
// remainder to the driver, so fee+payable always sums exactly to amount.
func (h *CreateInvoiceFromRideHandler) buildT1Legs(cmd CreateInvoiceFromRide) []domain.LedgerLeg {
	legs := []domain.LedgerLeg{
		{
			AccountType: domain.LedgerAccountClientReceivable,
			OwnerID:     cmd.ClientID,
			Currency:    cmd.Currency,
			Direction:   domain.LedgerDirectionDebit,
			AmountMinor: cmd.AmountMinor,
		},
	}

	if cmd.Type == domain.InvoiceTypeCancellationFee {
		legs = append(legs, domain.LedgerLeg{
			AccountType: domain.LedgerAccountPlatformRevenue,
			Currency:    cmd.Currency,
			Direction:   domain.LedgerDirectionCredit,
			AmountMinor: cmd.AmountMinor,
		})
		return legs
	}

	feeMinor := cmd.AmountMinor * h.commissionBps / 10000
	payableMinor := cmd.AmountMinor - feeMinor

	legs = append(legs,
		domain.LedgerLeg{
			AccountType: domain.LedgerAccountPlatformRevenue,
			Currency:    cmd.Currency,
			Direction:   domain.LedgerDirectionCredit,
			AmountMinor: feeMinor,
		},
		domain.LedgerLeg{
			AccountType: domain.LedgerAccountDriverPayable,
			OwnerID:     *cmd.DriverID,
			Currency:    cmd.Currency,
			Direction:   domain.LedgerDirectionCredit,
			AmountMinor: payableMinor,
		},
	)
	return legs
}
