package handler

import (
	app "billing-service/internal/application"
	"billing-service/internal/application/command"
	"billing-service/internal/application/services"
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/sirupsen/logrus"
)

// WebhookHandler applies verified Stripe webhook events through the exact
// same Finalize* commands ChargeWorker's synchronous path uses — this is
// the reuse the claim/charge/finalize split in Phase 1 was built for.
// Registered by cmd/main.go only when PAYMENT_PROVIDER=stripe.
type WebhookHandler struct {
	app          app.Application
	parser       services.ProviderEventParser
	pspEventRepo domain.PspEventRepository
	paymentRepo  domain.PaymentRepository
	invoiceRepo  domain.InvoiceRepository
	logger       *logrus.Entry
}

func NewWebhookHandler(
	application app.Application,
	parser services.ProviderEventParser,
	pspEventRepo domain.PspEventRepository,
	paymentRepo domain.PaymentRepository,
	invoiceRepo domain.InvoiceRepository,
	logger *logrus.Entry,
) *WebhookHandler {
	return &WebhookHandler{
		app: application, parser: parser, pspEventRepo: pspEventRepo,
		paymentRepo: paymentRepo, invoiceRepo: invoiceRepo, logger: logger,
	}
}

// StripeWebhook verifies the signature over the RAW body (must be read
// before any decode — that's the entire point of a signature), then applies
// the event. Response codes matter: Stripe retries on non-2xx, so a storage
// failure 500s (retry is correct) while an invalid signature or an event
// type we don't act on 400s/200s fast (retrying either wouldn't help).
func (h *WebhookHandler) StripeWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}

	event, ok, err := h.parser.ParseEvent(body, r.Header.Get("Stripe-Signature"))
	if err != nil {
		h.logger.WithError(err).Warn("webhook: signature verification failed")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if !ok {
		// A real, signature-valid event we don't act on (e.g.
		// customer.updated) — ack fast, nothing to retry.
		w.WriteHeader(http.StatusOK)
		return
	}

	if err := h.apply(r.Context(), event, body); err != nil {
		h.logger.WithError(err).WithField("event_id", event.EventID).Error("webhook: apply failed")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// apply is the psp_event insert-then-dispatch-then-mark-processed flow. A
// redelivery hits the (id) unique-violation; ProcessedAt distinguishes a
// true no-op (already fully handled) from a previously-interrupted delivery
// (safe to retry, since every effect dispatch touches is itself guarded).
func (h *WebhookHandler) apply(ctx context.Context, event services.ProviderEvent, rawPayload []byte) error {
	insertErr := h.pspEventRepo.Insert(ctx, &domain.PspEvent{
		ID: event.EventID, Type: event.EventType, APIVersion: event.APIVersion, Payload: rawPayload,
	})
	if insertErr != nil {
		if !errors.Is(insertErr, domain.ErrDuplicatePspEvent) {
			return insertErr
		}
		existing, err := h.pspEventRepo.GetByID(ctx, event.EventID)
		if err != nil {
			return err
		}
		if existing.ProcessedAt != nil {
			h.logger.WithField("event_id", event.EventID).Info("webhook: event already processed, skipping")
			return nil
		}
		// Fall through: retry the dispatch for an interrupted delivery.
	}

	if err := h.dispatch(ctx, event); err != nil {
		return err
	}
	return h.pspEventRepo.MarkProcessed(ctx, event.EventID)
}

func (h *WebhookHandler) dispatch(ctx context.Context, event services.ProviderEvent) error {
	payment, err := h.paymentRepo.GetByProviderIntentID(ctx, event.Result.ProviderIntentID)
	if err != nil {
		if errors.Is(err, commonerrors.ErrNotFound) {
			// Nothing in our DB matches this PaymentIntent. Not reachable
			// in normal operation — ChargeWorker always creates and
			// confirms the payment row before Stripe could ever emit an
			// event about it — so this is logged rather than retried.
			h.logger.WithField("provider_intent_id", event.Result.ProviderIntentID).Warn(
				"webhook: no matching payment row found")
			return nil
		}
		return err
	}

	switch event.Result.Status {
	case services.ChargeSucceeded:
		return h.app.Commands.FinalizeChargeSucceeded.Handle(ctx, command.FinalizeChargeSucceeded{
			PaymentID: payment.ID, InvoiceID: payment.InvoiceID, ProviderIntentID: event.Result.ProviderIntentID,
		})
	case services.ChargeProcessing:
		if _, err := h.paymentRepo.MarkProcessing(ctx, payment.ID, event.Result.ProviderIntentID); err != nil {
			return err
		}
		return h.invoiceRepo.SetNextAttemptAt(ctx, payment.InvoiceID, nil)
	default: // Failed / RequiresAction
		return h.app.Commands.FinalizeChargeFailed.Handle(ctx, command.FinalizeChargeFailed{
			PaymentID: payment.ID, InvoiceID: payment.InvoiceID,
			FailureCode: event.Result.FailureCode, FailureMessage: event.Result.FailureMessage,
		})
	}
}
