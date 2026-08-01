package stripe

import (
	"billing-service/internal/application/services"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	stripego "github.com/stripe/stripe-go/v84"
	"github.com/stripe/stripe-go/v84/webhook"
)

// StripeProvider implements PaymentProvider, CustomerVault, and
// ProviderEventParser against real Stripe, test mode only — see
// NewStripeProvider. Attaching a payment method server-side by its raw
// token (no Stripe.js/Elements anywhere in this repo) is only legal in test
// mode, which is this project's permanent constraint: sandbox only, now and
// in every later phase.
type StripeProvider struct {
	client        *stripego.Client
	webhookSecret string
	timeout       time.Duration
}

// NewStripeProvider refuses anything but a test-mode secret key — this
// keeps "sandbox only, permanently" enforced in code, not just in docs.
// timeout bounds every call to Stripe; since these calls happen with no
// database transaction open (see ChargeWorker), a hung request only blocks
// the one in-flight claim, never a DB connection or a row lock.
func NewStripeProvider(secretKey, webhookSecret string, timeout time.Duration) (*StripeProvider, error) {
	if !strings.HasPrefix(secretKey, "sk_test_") {
		return nil, fmt.Errorf("stripe: refusing to start with a non-test-mode secret key (must start with sk_test_)")
	}
	if webhookSecret == "" {
		return nil, fmt.Errorf("stripe: STRIPE_WEBHOOK_SECRET is required when PAYMENT_PROVIDER=stripe")
	}
	return &StripeProvider{client: stripego.NewClient(secretKey), webhookSecret: webhookSecret, timeout: timeout}, nil
}

func (p *StripeProvider) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, p.timeout)
}

// EnsureCustomer is idempotent via a deterministic key derived from our own
// client id — a retried call (e.g. after a crash between the Stripe call
// and persisting the customer row) returns the same Stripe customer instead
// of creating a duplicate.
func (p *StripeProvider) EnsureCustomer(ctx context.Context, clientID string) (string, error) {
	params := &stripego.CustomerCreateParams{
		Metadata: map[string]string{"client_id": clientID},
	}
	params.SetIdempotencyKey("customer:" + clientID)

	ctx, cancel := p.withTimeout(ctx)
	defer cancel()

	cust, err := p.client.V1Customers.Create(ctx, params)
	if err != nil {
		return "", err
	}
	return cust.ID, nil
}

func (p *StripeProvider) AttachPaymentMethod(ctx context.Context, providerCustomerID, providerPaymentMethodID string) (services.PaymentMethodDetails, error) {
	ctx, cancel := p.withTimeout(ctx)
	defer cancel()

	pm, err := p.client.V1PaymentMethods.Attach(ctx, providerPaymentMethodID, &stripego.PaymentMethodAttachParams{
		Customer: stripego.String(providerCustomerID),
	})
	if err != nil {
		return services.PaymentMethodDetails{}, err
	}
	if pm.Card == nil {
		return services.PaymentMethodDetails{}, nil
	}
	return services.PaymentMethodDetails{
		Brand:    string(pm.Card.Brand),
		Last4:    pm.Card.Last4,
		ExpMonth: int(pm.Card.ExpMonth),
		ExpYear:  int(pm.Card.ExpYear),
	}, nil
}

// Charge creates an off-session, immediately-confirmed PaymentIntent — the
// standard "merchant charges a saved card later" pattern. The existing
// invoice:{id}:attempt:{n} idempotency key drops straight into Stripe's own
// Idempotency-Key header, so a crashed-and-retried attempt (same key)
// cannot double-charge even against a real provider.
//
// A card decline on a synchronously-confirmed PaymentIntent comes back from
// Stripe as an API-level *stripe.Error (an HTTP error), not as a 200 response
// with a "failed" status — mapStripeOutcome handles both shapes so callers
// never have to know which one occurred.
func (p *StripeProvider) Charge(ctx context.Context, req services.ChargeRequest) (services.ChargeResult, error) {
	params := &stripego.PaymentIntentCreateParams{
		Amount:             stripego.Int64(req.AmountMinor),
		Currency:           stripego.String(strings.ToLower(req.Currency)),
		Customer:           stripego.String(req.ProviderCustomerID),
		PaymentMethod:      stripego.String(req.ProviderPaymentMethodID),
		Confirm:            stripego.Bool(true),
		OffSession:         stripego.Bool(true),
		PaymentMethodTypes: []*string{stripego.String("card")},
	}
	params.SetIdempotencyKey(req.IdempotencyKey)

	ctx, cancel := p.withTimeout(ctx)
	defer cancel()

	pi, err := p.client.V1PaymentIntents.Create(ctx, params)
	if err == nil {
		return mapStripeOutcome(pi, nil), nil
	}

	var stripeErr *stripego.Error
	if errors.As(err, &stripeErr) {
		return mapStripeOutcome(stripeErr.PaymentIntent, stripeErr), nil
	}
	// Not a recognizable payment outcome — a transport/auth/timeout
	// failure. Surface as a real error so ChargeWorker's generic
	// "provider_error" catch-all handles it instead of silently
	// misreporting a card decline.
	return services.ChargeResult{}, err
}

// ParseEvent verifies the webhook signature and, for the payment_intent
// lifecycle events this service acts on, unmarshals the intent embedded in
// the event and reuses mapStripeOutcome — the exact same mapping the
// synchronous Charge() path uses, since a webhook and a Charge response
// describe the same kind of outcome. IgnoreAPIVersionMismatch is required:
// since stripe-go v73, ConstructEvent errors by default when the Stripe
// account's configured API version differs from this library's, which is
// unrelated to whether the signature itself is valid.
func (p *StripeProvider) ParseEvent(payload []byte, signatureHeader string) (services.ProviderEvent, bool, error) {
	event, err := webhook.ConstructEventWithOptions(payload, signatureHeader, p.webhookSecret, webhook.ConstructEventOptions{
		IgnoreAPIVersionMismatch: true,
	})
	if err != nil {
		return services.ProviderEvent{}, false, err
	}

	switch event.Type {
	case stripego.EventTypePaymentIntentSucceeded,
		stripego.EventTypePaymentIntentPaymentFailed,
		stripego.EventTypePaymentIntentProcessing,
		stripego.EventTypePaymentIntentCanceled,
		stripego.EventTypePaymentIntentRequiresAction:
		// handled below
	default:
		// A real, signature-valid event this service doesn't act on (e.g.
		// customer.updated) — still worth a fast ack, not an error.
		return services.ProviderEvent{EventID: event.ID, EventType: string(event.Type), APIVersion: event.APIVersion}, false, nil
	}

	var pi stripego.PaymentIntent
	if err := json.Unmarshal(event.Data.Raw, &pi); err != nil {
		return services.ProviderEvent{}, false, err
	}

	return services.ProviderEvent{
		EventID: event.ID, EventType: string(event.Type), APIVersion: event.APIVersion,
		Result: mapStripeOutcome(&pi, pi.LastPaymentError),
	}, true, nil
}

// mapStripeOutcome is deliberately pure (no I/O) so it can be unit-tested
// without a network call — it is the only part of this adapter genuinely
// worth testing in isolation. Shared by both Charge (a synchronous answer)
// and ParseEvent (an asynchronous webhook), which is exactly why the two
// resolution paths can never disagree about what a given Stripe outcome
// means.
func mapStripeOutcome(pi *stripego.PaymentIntent, stripeErr *stripego.Error) services.ChargeResult {
	intentID := ""
	if pi != nil {
		intentID = pi.ID
	}

	if stripeErr == nil {
		switch pi.Status {
		case stripego.PaymentIntentStatusSucceeded:
			return services.ChargeResult{Status: services.ChargeSucceeded, ProviderIntentID: intentID}
		case stripego.PaymentIntentStatusProcessing:
			return services.ChargeResult{Status: services.ChargeProcessing, ProviderIntentID: intentID}
		case stripego.PaymentIntentStatusRequiresAction:
			return services.ChargeResult{
				Status: services.ChargeRequiresAction, ProviderIntentID: intentID,
				FailureCode: string(pi.Status), FailureMessage: "customer authentication required",
			}
		case stripego.PaymentIntentStatusCanceled:
			// Distinct from RequiresAction: a canceled intent will never
			// resolve on its own — it must count as a failed attempt
			// toward the retry budget, same as a decline.
			return services.ChargeResult{
				Status: services.ChargeFailed, ProviderIntentID: intentID,
				FailureCode: "canceled", FailureMessage: "payment intent was canceled",
			}
		default:
			// requires_payment_method / requires_confirmation /
			// requires_capture — not expected from our own Charge() call or
			// from the 5 webhook event types this service acts on, but
			// treated as failed rather than silently succeeding.
			return services.ChargeResult{
				Status: services.ChargeFailed, ProviderIntentID: intentID,
				FailureCode: string(pi.Status), FailureMessage: "payment intent did not succeed",
			}
		}
	}

	if stripeErr.Code == stripego.ErrorCodeAuthenticationRequired {
		return services.ChargeResult{
			Status: services.ChargeRequiresAction, ProviderIntentID: intentID,
			FailureCode: string(stripeErr.Code), FailureMessage: stripeErr.Msg,
		}
	}

	// The decline code (e.g. insufficient_funds) is the specific reason a
	// card was declined — more useful than the generic card_declined error
	// code that wraps it, and it's what the e2e-test fixtures assert on.
	failureCode := string(stripeErr.Code)
	if stripeErr.DeclineCode != "" {
		failureCode = string(stripeErr.DeclineCode)
	}
	return services.ChargeResult{
		Status: services.ChargeFailed, ProviderIntentID: intentID,
		FailureCode: failureCode, FailureMessage: stripeErr.Msg,
	}
}
