package stripe

import (
	"billing-service/internal/application/services"
	"testing"

	stripego "github.com/stripe/stripe-go/v84"
)

func TestMapStripeOutcome_Succeeded(t *testing.T) {
	pi := &stripego.PaymentIntent{ID: "pi_123", Status: stripego.PaymentIntentStatusSucceeded}

	result := mapStripeOutcome(pi, nil)

	if result.Status != services.ChargeSucceeded {
		t.Fatalf("status = %v, want %v", result.Status, services.ChargeSucceeded)
	}
	if result.ProviderIntentID != "pi_123" {
		t.Fatalf("ProviderIntentID = %q, want pi_123", result.ProviderIntentID)
	}
}

func TestMapStripeOutcome_Processing(t *testing.T) {
	pi := &stripego.PaymentIntent{ID: "pi_456", Status: stripego.PaymentIntentStatusProcessing}

	result := mapStripeOutcome(pi, nil)

	if result.Status != services.ChargeProcessing {
		t.Fatalf("status = %v, want %v", result.Status, services.ChargeProcessing)
	}
}

func TestMapStripeOutcome_CardDeclined_UsesDeclineCode(t *testing.T) {
	pi := &stripego.PaymentIntent{ID: "pi_789", Status: stripego.PaymentIntentStatusRequiresPaymentMethod}
	stripeErr := &stripego.Error{
		Code:          stripego.ErrorCodeCardDeclined,
		DeclineCode:   stripego.DeclineCodeGenericDecline,
		Msg:           "Your card was declined.",
		PaymentIntent: pi,
	}

	result := mapStripeOutcome(pi, stripeErr)

	if result.Status != services.ChargeFailed {
		t.Fatalf("status = %v, want %v", result.Status, services.ChargeFailed)
	}
	// The decline code (the specific reason) should win over the generic
	// card_declined error code that wraps it — this is what e2e-test
	// fixtures and the retry/uncollectible path key off of.
	if result.FailureCode != string(stripego.DeclineCodeGenericDecline) {
		t.Fatalf("FailureCode = %q, want %q", result.FailureCode, stripego.DeclineCodeGenericDecline)
	}
	if result.ProviderIntentID != "pi_789" {
		t.Fatalf("ProviderIntentID = %q, want pi_789", result.ProviderIntentID)
	}
}

func TestMapStripeOutcome_InsufficientFunds(t *testing.T) {
	stripeErr := &stripego.Error{
		Code:        stripego.ErrorCodeCardDeclined,
		DeclineCode: stripego.DeclineCodeInsufficientFunds,
		Msg:         "Your card has insufficient funds.",
	}

	result := mapStripeOutcome(nil, stripeErr)

	if result.Status != services.ChargeFailed {
		t.Fatalf("status = %v, want %v", result.Status, services.ChargeFailed)
	}
	if result.FailureCode != string(stripego.DeclineCodeInsufficientFunds) {
		t.Fatalf("FailureCode = %q, want %q", result.FailureCode, stripego.DeclineCodeInsufficientFunds)
	}
}

func TestMapStripeOutcome_AuthenticationRequired(t *testing.T) {
	pi := &stripego.PaymentIntent{ID: "pi_3ds", Status: stripego.PaymentIntentStatusRequiresAction}
	stripeErr := &stripego.Error{
		Code:          stripego.ErrorCodeAuthenticationRequired,
		Msg:           "The customer must authenticate this payment.",
		PaymentIntent: pi,
	}

	result := mapStripeOutcome(pi, stripeErr)

	if result.Status != services.ChargeRequiresAction {
		t.Fatalf("status = %v, want %v", result.Status, services.ChargeRequiresAction)
	}
	if result.ProviderIntentID != "pi_3ds" {
		t.Fatalf("ProviderIntentID = %q, want pi_3ds", result.ProviderIntentID)
	}
}

func TestMapStripeOutcome_Canceled(t *testing.T) {
	pi := &stripego.PaymentIntent{ID: "pi_canceled", Status: stripego.PaymentIntentStatusCanceled}

	result := mapStripeOutcome(pi, nil)

	if result.Status != services.ChargeFailed {
		t.Fatalf("status = %v, want %v (a canceled intent will never resolve on its own)", result.Status, services.ChargeFailed)
	}
	if result.FailureCode != "canceled" {
		t.Fatalf("FailureCode = %q, want %q", result.FailureCode, "canceled")
	}
}

func TestMapStripeOutcome_RequiresAction_NoAccompanyingError(t *testing.T) {
	// The webhook path (payment_intent.requires_action) delivers the
	// intent's status directly, with no *stripego.Error at all — distinct
	// from the synchronous Charge() path, where 3DS surfaces as an error.
	pi := &stripego.PaymentIntent{ID: "pi_webhook_3ds", Status: stripego.PaymentIntentStatusRequiresAction}

	result := mapStripeOutcome(pi, nil)

	if result.Status != services.ChargeRequiresAction {
		t.Fatalf("status = %v, want %v", result.Status, services.ChargeRequiresAction)
	}
}

func TestMapStripeOutcome_NoDeclineCode_FallsBackToErrorCode(t *testing.T) {
	stripeErr := &stripego.Error{
		Code: stripego.ErrorCodeExpiredCard,
		Msg:  "Your card has expired.",
	}

	result := mapStripeOutcome(nil, stripeErr)

	if result.Status != services.ChargeFailed {
		t.Fatalf("status = %v, want %v", result.Status, services.ChargeFailed)
	}
	if result.FailureCode != string(stripego.ErrorCodeExpiredCard) {
		t.Fatalf("FailureCode = %q, want %q", result.FailureCode, stripego.ErrorCodeExpiredCard)
	}
}
