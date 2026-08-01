package stripe

import (
	"billing-service/internal/application/services"
	"encoding/json"
	"testing"

	stripego "github.com/stripe/stripe-go/v84"
)

const testWebhookSecret = "whsec_test_secret"

// signedEventPayload builds a minimal, realistic Stripe Event JSON body
// (id/type/api_version/data.object) and signs it with
// stripe.GenerateTestSignedPayload — no network, no Stripe CLI, exactly
// what a real webhook delivery looks like on the wire.
func signedEventPayload(t *testing.T, eventID, eventType string, object map[string]any) (payload []byte, header string) {
	t.Helper()
	body := map[string]any{
		"id":          eventID,
		"object":      "event",
		"api_version": "2024-06-20",
		"type":        eventType,
		"data":        map[string]any{"object": object},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal event body: %v", err)
	}
	signed := stripego.GenerateTestSignedPayload(&stripego.UnsignedPayload{Payload: raw, Secret: testWebhookSecret})
	return signed.Payload, signed.Header
}

func newTestStripeProvider(t *testing.T) *StripeProvider {
	t.Helper()
	p, err := NewStripeProvider("sk_test_dummy", testWebhookSecret, 0)
	if err != nil {
		t.Fatalf("NewStripeProvider: %v", err)
	}
	return p
}

func TestParseEvent_ValidSignature_Succeeded(t *testing.T) {
	p := newTestStripeProvider(t)
	payload, header := signedEventPayload(t, "evt_succeeded_1", "payment_intent.succeeded", map[string]any{
		"id": "pi_test_1", "object": "payment_intent", "status": "succeeded",
	})

	event, ok, err := p.ParseEvent(payload, header)
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for a payment_intent.succeeded event")
	}
	if event.EventID != "evt_succeeded_1" {
		t.Fatalf("EventID = %q, want evt_succeeded_1", event.EventID)
	}
	if event.Result.Status != services.ChargeSucceeded {
		t.Fatalf("Result.Status = %v, want %v", event.Result.Status, services.ChargeSucceeded)
	}
	if event.Result.ProviderIntentID != "pi_test_1" {
		t.Fatalf("Result.ProviderIntentID = %q, want pi_test_1", event.Result.ProviderIntentID)
	}
}

func TestParseEvent_PaymentFailed_ExtractsDeclineCode(t *testing.T) {
	p := newTestStripeProvider(t)
	payload, header := signedEventPayload(t, "evt_failed_1", "payment_intent.payment_failed", map[string]any{
		"id": "pi_test_2", "object": "payment_intent", "status": "requires_payment_method",
		"last_payment_error": map[string]any{
			"type": "card_error", "code": "card_declined", "decline_code": "insufficient_funds",
			"message": "Your card has insufficient funds.",
		},
	})

	event, ok, err := p.ParseEvent(payload, header)
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for a payment_intent.payment_failed event")
	}
	if event.Result.Status != services.ChargeFailed {
		t.Fatalf("Result.Status = %v, want %v", event.Result.Status, services.ChargeFailed)
	}
	if event.Result.FailureCode != "insufficient_funds" {
		t.Fatalf("Result.FailureCode = %q, want insufficient_funds", event.Result.FailureCode)
	}
}

func TestParseEvent_InvalidSignature_Errors(t *testing.T) {
	p := newTestStripeProvider(t)
	payload, _ := signedEventPayload(t, "evt_bad_sig", "payment_intent.succeeded", map[string]any{
		"id": "pi_test_3", "object": "payment_intent", "status": "succeeded",
	})

	// A signature computed with the WRONG secret must fail verification —
	// this is the entire security boundary of the webhook endpoint.
	wrongSigned := stripego.GenerateTestSignedPayload(&stripego.UnsignedPayload{Payload: payload, Secret: "whsec_wrong_secret"})

	_, _, err := p.ParseEvent(payload, wrongSigned.Header)
	if err == nil {
		t.Fatal("expected an error for a signature computed with the wrong secret, got nil")
	}
}

func TestParseEvent_UnknownEventType_NotOk(t *testing.T) {
	p := newTestStripeProvider(t)
	payload, header := signedEventPayload(t, "evt_unrelated", "customer.updated", map[string]any{
		"id": "cus_test_1", "object": "customer",
	})

	event, ok, err := p.ParseEvent(payload, header)
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for an event type this service doesn't act on")
	}
	if event.EventID != "evt_unrelated" {
		t.Fatalf("EventID = %q, want evt_unrelated (still identifiable for logging even when ok=false)", event.EventID)
	}
}

func TestParseEvent_Replay_ProducesIdenticalResult(t *testing.T) {
	// ParseEvent itself has no dedup state — Stripe's at-least-once
	// redelivery is handled by the caller (the psp_event inbox), not here.
	// Parsing the same signed payload twice must produce the identical
	// normalized event both times.
	p := newTestStripeProvider(t)
	payload, header := signedEventPayload(t, "evt_replay_1", "payment_intent.succeeded", map[string]any{
		"id": "pi_test_4", "object": "payment_intent", "status": "succeeded",
	})

	first, ok1, err1 := p.ParseEvent(payload, header)
	second, ok2, err2 := p.ParseEvent(payload, header)

	if err1 != nil || err2 != nil {
		t.Fatalf("ParseEvent errors: %v / %v", err1, err2)
	}
	if !ok1 || !ok2 {
		t.Fatal("expected ok=true on both parses")
	}
	if first != second {
		t.Fatalf("replayed parse produced a different result: %+v vs %+v", first, second)
	}
}
