package services

// ProviderEvent is a verified, normalized webhook notification — reuses
// ChargeResult's vocabulary (Status/ProviderIntentID/FailureCode/
// FailureMessage) rather than inventing a second one, since a webhook and a
// synchronous Charge response describe the exact same kind of outcome.
type ProviderEvent struct {
	EventID    string
	EventType  string
	APIVersion string
	Result     ChargeResult
}

// ProviderEventParser verifies a webhook's signature and, if the event is
// one this service acts on (a payment-intent lifecycle event), normalizes
// it. ok=false means a real event this service doesn't act on (e.g. a
// customer.* event) — still worth a fast 200 ack, not an error.
type ProviderEventParser interface {
	ParseEvent(payload []byte, signatureHeader string) (event ProviderEvent, ok bool, err error)
}
