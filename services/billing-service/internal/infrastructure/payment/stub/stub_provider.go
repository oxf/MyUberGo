package stub

import (
	"billing-service/internal/application/services"
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"
)

// StubProvider picks its outcome deterministically from the payment
// method's provider ID prefix and honours the idempotency key: a replayed
// key returns the exact same cached result rather than "charging" again —
// this is what keeps a crashed-and-retried ChargeWorker tick safe.
type StubProvider struct {
	mu     sync.Mutex
	cached map[string]services.ChargeResult
}

func NewStubProvider() *StubProvider {
	return &StubProvider{cached: make(map[string]services.ChargeResult)}
}

func (p *StubProvider) Charge(ctx context.Context, req services.ChargeRequest) (services.ChargeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if cached, ok := p.cached[req.IdempotencyKey]; ok {
		return cached, nil
	}

	var result services.ChargeResult
	switch {
	case strings.HasPrefix(req.ProviderPaymentMethodID, "pm_stub_decline"):
		result = services.ChargeResult{Status: services.ChargeFailed, FailureCode: "card_declined", FailureMessage: "the card was declined"}
	case strings.HasPrefix(req.ProviderPaymentMethodID, "pm_stub_insufficient"):
		result = services.ChargeResult{Status: services.ChargeFailed, FailureCode: "insufficient_funds", FailureMessage: "insufficient funds"}
	default:
		result = services.ChargeResult{Status: services.ChargeSucceeded, ProviderIntentID: "pi_stub_" + randomHex()}
	}

	p.cached[req.IdempotencyKey] = result
	return result, nil
}

func randomHex() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "fallback"
	}
	return hex.EncodeToString(b)
}
