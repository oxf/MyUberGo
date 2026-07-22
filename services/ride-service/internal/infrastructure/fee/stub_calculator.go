package fee

import (
	"context"
	"ride-service/internal/application/services"
	"ride-service/internal/domain"
)

// StubCalculator is a placeholder CancellationFeeCalculator: it always
// returns zero. Real fee logic needs a Billing service that doesn't exist
// yet — this keeps the RideCancelledEvent's Fee field wired up so real
// logic can be dropped in later without touching the command/handler.
type StubCalculator struct{}

func NewStubCalculator() services.CancellationFeeCalculator {
	return &StubCalculator{}
}

func (s *StubCalculator) Calculate(ctx context.Context, ride *domain.Ride) (float64, error) {
	return 0, nil
}
