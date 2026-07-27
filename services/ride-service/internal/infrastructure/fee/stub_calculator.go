package fee

import (
	"context"
	"ride-service/internal/application/services"
	"ride-service/internal/domain"
)

// flatCancellationFeeMinor is a placeholder fee charged when a client
// cancels a ride that already had a driver assigned. Real fee logic needs a
// proper policy (distance-to-pickup, time since match, etc.) — this just
// keeps the ride.cancelled -> billing.invoice path exercisable instead of
// permanently dead (Calculate is only ever called when ride.DriverID != nil,
// so "a driver was assigned" is exactly this call site).
const flatCancellationFeeMinor = 300

type StubCalculator struct{}

func NewStubCalculator() services.CancellationFeeCalculator {
	return &StubCalculator{}
}

func (s *StubCalculator) Calculate(ctx context.Context, ride *domain.Ride) (int64, string, error) {
	return flatCancellationFeeMinor, ride.Currency, nil
}
