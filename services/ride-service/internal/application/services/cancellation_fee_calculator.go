package services

import (
	"context"
	"ride-service/internal/domain"
)

// CancellationFeeCalculator computes the fee (if any) charged for cancelling
// a ride. Only ever called for rides that already had a driver assigned.
// Returns the fee in minor units plus its currency (D8: currency travels
// with the amount, never assumed).
type CancellationFeeCalculator interface {
	Calculate(ctx context.Context, ride *domain.Ride) (feeMinor int64, currency string, err error)
}
