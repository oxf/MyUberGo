package services

import (
	"context"
	"ride-service/internal/domain"
)

// CancellationFeeCalculator computes the fee (if any) charged for cancelling
// a ride. Only ever called for rides that already had a driver assigned.
type CancellationFeeCalculator interface {
	Calculate(ctx context.Context, ride *domain.Ride) (float64, error)
}
