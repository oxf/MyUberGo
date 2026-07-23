package command

import (
	"context"
	commonerrors "ride-service/internal/common/errors"
	"ride-service/internal/domain"
	"time"
)

// fakeLifecycleRideRepo backs start_ride_test.go, complete_ride_test.go, and
// cancel_ride_test.go. fakeOutboxRepo/fakeTx (defined in create_ride_test.go)
// are reused as-is since they're generic.
type fakeLifecycleRideRepo struct {
	domain.RideRepository
	ride           *domain.Ride
	startedCalls   []string
	completedCalls []string
	cancelledCalls []string
}

func (f *fakeLifecycleRideRepo) GetRideForUpdate(ctx context.Context, id string) (*domain.Ride, error) {
	if f.ride == nil {
		return nil, commonerrors.ErrNotFound
	}
	return f.ride, nil
}

func (f *fakeLifecycleRideRepo) MarkRideStarted(ctx context.Context, id string, startedAt time.Time) error {
	f.startedCalls = append(f.startedCalls, id)
	return nil
}

func (f *fakeLifecycleRideRepo) CompleteRide(ctx context.Context, id string, finishedAt time.Time) error {
	f.completedCalls = append(f.completedCalls, id)
	return nil
}

func (f *fakeLifecycleRideRepo) CancelRide(ctx context.Context, id, reason string) error {
	f.cancelledCalls = append(f.cancelledCalls, id)
	return nil
}
