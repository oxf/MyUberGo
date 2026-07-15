package domain

import (
	"context"

	contracts "github.com/oxf/MyUber/contracts/kafka"
)

type RideRepository interface {
	SaveRide(ctx context.Context,
		event contracts.RideRequestedEvent) error
}

type DriverRepository interface {
	CreateDriver(ctx context.Context, event contracts.ShiftUpdatedEvent) error
}
