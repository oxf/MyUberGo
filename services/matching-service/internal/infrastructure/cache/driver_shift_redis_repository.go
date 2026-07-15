package cache

import (
	"context"
	"fmt"

	contracts "github.com/oxf/MyUber/contracts/kafka"
	"github.com/redis/go-redis/v9"
)

type DriverRepository struct {
	rdb *redis.Client
}

func NewDriverRepository(rdb *redis.Client) *DriverRepository {
	return &DriverRepository{rdb: rdb}
}

func (r *DriverRepository) CreateDriver(
	ctx context.Context,
	event contracts.ShiftUpdatedEvent,
) error {

	key := fmt.Sprintf("driver:%s", event.DriverID)

	values := map[string]any{
		"shiftID":   event.ShiftID,
		"status":    event.Status,
		"updatedAt": event.UpdatedAt,
	}

	return r.rdb.HSet(ctx, key, values).Err()
}
