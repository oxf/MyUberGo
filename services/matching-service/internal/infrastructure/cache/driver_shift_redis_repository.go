package cache

import (
	"context"
	"fmt"

	"matching-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/kafka"
	"github.com/redis/go-redis/v9"
)

const onlineDriversKey = "drivers:online"

type DriverRepository struct {
	rdb *redis.Client
}

func NewDriverRepository(rdb *redis.Client) *DriverRepository {
	return &DriverRepository{rdb: rdb}
}

func (r *DriverRepository) UpsertDriver(ctx context.Context, event contracts.ShiftUpdatedEvent) error {
	key := fmt.Sprintf("driver:%s", event.DriverID)

	if err := r.rdb.HSet(ctx, key, map[string]any{
		"shiftID":   event.ShiftID,
		"status":    event.Status,
		"rating":    event.Rating,
		"updatedAt": event.UpdatedAt,
	}).Err(); err != nil {
		return err
	}

	// Only "Online" drivers are matchable; any other status (Ended, ...)
	// removes them from the pool.
	if event.Status == "Online" {
		return r.rdb.ZAdd(ctx, onlineDriversKey, redis.Z{Score: event.Rating, Member: event.DriverID}).Err()
	}
	return r.rdb.ZRem(ctx, onlineDriversKey, event.DriverID).Err()
}

func (r *DriverRepository) TopOnlineDrivers(ctx context.Context, limit int) ([]domain.Candidate, error) {
	zs, err := r.rdb.ZRevRangeWithScores(ctx, onlineDriversKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]domain.Candidate, 0, len(zs))
	for _, z := range zs {
		out = append(out, domain.Candidate{DriverID: z.Member.(string), Rating: z.Score})
	}
	return out, nil
}

func (r *DriverRepository) RemoveOnline(ctx context.Context, driverID string) error {
	return r.rdb.ZRem(ctx, onlineDriversKey, driverID).Err()
}
