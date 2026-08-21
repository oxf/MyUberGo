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

	pipe := r.rdb.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"shiftID":   event.ShiftID,
		"status":    event.Status,
		"rating":    event.Rating,
		"userId":    event.UserID,
		"updatedAt": event.UpdatedAt,
	})
	// Only "Online" drivers are matchable; any other status (Ended, ...)
	// removes them from the pool.
	if event.Status == "Online" {
		pipe.ZAdd(ctx, onlineDriversKey, redis.Z{Score: event.Rating, Member: event.DriverID})
	} else {
		pipe.ZRem(ctx, onlineDriversKey, event.DriverID)
	}
	_, err := pipe.Exec(ctx)
	return err
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

func (r *DriverRepository) Rating(ctx context.Context, driverID string) (float64, error) {
	score, err := r.rdb.ZScore(ctx, onlineDriversKey, driverID).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return score, err
}

func (r *DriverRepository) AddOnline(ctx context.Context, driverID string, rating float64) error {
	return r.rdb.ZAdd(ctx, onlineDriversKey, redis.Z{Score: rating, Member: driverID}).Err()
}

func (r *DriverRepository) GetUserID(ctx context.Context, driverID string) (string, error) {
	userID, err := r.rdb.HGet(ctx, fmt.Sprintf("driver:%s", driverID), "userId").Result()
	if err == redis.Nil {
		return "", nil
	}
	return userID, err
}

// OnlineRatings is ZMSCORE against drivers:online for exactly the given ids — one round trip,
// same "0 = not present" convention as Rating (ratings are never actually 0 in practice).
func (r *DriverRepository) OnlineRatings(ctx context.Context, driverIDs []string) (map[string]float64, error) {
	out := make(map[string]float64, len(driverIDs))
	if len(driverIDs) == 0 {
		return out, nil
	}
	scores, err := r.rdb.ZMScore(ctx, onlineDriversKey, driverIDs...).Result()
	if err != nil {
		return nil, err
	}
	for i, id := range driverIDs {
		out[id] = scores[i]
	}
	return out, nil
}
