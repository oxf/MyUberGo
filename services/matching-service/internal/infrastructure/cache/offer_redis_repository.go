package cache

import (
	"context"
	"strconv"
	"time"

	"matching-service/internal/domain"

	"github.com/redis/go-redis/v9"
)

const offeredSetTTL = time.Hour

type OfferRepository struct {
	rdb *redis.Client
}

func NewOfferRepository(rdb *redis.Client) *OfferRepository {
	return &OfferRepository{rdb: rdb}
}

func offeredKey(rideID string) string   { return "ride:" + rideID + ":offered_drivers" }
func acceptedKey(rideID string) string  { return "ride:" + rideID + ":accepted_by" }
func cancelledKey(rideID string) string { return "ride:" + rideID + ":cancelled" }
func offerKey(driverID string) string   { return "driver:" + driverID + ":current_offer" }
func rateKey(driverID string) string    { return "driver:" + driverID + ":notifications:minute" }
func pendingKey(rideID string) string   { return "pending_ride:" + rideID }

func (r *OfferRepository) OfferedDrivers(ctx context.Context, rideID string) (map[string]bool, error) {
	members, err := r.rdb.SMembers(ctx, offeredKey(rideID)).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(members))
	for _, m := range members {
		out[m] = true
	}
	return out, nil
}

func (r *OfferRepository) TryOffer(ctx context.Context, rideID, driverID string, ttl time.Duration) (bool, error) {
	ok, err := r.rdb.SetNX(ctx, offerKey(driverID), rideID, ttl).Result()
	if err != nil || !ok {
		return false, err
	}
	pipe := r.rdb.Pipeline()
	pipe.SAdd(ctx, offeredKey(rideID), driverID)
	pipe.Expire(ctx, offeredKey(rideID), offeredSetTTL)
	_, err = pipe.Exec(ctx)
	return true, err
}

func (r *OfferRepository) CurrentOffer(ctx context.Context, driverID string) (string, time.Time, error) {
	rideID, err := r.rdb.Get(ctx, offerKey(driverID)).Result()
	if err == redis.Nil {
		return "", time.Time{}, nil
	}
	if err != nil {
		return "", time.Time{}, err
	}
	ttl, err := r.rdb.PTTL(ctx, offerKey(driverID)).Result()
	if err != nil {
		return "", time.Time{}, err
	}
	return rideID, time.Now().Add(ttl), nil
}

func (r *OfferRepository) HasCurrentOffer(ctx context.Context, driverID string) (bool, error) {
	n, err := r.rdb.Exists(ctx, offerKey(driverID)).Result()
	return n > 0, err
}

func (r *OfferRepository) ClearCurrentOffer(ctx context.Context, driverID string) error {
	return r.rdb.Del(ctx, offerKey(driverID)).Err()
}

func (r *OfferRepository) TryAccept(ctx context.Context, rideID, driverID string, ttl time.Duration) (bool, error) {
	return r.rdb.SetNX(ctx, acceptedKey(rideID), driverID, ttl).Result()
}

func (r *OfferRepository) AcceptedBy(ctx context.Context, rideID string) (string, error) {
	v, err := r.rdb.Get(ctx, acceptedKey(rideID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

func (r *OfferRepository) IsCancelled(ctx context.Context, rideID string) (bool, error) {
	n, err := r.rdb.Exists(ctx, cancelledKey(rideID)).Result()
	return n > 0, err
}

// SetCancelled is the producer for cancelledKey, driven by the ride.cancelled
// consumer. No TTL — mirrors the ride:{id} hash, which also lives for as
// long as the ride is relevant.
func (r *OfferRepository) SetCancelled(ctx context.Context, rideID string) error {
	return r.rdb.Set(ctx, cancelledKey(rideID), "1", 0).Err()
}

func (r *OfferRepository) OfferCount(ctx context.Context, driverID string) (int64, error) {
	v, err := r.rdb.Get(ctx, rateKey(driverID)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

// IncrOfferCount bumps the per-driver notification counter and resets its
// TTL to 60s on every call. That makes this a *sliding* window (each new
// offer pushes the reset-time out) rather than a fixed one — an accepted
// simplification per the plan, not an oversight.
func (r *OfferRepository) IncrOfferCount(ctx context.Context, driverID string) error {
	pipe := r.rdb.Pipeline()
	incr := pipe.Incr(ctx, rateKey(driverID))
	pipe.Expire(ctx, rateKey(driverID), time.Minute)
	_, err := pipe.Exec(ctx)
	_ = incr
	return err
}

func (r *OfferRepository) SetPending(ctx context.Context, p domain.PendingRide) error {
	return r.rdb.HSet(ctx, pendingKey(p.RideID), map[string]any{
		"attempt":  p.Attempt,
		"deadline": p.Deadline.UTC().Format(time.RFC3339Nano),
	}).Err()
}

func (r *OfferRepository) DeletePending(ctx context.Context, rideID string) error {
	return r.rdb.Del(ctx, pendingKey(rideID)).Err()
}

func (r *OfferRepository) ListPending(ctx context.Context) ([]domain.PendingRide, error) {
	var out []domain.PendingRide
	iter := r.rdb.Scan(ctx, 0, "pending_ride:*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		m, err := r.rdb.HGetAll(ctx, key).Result()
		if err != nil || len(m) == 0 {
			continue
		}
		attempt, _ := strconv.Atoi(m["attempt"])
		deadline, err := time.Parse(time.RFC3339Nano, m["deadline"])
		if err != nil {
			continue
		}
		out = append(out, domain.PendingRide{
			RideID:   key[len("pending_ride:"):],
			Attempt:  attempt,
			Deadline: deadline,
		})
	}
	return out, iter.Err()
}
