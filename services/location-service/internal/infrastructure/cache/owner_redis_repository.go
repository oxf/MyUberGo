package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// ownerTTL ages a driver out of the mapping if they never open another
// shift; a redelivered/refreshed shift.updated event just resets it.
const ownerTTL = 7 * 24 * time.Hour

func ownerKey(driverID string) string    { return "loc:driver:" + driverID + ":owner" }
func userDriverKey(userID string) string { return "loc:user:" + userID + ":driver" }

// OwnerRepository caches the driver<->user mapping in dedicated loc:-prefixed
// keys, distinct from matching-service's unprefixed driver:{id} hash.
type OwnerRepository struct {
	rdb *redis.Client
}

func NewOwnerRepository(rdb *redis.Client) *OwnerRepository {
	return &OwnerRepository{rdb: rdb}
}

// SetOwner writes both directions of the mapping in one pipeline —
// idempotent, safe against shift.updated redelivery.
func (r *OwnerRepository) SetOwner(ctx context.Context, driverID, userID string) error {
	pipe := r.rdb.Pipeline()
	pipe.Set(ctx, ownerKey(driverID), userID, ownerTTL)
	pipe.Set(ctx, userDriverKey(userID), driverID, ownerTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// DriverIDForUser returns ("", nil) if the user has no cached driver mapping
// yet — see domain.OwnerRepository's doc comment; not an error case.
func (r *OwnerRepository) DriverIDForUser(ctx context.Context, userID string) (string, error) {
	driverID, err := r.rdb.Get(ctx, userDriverKey(userID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return driverID, err
}
