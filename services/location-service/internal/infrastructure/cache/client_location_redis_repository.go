package cache

import (
	"context"
	"time"

	"location-service/internal/domain"

	"github.com/redis/go-redis/v9"
)

// clientLocationTTL mirrors locationTTL but is named independently — no
// StalenessWorker-style read-path filter needs to stay in sync with it.
const clientLocationTTL = 5 * time.Minute

func clientKey(clientID string) string { return "loc:client:" + clientID }

// ClientLocationRepository persists live client (rider) positions — no geo
// index, since clients are never discovery candidates.
type ClientLocationRepository struct {
	rdb *redis.Client
}

func NewClientLocationRepository(rdb *redis.Client) *ClientLocationRepository {
	return &ClientLocationRepository{rdb: rdb}
}

// LastPosition reads loc:client:{id} in one round trip. A missing key (never
// pinged, or expired) returns (nil, nil) — not an error case.
func (r *ClientLocationRepository) LastPosition(ctx context.Context, clientID string) (*domain.Position, error) {
	m, err := r.rdb.HGetAll(ctx, clientKey(clientID)).Result()
	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}
	pos, err := decodePositionHash(m)
	if err != nil {
		return nil, err
	}
	return &pos, nil
}

// UpsertPosition writes the detail hash in one pipeline — no geo/lastseen
// index writes, unlike DriverLocationRepository.
func (r *ClientLocationRepository) UpsertPosition(ctx context.Context, clientID string, pos domain.Position) error {
	pipe := r.rdb.Pipeline()
	pipe.HSet(ctx, clientKey(clientID), positionHash(pos))
	pipe.Expire(ctx, clientKey(clientID), clientLocationTTL)
	_, err := pipe.Exec(ctx)
	return err
}
