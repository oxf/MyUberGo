package cache

import (
	"context"
	"strings"
	"time"

	"location-service/internal/domain"

	"github.com/redis/go-redis/v9"
)

const (
	// pendingTTL: a one-sided pending key (requested-never-accepted or vice versa) self-cleans.
	pendingTTL = time.Hour
	// participantsTTL/activeRideTTL: belt-and-braces if CloseWindow is never called.
	participantsTTL = 24 * time.Hour
	activeRideTTL   = 24 * time.Hour
	// activeIndexKey is a SET of ride ids with a currently-open tracking
	// window — the fleet-wide index ListLivePositions uses to find which
	// clients currently have a position worth showing. No TTL of its own
	// (SADD has no per-member expiry); a stray stale id just costs one
	// harmless Participants lookup that returns ok=false, since CloseWindow
	// (the source of truth) reliably removes it on ride completion/cancellation.
	activeIndexKey = "loc:tracking:active"
)

func pendingClientKey(rideID string) string      { return "loc:ride:" + rideID + ":pending_client" }
func pendingDriverKey(rideID string) string      { return "loc:ride:" + rideID + ":pending_driver" }
func participantsKey(rideID string) string       { return "loc:ride:" + rideID + ":participants" }
func driverActiveRideKey(driverID string) string { return "loc:driver:" + driverID + ":activeRide" }
func clientActiveRideKey(clientID string) string { return "loc:client:" + clientID + ":activeRide" }

// TrackingRepository backs domain.TrackingRepository against Redis — each
// handler writes its pending key first, then checks the other's (proof: CLAUDE.md).
type TrackingRepository struct {
	rdb *redis.Client
}

func NewTrackingRepository(rdb *redis.Client) *TrackingRepository {
	return &TrackingRepository{rdb: rdb}
}

func (r *TrackingRepository) RecordRideRequested(ctx context.Context, rideID, clientID string) error {
	if err := r.rdb.Set(ctx, pendingClientKey(rideID), clientID, pendingTTL).Err(); err != nil {
		return err
	}

	driverPending, err := r.rdb.Get(ctx, pendingDriverKey(rideID)).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}

	driverID, acceptedAt := parseDriverPending(driverPending)
	return r.completeWindow(ctx, rideID, clientID, driverID, acceptedAt)
}

func (r *TrackingRepository) RecordRideAccepted(ctx context.Context, rideID, driverID string, acceptedAt time.Time) error {
	if err := r.rdb.Set(ctx, pendingDriverKey(rideID), encodeDriverPending(driverID, acceptedAt), pendingTTL).Err(); err != nil {
		return err
	}

	clientID, err := r.rdb.Get(ctx, pendingClientKey(rideID)).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}

	return r.completeWindow(ctx, rideID, clientID, driverID, acceptedAt)
}

// completeWindow writes participants + reverse-lookup keys and clears
// pending, all in one pipeline. Safe to run twice for the same ride.
func (r *TrackingRepository) completeWindow(ctx context.Context, rideID, clientID, driverID string, openedAt time.Time) error {
	pipe := r.rdb.Pipeline()
	pipe.HSet(ctx, participantsKey(rideID), map[string]any{
		"clientId": clientID,
		"driverId": driverID,
		"openedAt": openedAt.UTC().Format(time.RFC3339),
	})
	pipe.Expire(ctx, participantsKey(rideID), participantsTTL)
	pipe.Set(ctx, driverActiveRideKey(driverID), rideID, activeRideTTL)
	pipe.Set(ctx, clientActiveRideKey(clientID), rideID, activeRideTTL)
	pipe.SAdd(ctx, activeIndexKey, rideID)
	pipe.Del(ctx, pendingClientKey(rideID), pendingDriverKey(rideID))
	_, err := pipe.Exec(ctx)
	return err
}

// CloseWindow reads first (to know which reverse-lookup keys to clear), then
// deletes everything — missing/already-closed is a safe no-op, not an error.
func (r *TrackingRepository) CloseWindow(ctx context.Context, rideID string) error {
	m, err := r.rdb.HGetAll(ctx, participantsKey(rideID)).Result()
	if err != nil {
		return err
	}

	pipe := r.rdb.Pipeline()
	pipe.Del(ctx, participantsKey(rideID), pendingClientKey(rideID), pendingDriverKey(rideID))
	if driverID := m["driverId"]; driverID != "" {
		pipe.Del(ctx, driverActiveRideKey(driverID))
	}
	if clientID := m["clientId"]; clientID != "" {
		pipe.Del(ctx, clientActiveRideKey(clientID))
	}
	pipe.SRem(ctx, activeIndexKey, rideID)
	_, err = pipe.Exec(ctx)
	return err
}

// ActiveRideIDs returns every ride id with a currently-open tracking window.
func (r *TrackingRepository) ActiveRideIDs(ctx context.Context) ([]string, error) {
	return r.rdb.SMembers(ctx, activeIndexKey).Result()
}

func (r *TrackingRepository) Participants(ctx context.Context, rideID string) (domain.Participants, bool, error) {
	m, err := r.rdb.HGetAll(ctx, participantsKey(rideID)).Result()
	if err != nil {
		return domain.Participants{}, false, err
	}
	if len(m) == 0 {
		return domain.Participants{}, false, nil
	}

	openedAt, err := time.Parse(time.RFC3339, m["openedAt"])
	if err != nil {
		return domain.Participants{}, false, err
	}

	return domain.Participants{
		RideID:   rideID,
		ClientID: m["clientId"],
		DriverID: m["driverId"],
		OpenedAt: openedAt,
	}, true, nil
}

func (r *TrackingRepository) ActiveRideForDriver(ctx context.Context, driverID string) (string, error) {
	rideID, err := r.rdb.Get(ctx, driverActiveRideKey(driverID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return rideID, err
}

func (r *TrackingRepository) ActiveRideForClient(ctx context.Context, clientID string) (string, error) {
	rideID, err := r.rdb.Get(ctx, clientActiveRideKey(clientID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return rideID, err
}

func encodeDriverPending(driverID string, acceptedAt time.Time) string {
	return driverID + "|" + acceptedAt.UTC().Format(time.RFC3339)
}

// parseDriverPending is deliberately lenient: a malformed acceptedAt (should
// never happen) falls back to now rather than failing the whole write.
func parseDriverPending(raw string) (driverID string, acceptedAt time.Time) {
	driverID, tsPart, found := strings.Cut(raw, "|")
	if !found {
		return raw, time.Now().UTC()
	}
	ts, err := time.Parse(time.RFC3339, tsPart)
	if err != nil {
		return driverID, time.Now().UTC()
	}
	return driverID, ts
}
