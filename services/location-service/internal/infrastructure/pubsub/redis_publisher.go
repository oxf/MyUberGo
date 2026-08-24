package pubsub

import (
	"context"
	"encoding/json"
	"time"

	"location-service/internal/domain"

	"github.com/redis/go-redis/v9"
)

func rideChannel(rideID string) string { return "loc:ride:" + rideID + ":positions" }

// wireMessage is the JSON envelope published to loc:ride:{id}:positions —
// kept separate from domain.PositionUpdate so the wire format doesn't leak
// Go-internal field naming and can version independently of the domain type.
type wireMessage struct {
	RideID     string  `json:"rideId"`
	Subject    string  `json:"subject"`
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
	AccuracyM  float64 `json:"accuracyM"`
	HeadingDeg float64 `json:"headingDeg"`
	SpeedMps   float64 `json:"speedMps"`
	DeviceTs   string  `json:"deviceTs"`
	ServerTs   string  `json:"serverTs"`
}

// RedisPublisher implements domain.PositionPublisher — the write side of the
// cross-instance fan-out described in LOCATION_SPEC.md §7.3. It has no
// awareness of who (if anyone) is subscribed; that's Dispatcher's job
// (Stage 4). Publishing to a channel with zero subscribers is a no-op in
// Redis, so this stage is safely "publish with no one listening yet."
type RedisPublisher struct {
	rdb *redis.Client
}

func NewRedisPublisher(rdb *redis.Client) *RedisPublisher {
	return &RedisPublisher{rdb: rdb}
}

func (p *RedisPublisher) Publish(ctx context.Context, update domain.PositionUpdate) error {
	payload, err := json.Marshal(encodeWireMessage(update))
	if err != nil {
		return err
	}
	return p.rdb.Publish(ctx, rideChannel(update.RideID), payload).Err()
}

func encodeWireMessage(update domain.PositionUpdate) wireMessage {
	return wireMessage{
		RideID:     update.RideID,
		Subject:    string(update.Subject),
		Lat:        update.Position.Coordinate.Lat,
		Lon:        update.Position.Coordinate.Lon,
		AccuracyM:  update.Position.AccuracyM,
		HeadingDeg: update.Position.HeadingDeg,
		SpeedMps:   update.Position.SpeedMps,
		DeviceTs:   update.Position.DeviceTs.UTC().Format(time.RFC3339),
		ServerTs:   update.Position.ServerTs.UTC().Format(time.RFC3339),
	}
}

// decodeWireMessage is encodeWireMessage's inverse — used by Dispatcher to
// turn a raw Pub/Sub payload back into a domain.PositionUpdate.
func decodeWireMessage(raw []byte) (domain.PositionUpdate, error) {
	var w wireMessage
	if err := json.Unmarshal(raw, &w); err != nil {
		return domain.PositionUpdate{}, err
	}
	deviceTs, err := time.Parse(time.RFC3339, w.DeviceTs)
	if err != nil {
		return domain.PositionUpdate{}, err
	}
	serverTs, err := time.Parse(time.RFC3339, w.ServerTs)
	if err != nil {
		return domain.PositionUpdate{}, err
	}
	coord, err := domain.NewCoordinate(w.Lat, w.Lon)
	if err != nil {
		return domain.PositionUpdate{}, err
	}
	return domain.PositionUpdate{
		RideID:  w.RideID,
		Subject: domain.SubjectType(w.Subject),
		Position: domain.Position{
			Coordinate: coord,
			AccuracyM:  w.AccuracyM,
			HeadingDeg: w.HeadingDeg,
			SpeedMps:   w.SpeedMps,
			DeviceTs:   deviceTs,
			ServerTs:   serverTs,
		},
	}, nil
}
