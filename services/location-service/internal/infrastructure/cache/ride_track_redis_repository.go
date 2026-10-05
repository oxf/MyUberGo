package cache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"location-service/internal/domain"

	"github.com/redis/go-redis/v9"
)

const (
	// trackStreamMaxLen caps the buffer at ~5000 entries (approximate
	// trimming) so a slow/down archiver never grows the stream unbounded —
	// old pings are dropped, not queued (LOCATION_SPEC.md §6.2).
	trackStreamMaxLen = 5000
	// trackStreamTTL: belt-and-braces if a ride never gets archived/trimmed.
	trackStreamTTL = 24 * time.Hour
)

func trackStreamKey(rideID string) string     { return "loc:ride:" + rideID + ":track" }
func trackArchivedIDKey(rideID string) string { return "loc:ride:" + rideID + ":archived_id" }

// RideTrackRepository backs domain.RideTrackRepository with a capped Redis
// Stream: ordered, range-readable by id, trims itself.
type RideTrackRepository struct {
	rdb *redis.Client
}

func NewRideTrackRepository(rdb *redis.Client) *RideTrackRepository {
	return &RideTrackRepository{rdb: rdb}
}

func (r *RideTrackRepository) Append(ctx context.Context, rideID string, subject domain.SubjectType, pos domain.Position) error {
	key := trackStreamKey(rideID)
	pipe := r.rdb.Pipeline()
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		MaxLen: trackStreamMaxLen,
		Approx: true,
		Values: trackEntryValues(subject, pos),
	})
	pipe.Expire(ctx, key, trackStreamTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// Range reads a ride's track starting strictly after afterID. Any entry
// that fails to decode is skipped rather than failing the whole read — a
// partial summary beats no summary.
func (r *RideTrackRepository) Range(ctx context.Context, rideID string, afterID string) ([]domain.TrackEntry, error) {
	start := "-"
	if afterID != "" {
		start = "(" + afterID
	}

	msgs, err := r.rdb.XRange(ctx, trackStreamKey(rideID), start, "+").Result()
	if err != nil {
		return nil, err
	}

	entries := make([]domain.TrackEntry, 0, len(msgs))
	for _, m := range msgs {
		entry, err := decodeTrackEntry(m)
		if err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (r *RideTrackRepository) Trim(ctx context.Context, rideID string) error {
	return r.rdb.Del(ctx, trackStreamKey(rideID), trackArchivedIDKey(rideID)).Err()
}

func (r *RideTrackRepository) LastArchivedID(ctx context.Context, rideID string) (string, error) {
	id, err := r.rdb.Get(ctx, trackArchivedIDKey(rideID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return id, err
}

func (r *RideTrackRepository) SetLastArchivedID(ctx context.Context, rideID string, streamID string) error {
	return r.rdb.Set(ctx, trackArchivedIDKey(rideID), streamID, trackStreamTTL).Err()
}

// trackEntryValues formats a position as a stream entry body. Deliberately
// not position_codec.go's shared positionHash: this stream is ordered by
// device timestamp, so entries need sub-second resolution to disambiguate
// pings inside the same second, which RFC3339Nano gives (RFC3339 doesn't).
func trackEntryValues(subject domain.SubjectType, pos domain.Position) map[string]any {
	return map[string]any{
		"subject":    string(subject),
		"lat":        pos.Coordinate.Lat,
		"lon":        pos.Coordinate.Lon,
		"accuracyM":  pos.AccuracyM,
		"headingDeg": pos.HeadingDeg,
		"speedMps":   pos.SpeedMps,
		"deviceTs":   pos.DeviceTs.UTC().Format(time.RFC3339Nano),
		"serverTs":   pos.ServerTs.UTC().Format(time.RFC3339Nano),
	}
}

func decodeTrackEntry(msg redis.XMessage) (domain.TrackEntry, error) {
	values := make(map[string]string, len(msg.Values))
	for k, v := range msg.Values {
		s, ok := v.(string)
		if !ok {
			return domain.TrackEntry{}, fmt.Errorf("track entry field %q not a string", k)
		}
		values[k] = s
	}

	lat, err := strconv.ParseFloat(values["lat"], 64)
	if err != nil {
		return domain.TrackEntry{}, err
	}
	lon, err := strconv.ParseFloat(values["lon"], 64)
	if err != nil {
		return domain.TrackEntry{}, err
	}
	accuracyM, err := strconv.ParseFloat(values["accuracyM"], 64)
	if err != nil {
		return domain.TrackEntry{}, err
	}
	headingDeg, err := strconv.ParseFloat(values["headingDeg"], 64)
	if err != nil {
		return domain.TrackEntry{}, err
	}
	speedMps, err := strconv.ParseFloat(values["speedMps"], 64)
	if err != nil {
		return domain.TrackEntry{}, err
	}
	// time.Parse tolerates a fractional-second field beyond what the layout
	// specifies, so RFC3339 parses RFC3339Nano-formatted input unchanged.
	deviceTs, err := time.Parse(time.RFC3339, values["deviceTs"])
	if err != nil {
		return domain.TrackEntry{}, err
	}
	serverTs, err := time.Parse(time.RFC3339, values["serverTs"])
	if err != nil {
		return domain.TrackEntry{}, err
	}

	coord, err := domain.NewCoordinate(lat, lon)
	if err != nil {
		return domain.TrackEntry{}, err
	}

	return domain.TrackEntry{
		StreamID: msg.ID,
		Subject:  domain.SubjectType(values["subject"]),
		Position: domain.Position{
			Coordinate: coord,
			AccuracyM:  accuracyM,
			HeadingDeg: headingDeg,
			SpeedMps:   speedMps,
			DeviceTs:   deviceTs,
			ServerTs:   serverTs,
		},
	}, nil
}
