package domain

import "time"

// RawPing is one unvalidated position sample as received from the wire — no
// driverId field: identity is resolved from the authenticated userId instead.
type RawPing struct {
	Lat        float64
	Lon        float64
	AccuracyM  float64
	HeadingDeg float64
	SpeedMps   float64
	DeviceTs   time.Time
}

// RejectReason names why a ping failed validation — loggable/counted per
// reason rather than a bare bool ("count by reason, don't just drop").
type RejectReason string

const (
	RejectNone              RejectReason = ""
	RejectInvalidCoordinate RejectReason = "invalid_coordinate"
	RejectPoorAccuracy      RejectReason = "poor_accuracy"
	RejectClockSkewFuture   RejectReason = "clock_skew_future"
	RejectClockSkewPast     RejectReason = "clock_skew_past"
	RejectTeleport          RejectReason = "teleport"
	RejectStaleOrdering     RejectReason = "stale_ordering"
)

// ValidationConfig bounds accepted pings — see LOCATION_SPEC.md §5.5 and §15
// for the env vars that populate this.
type ValidationConfig struct {
	MaxAccuracyM  float64
	MaxSpeedKmh   float64
	MaxFutureSkew time.Duration
	MaxPastSkew   time.Duration
}

// ValidatePing checks static bounds plus, if previous is non-nil, an
// ordering/teleport guard by DeviceTs — last-write-wins, no Lua script.
func ValidatePing(raw RawPing, previous *Position, cfg ValidationConfig, now time.Time) (Position, RejectReason) {
	coord, err := NewCoordinate(raw.Lat, raw.Lon)
	if err != nil {
		return Position{}, RejectInvalidCoordinate
	}
	if raw.AccuracyM > cfg.MaxAccuracyM {
		return Position{}, RejectPoorAccuracy
	}
	if raw.DeviceTs.After(now.Add(cfg.MaxFutureSkew)) {
		return Position{}, RejectClockSkewFuture
	}
	if raw.DeviceTs.Before(now.Add(-cfg.MaxPastSkew)) {
		return Position{}, RejectClockSkewPast
	}

	if previous != nil {
		if !raw.DeviceTs.After(previous.DeviceTs) {
			return Position{}, RejectStaleOrdering
		}
		if dt := raw.DeviceTs.Sub(previous.DeviceTs).Seconds(); dt > 0 {
			speedKmh := HaversineMeters(previous.Coordinate, coord) / dt * 3.6
			if speedKmh > cfg.MaxSpeedKmh {
				return Position{}, RejectTeleport
			}
		}
	}

	return Position{
		Coordinate: coord,
		AccuracyM:  raw.AccuracyM,
		HeadingDeg: raw.HeadingDeg,
		SpeedMps:   raw.SpeedMps,
		DeviceTs:   raw.DeviceTs,
		ServerTs:   now,
	}, RejectNone
}
