package domain

import (
	"testing"
	"time"
)

func testConfig() ValidationConfig {
	return ValidationConfig{
		MaxAccuracyM:  100,
		MaxSpeedKmh:   200,
		MaxFutureSkew: 2 * time.Minute,
		MaxPastSkew:   10 * time.Minute,
	}
}

func TestValidatePing(t *testing.T) {
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	cfg := testConfig()

	// ~69m north of now's implicit previous point, used by the teleport cases below.
	limassol := RawPing{Lat: 34.707, Lon: 33.022, DeviceTs: now}

	tests := []struct {
		name     string
		raw      RawPing
		previous *Position
		now      time.Time
		want     RejectReason
	}{
		{
			name: "accepted, no previous position",
			raw:  RawPing{Lat: 34.707, Lon: 33.022, AccuracyM: 10, DeviceTs: now},
			now:  now,
			want: RejectNone,
		},
		{
			name: "invalid coordinate, lat out of range",
			raw:  RawPing{Lat: 91, Lon: 33.022, DeviceTs: now},
			now:  now,
			want: RejectInvalidCoordinate,
		},
		{
			name: "invalid coordinate, lon out of range",
			raw:  RawPing{Lat: 34.707, Lon: 181, DeviceTs: now},
			now:  now,
			want: RejectInvalidCoordinate,
		},
		{
			name: "poor accuracy rejected",
			raw:  RawPing{Lat: 34.707, Lon: 33.022, AccuracyM: 500, DeviceTs: now},
			now:  now,
			want: RejectPoorAccuracy,
		},
		{
			name: "accuracy at the boundary is accepted",
			raw:  RawPing{Lat: 34.707, Lon: 33.022, AccuracyM: 100, DeviceTs: now},
			now:  now,
			want: RejectNone,
		},
		{
			name: "clock skew too far in the future",
			raw:  RawPing{Lat: 34.707, Lon: 33.022, DeviceTs: now.Add(5 * time.Minute)},
			now:  now,
			want: RejectClockSkewFuture,
		},
		{
			name: "clock skew too far in the past",
			raw:  RawPing{Lat: 34.707, Lon: 33.022, DeviceTs: now.Add(-20 * time.Minute)},
			now:  now,
			want: RejectClockSkewPast,
		},
		{
			name:     "stale ordering, deviceTs not after previous",
			raw:      RawPing{Lat: 34.707, Lon: 33.022, DeviceTs: now},
			previous: &Position{Coordinate: Coordinate{Lat: 34.707, Lon: 33.022}, DeviceTs: now},
			now:      now,
			want:     RejectStaleOrdering,
		},
		{
			name:     "stale ordering, deviceTs before previous",
			raw:      RawPing{Lat: 34.707, Lon: 33.022, DeviceTs: now.Add(-time.Second)},
			previous: &Position{Coordinate: Coordinate{Lat: 34.707, Lon: 33.022}, DeviceTs: now},
			now:      now,
			want:     RejectStaleOrdering,
		},
		{
			// ~1.1km away in 5s implies ~800km/h — well past a 200km/h cap.
			name:     "teleport rejected",
			raw:      RawPing{Lat: limassol.Lat + 0.01, Lon: limassol.Lon, DeviceTs: now.Add(5 * time.Second)},
			previous: &Position{Coordinate: Coordinate{Lat: limassol.Lat, Lon: limassol.Lon}, DeviceTs: now},
			now:      now.Add(5 * time.Second),
			want:     RejectTeleport,
		},
		{
			// 50km/h for 5s is ~69m, well under the 200km/h cap.
			name:     "plausible movement accepted",
			raw:      RawPing{Lat: limassol.Lat + 0.00062, Lon: limassol.Lon, DeviceTs: now.Add(5 * time.Second)},
			previous: &Position{Coordinate: Coordinate{Lat: limassol.Lat, Lon: limassol.Lon}, DeviceTs: now},
			now:      now.Add(5 * time.Second),
			want:     RejectNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pos, reason := ValidatePing(tt.raw, tt.previous, cfg, tt.now)
			if reason != tt.want {
				t.Fatalf("got reason %q, want %q", reason, tt.want)
			}
			if reason == RejectNone {
				if pos.Coordinate.Lat != tt.raw.Lat || pos.Coordinate.Lon != tt.raw.Lon {
					t.Fatalf("accepted position coordinate mismatch: got %+v, raw was (%v,%v)", pos.Coordinate, tt.raw.Lat, tt.raw.Lon)
				}
				if !pos.ServerTs.Equal(tt.now) {
					t.Fatalf("ServerTs = %v, want %v", pos.ServerTs, tt.now)
				}
			}
		})
	}
}
