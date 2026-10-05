package domain

import (
	"testing"
	"time"
)

func TestBuildSummaryFallback_EmptyTrack(t *testing.T) {
	_, err := BuildSummaryFallback("ride-1", "client-1", "driver-1", time.Now(), time.Now(), nil, 5)
	if err != ErrEmptyTrack {
		t.Fatalf("want ErrEmptyTrack, got %v", err)
	}
}

func TestBuildSummaryFallback_SinglePointDoesNotPanic(t *testing.T) {
	started := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	ended := started.Add(5 * time.Minute)
	positions := []Position{
		{Coordinate: Coordinate{Lat: 34.7, Lon: 33.0}, DeviceTs: started, ServerTs: started},
	}

	summary, err := BuildSummaryFallback("ride-1", "client-1", "driver-1", started, ended, positions, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if summary.DistanceM != 0 {
		t.Fatalf("single point: want distance 0, got %d", summary.DistanceM)
	}
	if summary.Source != SourceSimplified {
		t.Fatalf("want SourceSimplified, got %v", summary.Source)
	}
	if summary.PointCount != 1 {
		t.Fatalf("want point count 1, got %d", summary.PointCount)
	}
}

func TestBuildSummaryFallback_DistanceIsIntegerMetres(t *testing.T) {
	started := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	ended := started.Add(10 * time.Minute)
	positions := []Position{
		{Coordinate: Coordinate{Lat: 34.700, Lon: 33.000}, DeviceTs: started},
		{Coordinate: Coordinate{Lat: 34.701, Lon: 33.000}, DeviceTs: started.Add(time.Minute)},
		{Coordinate: Coordinate{Lat: 34.702, Lon: 33.000}, DeviceTs: started.Add(2 * time.Minute)},
	}

	summary, err := BuildSummaryFallback("ride-1", "client-1", "driver-1", started, ended, positions, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ~0.001 degrees latitude apart, twice — roughly 222m total. The exact
	// value matters less than the type: never a float, always a rounded int64.
	if summary.DistanceM <= 0 {
		t.Fatalf("want positive distance, got %d", summary.DistanceM)
	}
	if summary.DurationS != 600 {
		t.Fatalf("want duration 600s, got %d", summary.DurationS)
	}
	if summary.Start != positions[0].Coordinate {
		t.Fatalf("start coordinate mismatch: got %+v", summary.Start)
	}
	if summary.End != positions[len(positions)-1].Coordinate {
		t.Fatalf("end coordinate mismatch: got %+v", summary.End)
	}
}
