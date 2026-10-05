package domain

import "testing"

func TestSimplifyRDP_StraightLineCollapses(t *testing.T) {
	// Points evenly spaced along a straight line north — none should survive
	// except the endpoints.
	track := []Coordinate{
		{Lat: 34.700, Lon: 33.000},
		{Lat: 34.701, Lon: 33.000},
		{Lat: 34.702, Lon: 33.000},
		{Lat: 34.703, Lon: 33.000},
		{Lat: 34.704, Lon: 33.000},
	}

	got := SimplifyRDP(track, 5)

	if len(got) != 2 {
		t.Fatalf("straight line: want 2 points, got %d: %+v", len(got), got)
	}
	if got[0] != track[0] || got[1] != track[len(track)-1] {
		t.Fatalf("straight line: endpoints not preserved: %+v", got)
	}
}

func TestSimplifyRDP_TurnIsPreserved(t *testing.T) {
	// A 90-degree turn: north for a while, then east. The corner point must
	// survive — this is the exact case every-Nth sampling loses
	// (LOCATION_SPEC.md §16 trap 8).
	track := []Coordinate{
		{Lat: 34.700, Lon: 33.000},
		{Lat: 34.701, Lon: 33.000},
		{Lat: 34.702, Lon: 33.000}, // corner
		{Lat: 34.702, Lon: 33.001},
		{Lat: 34.702, Lon: 33.002},
	}

	got := SimplifyRDP(track, 5)

	found := false
	for _, c := range got {
		if c == track[2] {
			found = true
		}
	}
	if !found {
		t.Fatalf("turn corner %+v was dropped by simplification: %+v", track[2], got)
	}
	if len(got) < 3 {
		t.Fatalf("want at least 3 points (both legs + corner), got %d: %+v", len(got), got)
	}
}

func TestSimplifyRDP_ShortTrackUnchanged(t *testing.T) {
	track := []Coordinate{{Lat: 1, Lon: 1}, {Lat: 2, Lon: 2}}
	got := SimplifyRDP(track, 5)
	if len(got) != 2 {
		t.Fatalf("want unchanged 2-point track, got %d", len(got))
	}
}
