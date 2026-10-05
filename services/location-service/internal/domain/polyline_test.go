package domain

import "testing"

// TestEncodePolyline_KnownFixture is Google's own published example:
// https://developers.google.com/maps/documentation/utilities/polylinealgorithm
func TestEncodePolyline_KnownFixture(t *testing.T) {
	track := []Coordinate{
		{Lat: 38.5, Lon: -120.2},
		{Lat: 40.7, Lon: -120.95},
		{Lat: 43.252, Lon: -126.453},
	}
	want := "_p~iF~ps|U_ulLnnqC_mqNvxq`@"

	got := EncodePolyline(track)
	if got != want {
		t.Fatalf("EncodePolyline() = %q, want %q", got, want)
	}
}

func TestPolyline_RoundTrip(t *testing.T) {
	track := []Coordinate{
		{Lat: 34.70742, Lon: 33.02218},
		{Lat: 34.70850, Lon: 33.02350},
		{Lat: 34.71020, Lon: 33.02600},
	}

	encoded := EncodePolyline(track)
	decoded := DecodePolyline(encoded)

	if len(decoded) != len(track) {
		t.Fatalf("round trip: got %d points, want %d", len(decoded), len(track))
	}
	for i := range track {
		if diffAbs(decoded[i].Lat-track[i].Lat) > 1e-5 || diffAbs(decoded[i].Lon-track[i].Lon) > 1e-5 {
			t.Fatalf("round trip point %d: got %+v, want %+v", i, decoded[i], track[i])
		}
	}
}

func diffAbs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
