package domain

import "strings"

// EncodePolyline implements Google's encoded polyline algorithm at
// precision 5 (the standard precision every mapping library decodes
// natively) — ~5-8 bytes/point vs ~40 for a JSON coordinate array
// (LOCATION_SPEC.md §2.5).
func EncodePolyline(track []Coordinate) string {
	var sb strings.Builder
	var prevLat, prevLon int64

	for _, c := range track {
		lat := round1e5(c.Lat)
		lon := round1e5(c.Lon)

		encodeSignedNumber(&sb, lat-prevLat)
		encodeSignedNumber(&sb, lon-prevLon)

		prevLat = lat
		prevLon = lon
	}
	return sb.String()
}

// DecodePolyline inverts EncodePolyline.
func DecodePolyline(encoded string) []Coordinate {
	var coords []Coordinate
	index, lat, lon := 0, int64(0), int64(0)

	for index < len(encoded) {
		lat += decodeSignedNumber(encoded, &index)
		lon += decodeSignedNumber(encoded, &index)
		coords = append(coords, Coordinate{Lat: float64(lat) / 1e5, Lon: float64(lon) / 1e5})
	}
	return coords
}

func round1e5(v float64) int64 {
	if v >= 0 {
		return int64(v*1e5 + 0.5)
	}
	return int64(v*1e5 - 0.5)
}

func encodeSignedNumber(sb *strings.Builder, n int64) {
	shifted := n << 1
	if n < 0 {
		shifted = ^shifted
	}
	encodeUnsignedNumber(sb, shifted)
}

func encodeUnsignedNumber(sb *strings.Builder, n int64) {
	for n >= 0x20 {
		sb.WriteByte(byte((0x20 | (n & 0x1f)) + 63))
		n >>= 5
	}
	sb.WriteByte(byte(n + 63))
}

func decodeSignedNumber(encoded string, index *int) int64 {
	result := int64(0)
	shift := uint(0)
	for {
		b := int64(encoded[*index]) - 63
		*index++
		result |= (b & 0x1f) << shift
		shift += 5
		if b < 0x20 {
			break
		}
	}
	if result&1 != 0 {
		return ^(result >> 1)
	}
	return result >> 1
}
