package cache

import (
	"strconv"
	"time"

	"location-service/internal/domain"
)

// positionHash encodes a domain.Position into the field map both
// DriverLocationRepository and ClientLocationRepository write to a Redis hash.
func positionHash(pos domain.Position) map[string]any {
	return map[string]any{
		"lat":        pos.Coordinate.Lat,
		"lon":        pos.Coordinate.Lon,
		"accuracyM":  pos.AccuracyM,
		"headingDeg": pos.HeadingDeg,
		"speedMps":   pos.SpeedMps,
		"deviceTs":   pos.DeviceTs.UTC().Format(time.RFC3339),
		"serverTs":   pos.ServerTs.UTC().Format(time.RFC3339),
	}
}

// decodePositionHash is positionHash's inverse — the caller is responsible
// for treating an empty map as "not found" before calling this.
func decodePositionHash(m map[string]string) (domain.Position, error) {
	lat, err := strconv.ParseFloat(m["lat"], 64)
	if err != nil {
		return domain.Position{}, err
	}
	lon, err := strconv.ParseFloat(m["lon"], 64)
	if err != nil {
		return domain.Position{}, err
	}
	accuracyM, err := strconv.ParseFloat(m["accuracyM"], 64)
	if err != nil {
		return domain.Position{}, err
	}
	headingDeg, err := strconv.ParseFloat(m["headingDeg"], 64)
	if err != nil {
		return domain.Position{}, err
	}
	speedMps, err := strconv.ParseFloat(m["speedMps"], 64)
	if err != nil {
		return domain.Position{}, err
	}
	deviceTs, err := time.Parse(time.RFC3339, m["deviceTs"])
	if err != nil {
		return domain.Position{}, err
	}
	serverTs, err := time.Parse(time.RFC3339, m["serverTs"])
	if err != nil {
		return domain.Position{}, err
	}

	coord, err := domain.NewCoordinate(lat, lon)
	if err != nil {
		return domain.Position{}, err
	}

	return domain.Position{
		Coordinate: coord,
		AccuracyM:  accuracyM,
		HeadingDeg: headingDeg,
		SpeedMps:   speedMps,
		DeviceTs:   deviceTs,
		ServerTs:   serverTs,
	}, nil
}
