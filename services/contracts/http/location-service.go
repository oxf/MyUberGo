package contracts

// LocationPingDto is one position sample. No driverId/clientId field — ingest resolves
// caller identity server-side from Kong's X-User-Id, since a self-asserted id is spoofable.
type LocationPingDto struct {
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
	AccuracyM  float64 `json:"accuracyM"`
	HeadingDeg float64 `json:"headingDeg"`
	SpeedMps   float64 `json:"speedMps"`
	// DeviceTs is RFC3339, the device's fix-capture time — distinct from arrival
	// time, since a batch may be replayed late after a connectivity gap.
	DeviceTs string `json:"deviceTs"`
}

type LocationBatchRequest struct {
	Pings []LocationPingDto `json:"pings"`
}

type LocationBatchResponse struct {
	Accepted int `json:"accepted"`
	Rejected int `json:"rejected"`
}

// NearbyDriverDto is one geographic candidate — no availability/ranking data.
// matching-service intersects with drivers:online and ranks; Location only answers "where".
type NearbyDriverDto struct {
	DriverId  string `json:"driverId"`
	DistanceM int64  `json:"distanceM"`
}

type NearbyDriversResponse struct {
	Candidates []NearbyDriverDto `json:"candidates"`
}
