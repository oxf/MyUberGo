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

// CounterpartyPositionResponse is the non-WS one-shot live-tracking fallback.
// Subject is "driver" or "client" — whichever side the caller is *not*.
type CounterpartyPositionResponse struct {
	Subject    string  `json:"subject"`
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
	AccuracyM  float64 `json:"accuracyM"`
	HeadingDeg float64 `json:"headingDeg"`
	SpeedMps   float64 `json:"speedMps"`
	DeviceTs   string  `json:"deviceTs"`
	ServerTs   string  `json:"serverTs"`
}

// LocationUpdateFrame is a client->server WS frame — one ping per frame, no
// rideId/subject: both are resolved server-side, never taken from the body.
type LocationUpdateFrame struct {
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
	AccuracyM  float64 `json:"accuracyM"`
	HeadingDeg float64 `json:"headingDeg"`
	SpeedMps   float64 `json:"speedMps"`
	DeviceTs   string  `json:"deviceTs"`
}

// CounterpartyLocationUpdate is a server->client WS frame on GET /ws — the
// push counterpart of CounterpartyPositionResponse's one-shot pull.
type CounterpartyLocationUpdate struct {
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

// LiveDriverPositionDto is one driver's current position for the admin
// live-positions map. No accuracy field — this is a display feature, not a
// telemetry viewer; heading/speed are kept since they're cheap and useful
// for a marker's rotation/tooltip.
type LiveDriverPositionDto struct {
	DriverId   string  `json:"driverId"`
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
	HeadingDeg float64 `json:"headingDeg"`
	SpeedMps   float64 `json:"speedMps"`
	ServerTs   string  `json:"serverTs"`
}

// LiveClientPositionDto is one client's current position — only present
// while their ride's tracking window is open (LOCATION_SPEC.md §17 decision
// 5), hence the accompanying RideId so the map can label it usefully.
type LiveClientPositionDto struct {
	ClientId string  `json:"clientId"`
	RideId   string  `json:"rideId"`
	Lat      float64 `json:"lat"`
	Lon      float64 `json:"lon"`
	ServerTs string  `json:"serverTs"`
}

// LivePositionsResponse answers GET /api/location/positions — the admin
// fleet-wide live-positions map snapshot. Not paged: a plain wrapped array,
// matching NearbyDriversResponse's shape in this same file.
type LivePositionsResponse struct {
	Drivers []LiveDriverPositionDto `json:"drivers"`
	Clients []LiveClientPositionDto `json:"clients"`
}
