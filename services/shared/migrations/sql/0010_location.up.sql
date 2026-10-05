CREATE SCHEMA IF NOT EXISTS location;

-- One row per completed ride, built by location-service once the raw track
-- has been simplified/map-matched. Not a system of record for money — the
-- quoted price at ride request remains binding (see billing.invoice's
-- actual_distance_m/actual_duration_s, recorded not re-priced).
CREATE TABLE IF NOT EXISTS location.ride_summary (
    ride_id UUID PRIMARY KEY,
    client_id UUID NOT NULL,
    driver_id UUID NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NOT NULL,
    start_lat DOUBLE PRECISION NOT NULL,
    start_lon DOUBLE PRECISION NOT NULL,
    end_lat DOUBLE PRECISION NOT NULL,
    end_lon DOUBLE PRECISION NOT NULL,
    -- Encoded polyline, precision 5 — see domain.EncodePolyline.
    polyline TEXT NOT NULL,
    -- Integer metres, never a float — same discipline as the repo's
    -- money-minor-units convention.
    distance_m BIGINT NOT NULL,
    duration_s INTEGER NOT NULL,
    -- Raw pings that fed this summary, for auditability.
    point_count INTEGER NOT NULL,
    -- 'MapMatched' once the Slice-4 Geoapify adapter exists; until then every
    -- summary is 'Simplified' (Ramer-Douglas-Peucker + Haversine fallback).
    source TEXT NOT NULL CHECK (source IN ('MapMatched', 'Simplified')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS location.outbox_message (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    retries INTEGER DEFAULT 0,
    -- See ride.outbox_message.claimed_until for why this exists.
    claimed_until TIMESTAMPTZ,
    -- See ride.outbox_message.trace_context for why this exists.
    trace_context JSONB
);

CREATE INDEX idx_location_outbox_processed ON location.outbox_message(processed);

-- See 0009_outbox_index for why NOW() can't go in a static index predicate.
CREATE INDEX idx_location_outbox_unprocessed ON location.outbox_message(created_at) WHERE processed = false;
