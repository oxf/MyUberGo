CREATE SCHEMA IF NOT EXISTS ride;

CREATE TABLE IF NOT EXISTS ride.tariff (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT UNIQUE NOT NULL,
    base_fare_minor BIGINT NOT NULL DEFAULT 0,
    price_per_km_minor BIGINT NOT NULL,
    price_per_min_minor BIGINT NOT NULL DEFAULT 0,
    currency CHAR(3) NOT NULL DEFAULT 'EUR'
);

INSERT INTO ride.tariff (name, base_fare_minor, price_per_km_minor, price_per_min_minor, currency) VALUES
('Standard', 300, 100, 20, 'EUR'),
('Comfort', 400, 150, 30, 'EUR'),
('Premium', 600, 200, 40, 'EUR'),
('Luxury', 1000, 300, 50, 'EUR'),
('Standard USD', 300, 100, 20, 'USD');

CREATE TABLE IF NOT EXISTS ride.ride (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id UUID NOT NULL REFERENCES auth.client(id),
    driver_id UUID,
    status TEXT NOT NULL DEFAULT 'Requested' CHECK (status IN ('Requested', 'Matched', 'InProgress', 'Completed', 'Cancelled')),
    pickup_lat DOUBLE PRECISION NOT NULL,
    pickup_lng DOUBLE PRECISION NOT NULL,
    pickup_address TEXT NOT NULL DEFAULT '',
    dest_lat DOUBLE PRECISION NOT NULL,
    dest_lng DOUBLE PRECISION NOT NULL,
    dest_address TEXT NOT NULL DEFAULT '',
    estimated_price_minor BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    estimated_distance_km DOUBLE PRECISION NOT NULL,
    actual_distance_km DOUBLE PRECISION,
    bill_id UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    matched_at TIMESTAMP WITH TIME ZONE,
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    cancelled_at TIMESTAMP WITH TIME ZONE,
    cancellation_reason TEXT
);

CREATE INDEX idx_ride_client_id ON ride.ride(client_id);
CREATE INDEX idx_ride_driver_id ON ride.ride(driver_id);
CREATE INDEX idx_ride_status ON ride.ride(status);
CREATE INDEX idx_ride_created_at ON ride.ride(created_at);

CREATE TABLE IF NOT EXISTS ride.outbox_message (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    retries INTEGER DEFAULT 0,
    -- The in-flight claim lease: the outbox worker's claim step sets this to
    -- NOW()+lease so a crashed worker's claim is recoverable (resumed once
    -- the lease elapses) without a concurrent tick re-claiming the same row
    -- while the first publish might still be in flight. Same concept as
    -- billing.payment.claimed_until.
    claimed_until TIMESTAMPTZ,
    -- W3C trace context (JSON, e.g. {"traceparent":"00-...-...-01"}) active
    -- when the row was inserted, captured by the outbox repository's Insert
    -- inside the same transaction as the domain write — NULL for rows
    -- inserted with no active trace. Lets the outbox worker's eventual
    -- Kafka publish (running on its own background ticker context, with no
    -- other link back to the originating HTTP request) still join that
    -- request's trace. See services/observability/obsoutbox.
    trace_context JSONB
);

CREATE INDEX idx_outbox_processed ON ride.outbox_message(processed);

-- ride.ride.driver_id -> driver.driver(id) is added in the 0005 migration,
-- once the driver schema (created by 0004) exists.
