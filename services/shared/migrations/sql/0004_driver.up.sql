CREATE SCHEMA IF NOT EXISTS driver;

CREATE TABLE IF NOT EXISTS driver.driver (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID UNIQUE NOT NULL REFERENCES auth.user(id),
    rating DOUBLE PRECISION NOT NULL DEFAULT 5.0,
    vehicle_type TEXT NOT NULL DEFAULT 'Sedan',
    license_plate TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'Offline' CHECK (status IN ('Offline', 'Online', 'OnRide')),
    total_rides_completed INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

CREATE INDEX idx_driver_status ON driver.driver(status);
CREATE INDEX idx_driver_user_id ON driver.driver(user_id);

CREATE TABLE IF NOT EXISTS driver.shift (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    driver_id UUID NOT NULL REFERENCES driver.driver(id) ON DELETE CASCADE,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMP WITH TIME ZONE,
    total_rides INTEGER DEFAULT 0,
    total_earnings_minor BIGINT NOT NULL DEFAULT 0,
    currency CHAR(3) NOT NULL DEFAULT 'EUR'
);

CREATE INDEX idx_shift_driver_id ON driver.shift(driver_id);
CREATE INDEX idx_shift_started_at ON driver.shift(started_at);

CREATE TABLE IF NOT EXISTS driver.outbox_message (
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

CREATE INDEX idx_outbox_processed ON driver.outbox_message(processed);
