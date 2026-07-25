-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================
-- INIT SCHEMAS
-- =============================================
CREATE SCHEMA IF NOT EXISTS auth;
CREATE SCHEMA IF NOT EXISTS ride;
CREATE SCHEMA IF NOT EXISTS driver;

-- =============================================
-- AUTH SCHEMA
-- =============================================

CREATE TABLE IF NOT EXISTS auth.user (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    phone TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL CHECK (role IN ('Client', 'Driver')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX idx_user_email ON auth.user(email);
CREATE INDEX idx_user_role ON auth.user(role);

CREATE TABLE IF NOT EXISTS auth.refresh_token (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES auth.user(id) ON DELETE CASCADE,
    token TEXT NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_refresh_token_user_id ON auth.refresh_token(user_id);
CREATE INDEX idx_refresh_token_token ON auth.refresh_token(token);

-- =============================================
-- RIDE SCHEMA
-- =============================================

CREATE TABLE IF NOT EXISTS ride.tariff (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT UNIQUE NOT NULL,
    base_fare DOUBLE PRECISION NOT NULL DEFAULT 0,
    price_per_km DOUBLE PRECISION NOT NULL,
    price_per_min DOUBLE PRECISION NOT NULL DEFAULT 0
);

INSERT INTO ride.tariff (name, base_fare, price_per_km, price_per_min) VALUES
('Standard', 3.00, 1.00, 0.20),
('Comfort', 4.00, 1.50, 0.30),
('Premium', 6.00, 2.00, 0.40),
('Luxury', 10.00, 3.00, 0.50);

CREATE TABLE IF NOT EXISTS ride.ride (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id UUID NOT NULL REFERENCES auth.user(id),
    driver_id UUID,
    status TEXT NOT NULL DEFAULT 'Requested' CHECK (status IN ('Requested', 'Matched', 'InProgress', 'Completed', 'Cancelled')),
    pickup_lat DOUBLE PRECISION NOT NULL,
    pickup_lng DOUBLE PRECISION NOT NULL,
    pickup_address TEXT NOT NULL DEFAULT '',
    dest_lat DOUBLE PRECISION NOT NULL,
    dest_lng DOUBLE PRECISION NOT NULL,
    dest_address TEXT NOT NULL DEFAULT '',
    estimated_price DOUBLE PRECISION NOT NULL,
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
    retries INTEGER DEFAULT 0
);

CREATE INDEX idx_outbox_processed ON ride.outbox_message(processed);

-- =============================================
-- DRIVER SCHEMA
-- =============================================

CREATE TABLE IF NOT EXISTS driver.driver_profile (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID UNIQUE NOT NULL REFERENCES auth.user(id),
    name TEXT NOT NULL DEFAULT '',
    phone TEXT NOT NULL DEFAULT '',
    rating DOUBLE PRECISION NOT NULL DEFAULT 5.0,
    vehicle_type TEXT NOT NULL DEFAULT 'Sedan',
    license_plate TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'Offline' CHECK (status IN ('Offline', 'Online', 'OnRide')),
    total_rides_completed INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

CREATE INDEX idx_driver_status ON driver.driver_profile(status);
CREATE INDEX idx_driver_user_id ON driver.driver_profile(user_id);

CREATE TABLE IF NOT EXISTS driver.shift (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    driver_id UUID NOT NULL REFERENCES driver.driver_profile(id) ON DELETE CASCADE,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMP WITH TIME ZONE,
    total_rides INTEGER DEFAULT 0,
    total_earnings DOUBLE PRECISION DEFAULT 0
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
    retries INTEGER DEFAULT 0
    );

CREATE INDEX idx_outbox_processed ON driver.outbox_message(processed);
