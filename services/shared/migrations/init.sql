-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================
-- INIT SCHEMAS
-- =============================================
CREATE SCHEMA IF NOT EXISTS auth;
CREATE SCHEMA IF NOT EXISTS ride;
CREATE SCHEMA IF NOT EXISTS driver;
CREATE SCHEMA IF NOT EXISTS billing;

-- =============================================
-- AUTH SCHEMA
-- =============================================

CREATE TABLE IF NOT EXISTS auth.user (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    phone TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL CHECK (role IN ('Client', 'Driver', 'Admin')),
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

-- Role tables: auth.user is the shared login identity; each role gets its
-- own surrogate-keyed table for role-specific data (see CLAUDE.md "Data
-- model" / the 2026-07-25 role-table refactor in PLAN.md).
CREATE TABLE IF NOT EXISTS auth.client (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID UNIQUE NOT NULL REFERENCES auth.user(id),
    rating DOUBLE PRECISION NOT NULL DEFAULT 5.0,
    total_rides_completed INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_client_user_id ON auth.client(user_id);

CREATE TABLE IF NOT EXISTS auth.admin (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID UNIQUE NOT NULL REFERENCES auth.user(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_admin_user_id ON auth.admin(user_id);

-- Seed admin (local dev only) — email: admin@myubergo.local, password: admin123
INSERT INTO auth.user (id, email, password_hash, name, role)
VALUES ('00000000-0000-0000-0000-000000000001', 'admin@myubergo.local', '$2a$10$cmumCedXy0EcsJU3/ClYVec8oT3S03ClHZN/X0eg2ipLz6vOP4EVO', 'Admin', 'Admin');

INSERT INTO auth.admin (user_id)
VALUES ('00000000-0000-0000-0000-000000000001');

-- =============================================
-- RIDE SCHEMA
-- =============================================

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
    retries INTEGER DEFAULT 0
);

CREATE INDEX idx_outbox_processed ON ride.outbox_message(processed);

-- =============================================
-- DRIVER SCHEMA
-- =============================================

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
    retries INTEGER DEFAULT 0
    );

CREATE INDEX idx_outbox_processed ON driver.outbox_message(processed);

-- ride.ride.driver_id -> driver.driver(id): added here (bottom of file)
-- because the driver schema is created after the ride schema above.
ALTER TABLE ride.ride ADD CONSTRAINT fk_ride_driver_id FOREIGN KEY (driver_id) REFERENCES driver.driver(id);

-- =============================================
-- BILLING SCHEMA
-- =============================================
-- One provider-side customer per client per provider.
CREATE TABLE IF NOT EXISTS billing.customer (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id UUID NOT NULL REFERENCES auth.client(id),
    provider TEXT NOT NULL,
    provider_customer_id TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (client_id, provider)
);

CREATE TABLE IF NOT EXISTS billing.payment_method (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id UUID NOT NULL REFERENCES auth.client(id),
    provider TEXT NOT NULL,
    provider_payment_method_id TEXT NOT NULL,
    brand TEXT NOT NULL,
    last4 TEXT NOT NULL,
    exp_month INTEGER NOT NULL,
    exp_year INTEGER NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'expired', 'removed')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payment_method_client_id ON billing.payment_method(client_id);

-- Never store a PAN, CVC, or full expiry-plus-number here — brand/last4 are
-- display metadata only, sourced from the payment provider.
CREATE UNIQUE INDEX idx_payment_method_one_default
    ON billing.payment_method(client_id)
    WHERE is_default AND status = 'active';

-- Prevents a retried/double-submitted POST /payment-methods from creating
-- two rows for the same underlying provider card. AddPaymentMethodHandler
-- treats the resulting unique-violation as an idempotent no-op (re-reads
-- and returns the existing row), the same idiom as ErrDuplicateInvoice.
CREATE UNIQUE INDEX idx_payment_method_dedupe
    ON billing.payment_method(client_id, provider, provider_payment_method_id)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS billing.invoice (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ride_id UUID NOT NULL REFERENCES ride.ride(id),
    client_id UUID NOT NULL REFERENCES auth.client(id),
    driver_id UUID REFERENCES driver.driver(id),
    type TEXT NOT NULL CHECK (type IN ('ride_fare', 'cancellation_fee')),
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'paid', 'uncollectible', 'void')),
    amount_minor BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    -- attempt_count is NOT a column here — it's derived at read time via a
    -- correlated subquery over billing.payment (COUNT(*) WHERE invoice_id=...),
    -- consistent with the ledger's own "never a stored column, compute at
    -- query time" rule a few tables down. next_attempt_at is purely the
    -- retry SCHEDULE now; the in-flight claim LEASE lives on
    -- billing.payment.claimed_until instead — the two were conflated on this
    -- one column before this migration, which is what made ChargeWorker's
    -- claim/resume logic implicit rather than schema-documented.
    next_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    paid_at TIMESTAMPTZ,
    -- Idempotency guard against redelivered ride.completed/ride.cancelled
    -- Kafka events: a consumer catches the unique-violation instead of
    -- pre-checking with a SELECT (which would race).
    UNIQUE (ride_id, type)
);

CREATE INDEX idx_invoice_charge_sweep ON billing.invoice(status, next_attempt_at);
CREATE INDEX idx_invoice_client_id ON billing.invoice(client_id);

CREATE TABLE IF NOT EXISTS billing.invoice_line (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    invoice_id UUID NOT NULL REFERENCES billing.invoice(id),
    kind TEXT NOT NULL CHECK (kind IN ('base_fare', 'distance', 'time', 'cancellation_fee', 'platform_fee')),
    amount_minor BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_invoice_line_invoice_id ON billing.invoice_line(invoice_id);

-- One row per collection attempt, never mutated in place except for status.
CREATE TABLE IF NOT EXISTS billing.payment (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    invoice_id UUID NOT NULL REFERENCES billing.invoice(id),
    attempt_no INTEGER NOT NULL,
    provider TEXT NOT NULL,
    provider_payment_intent_id TEXT,
    payment_method_id UUID REFERENCES billing.payment_method(id),
    amount_minor BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'succeeded', 'failed')),
    failure_code TEXT,
    failure_message TEXT,
    -- Format: invoice:{invoice_id}:attempt:{attempt_no} — deterministic, so a
    -- crashed-and-retried attempt reuses the same key and cannot double-charge.
    idempotency_key TEXT NOT NULL,
    -- The in-flight claim lease: ChargeWorker's claim step sets this to
    -- NOW()+CHARGE_LEASE so a crashed worker's claim is recoverable (resumed
    -- once the lease elapses) without another tick re-charging while the
    -- first attempt might still be in flight. Purely a claim concept —
    -- unrelated to invoice.next_attempt_at's retry scheduling.
    claimed_until TIMESTAMPTZ,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (invoice_id, attempt_no),
    UNIQUE (idempotency_key)
);

CREATE INDEX idx_payment_invoice_id ON billing.payment(invoice_id);

-- The webhook handler (Phase 2) looks a payment up by this exact column —
-- this index makes that lookup provably 1:1 rather than merely "correct
-- because the code never produces a duplicate."
CREATE UNIQUE INDEX idx_payment_provider_intent
    ON billing.payment(provider_payment_intent_id)
    WHERE provider_payment_intent_id IS NOT NULL;

-- Ledger: append-only double-entry bookkeeping. Balances are always
-- SUM(credits) - SUM(debits) computed at query time, never a stored column.
CREATE TABLE IF NOT EXISTS billing.ledger_account (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type TEXT NOT NULL CHECK (type IN ('client_receivable', 'driver_payable', 'platform_revenue', 'psp_clearing', 'psp_fees', 'bad_debt')),
    owner_id UUID,
    currency CHAR(3) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    -- Accounts are per-currency: a client with EUR and USD rides has two
    -- distinct client_receivable accounts. owner_id is null for
    -- platform-level accounts (platform_revenue, psp_clearing, bad_debt).
);

-- A plain UNIQUE(type, owner_id, currency) constraint would NOT collapse
-- platform-level accounts (owner_id IS NULL) to one row per currency —
-- Postgres treats every NULL as distinct in a unique index. COALESCE to a
-- sentinel UUID so get-or-create actually has something to conflict on.
CREATE UNIQUE INDEX idx_ledger_account_unique
    ON billing.ledger_account (type, COALESCE(owner_id, '00000000-0000-0000-0000-000000000000'::uuid), currency);

CREATE TABLE IF NOT EXISTS billing.ledger_transaction (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type TEXT NOT NULL CHECK (type IN ('invoice_opened', 'payment_succeeded', 'invoice_uncollectible')),
    ref_type TEXT NOT NULL,
    ref_id UUID NOT NULL,
    currency CHAR(3) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Append-only: never UPDATE, never DELETE. Corrections are new compensating
-- transactions.
CREATE TABLE IF NOT EXISTS billing.ledger_entry (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id UUID NOT NULL REFERENCES billing.ledger_transaction(id),
    account_id UUID NOT NULL REFERENCES billing.ledger_account(id),
    direction TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency CHAR(3) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_ledger_entry_account_created ON billing.ledger_entry(account_id, created_at);

CREATE TABLE IF NOT EXISTS billing.outbox_message (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    retries INTEGER DEFAULT 0
);

CREATE INDEX idx_billing_outbox_processed ON billing.outbox_message(processed);

-- Inbound Stripe webhook inbox. id is Stripe's own event id (e.g.
-- "evt_..."), so a redelivered webhook hits this primary key's
-- unique-violation as the idempotency guard — the same insert-then-process
-- idiom as billing.invoice's (ride_id, type) constraint. processed_at is
-- NULL until the event's effect (a Finalize* command or a MarkProcessing
-- status flip) has actually been applied, not merely stored — a delivery
-- that fails mid-processing leaves this NULL so the next redelivery retries
-- the effect instead of being silently swallowed as "already seen."
CREATE TABLE IF NOT EXISTS billing.psp_event (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    api_version TEXT NOT NULL DEFAULT '',
    payload JSONB NOT NULL,
    received_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    process_error TEXT
);
