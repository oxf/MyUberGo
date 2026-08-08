CREATE SCHEMA IF NOT EXISTS auth;

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
