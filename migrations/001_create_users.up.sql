-- Migration 001: Users
-- Creates the core identity table shared by all actor types (merchant, driver,
-- customer, dispatcher). The phone column has a unique index because it is the
-- primary authentication credential in Rwanda, where phone penetration far
-- exceeds email usage.

BEGIN;

CREATE TABLE IF NOT EXISTS users (
    id            TEXT        NOT NULL,
    full_name     TEXT        NOT NULL,
    phone         TEXT        NOT NULL,
    email         TEXT,
    role          TEXT        NOT NULL,   -- 'MERCHANT' | 'DRIVER' | 'CUSTOMER' | 'DISPATCHER'
    password_hash TEXT        NOT NULL,   -- bcrypt hash only — plaintext never stored
    is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
    is_verified   BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_role_check CHECK (
        role IN ('MERCHANT', 'DRIVER', 'CUSTOMER', 'DISPATCHER')
    )
);

-- Unique index on phone: one account per Rwanda SIM card.
CREATE UNIQUE INDEX IF NOT EXISTS users_phone_uniq ON users (phone);

-- Index for JWT subject lookups (GetByID is called on every authenticated request).
CREATE INDEX IF NOT EXISTS users_id_idx ON users (id);

COMMIT;
