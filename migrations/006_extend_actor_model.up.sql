-- Migration 006: Full Actor Model Extension
--
-- This migration extends the schema to support the complete three-actor
-- choreography: Merchant → Driver → Customer.
--
-- Changes:
--   1. ALTER driver_profiles — add vehicle specs, legal IDs, driver status
--   2. ALTER deliveries     — add vehicle_type_required, package_category
--   3. CREATE business_profiles — merchant/business hub data
--   4. CREATE customer_profiles — lightweight customer saved-address store
--
-- All ALTER TABLE statements use IF NOT EXISTS / IF EXISTS guards so the
-- migration is idempotent and safe to re-run in CI.

BEGIN;

-- ── 1. Extend driver_profiles ─────────────────────────────────────────────────

-- Driver operational status lifecycle:
--   PENDING_VERIFICATION → (admin approves) → ACTIVE
--   ACTIVE → (takes a job) → ON_TRIP → (completes) → ACTIVE
--   ACTIVE / ON_TRIP → (admin suspends) → SUSPENDED
ALTER TABLE driver_profiles
    ADD COLUMN IF NOT EXISTS status          TEXT    NOT NULL DEFAULT 'PENDING_VERIFICATION',
    ADD COLUMN IF NOT EXISTS national_id     TEXT,            -- 16-digit Rwanda NID; NULL until submitted
    ADD COLUMN IF NOT EXISTS license_number  TEXT,            -- driving licence; NULL until submitted
    ADD COLUMN IF NOT EXISTS vehicle_type    TEXT    NOT NULL DEFAULT 'MOTORCYCLE',
    ADD COLUMN IF NOT EXISTS max_weight_kg   DOUBLE PRECISION NOT NULL DEFAULT 50;

-- Status constraint
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'driver_profiles_status_check'
    ) THEN
        ALTER TABLE driver_profiles
            ADD CONSTRAINT driver_profiles_status_check CHECK (
                status IN ('PENDING_VERIFICATION','ACTIVE','SUSPENDED','ON_TRIP')
            );
    END IF;
END $$;

-- Vehicle type constraint
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'driver_profiles_vehicle_type_check'
    ) THEN
        ALTER TABLE driver_profiles
            ADD CONSTRAINT driver_profiles_vehicle_type_check CHECK (
                vehicle_type IN ('MOTORCYCLE','CAR','VAN','TRUCK')
            );
    END IF;
END $$;

-- Weight must be positive
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'driver_profiles_weight_positive'
    ) THEN
        ALTER TABLE driver_profiles
            ADD CONSTRAINT driver_profiles_weight_positive CHECK (max_weight_kg > 0);
    END IF;
END $$;

-- Unique index on national_id — one driving account per citizen
CREATE UNIQUE INDEX IF NOT EXISTS driver_profiles_nid_uniq
    ON driver_profiles (national_id)
    WHERE national_id IS NOT NULL;

-- Dispatcher admin query: all drivers pending review
CREATE INDEX IF NOT EXISTS driver_profiles_status_idx
    ON driver_profiles (status);

-- Matching engine query: active online drivers by vehicle type
CREATE INDEX IF NOT EXISTS driver_profiles_matching_idx
    ON driver_profiles (status, vehicle_type, max_weight_kg)
    WHERE status = 'ACTIVE' AND is_online = TRUE;

-- ── 2. Extend deliveries ──────────────────────────────────────────────────────

-- vehicle_type_required: empty string means any vehicle type is acceptable.
-- package_category: used for insurance, customs, and vehicle matching.
ALTER TABLE deliveries
    ADD COLUMN IF NOT EXISTS vehicle_type_required  TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS package_category       TEXT NOT NULL DEFAULT 'GENERAL';

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'deliveries_vehicle_type_required_check'
    ) THEN
        ALTER TABLE deliveries
            ADD CONSTRAINT deliveries_vehicle_type_required_check CHECK (
                vehicle_type_required IN ('','MOTORCYCLE','CAR','VAN','TRUCK')
            );
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'deliveries_package_category_check'
    ) THEN
        ALTER TABLE deliveries
            ADD CONSTRAINT deliveries_package_category_check CHECK (
                package_category IN (
                    'GENERAL','ELECTRONICS','FRAGILE',
                    'PERISHABLE','DOCUMENTS','BULK'
                )
            );
    END IF;
END $$;

-- ── 3. Create business_profiles ───────────────────────────────────────────────
-- One row per MERCHANT user. The geofenced home_lat/home_lng is the physical
-- pickup hub used by OSRM to compute routing matrices for driver dispatch.

CREATE TABLE IF NOT EXISTS business_profiles (
    user_id         TEXT             NOT NULL,
    company_name    TEXT             NOT NULL DEFAULT '',
    tin_number      TEXT,                         -- Rwanda RRA TIN; sensitive, never in API responses
    contact_name    TEXT             NOT NULL DEFAULT '',
    pickup_address  TEXT             NOT NULL DEFAULT '',
    home_lat        DOUBLE PRECISION NOT NULL DEFAULT 0,
    home_lng        DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- PostGIS geometry for routing matrix queries
    home_geom       GEOMETRY(Point, 4326) GENERATED ALWAYS AS (
                        ST_SetSRID(ST_MakePoint(home_lng, home_lat), 4326)
                    ) STORED,

    district        TEXT             NOT NULL DEFAULT '',
    is_verified     BOOLEAN          NOT NULL DEFAULT FALSE,

    CONSTRAINT business_profiles_pkey    PRIMARY KEY (user_id),
    CONSTRAINT business_profiles_user_fk FOREIGN KEY (user_id) REFERENCES users (id)
        ON DELETE CASCADE
);

-- Unique TIN: one business account per tax registration number
CREATE UNIQUE INDEX IF NOT EXISTS business_profiles_tin_uniq
    ON business_profiles (tin_number)
    WHERE tin_number IS NOT NULL;

-- Spatial index for merchant-density maps and nearest-merchant queries
CREATE INDEX IF NOT EXISTS business_profiles_geom_idx
    ON business_profiles USING GIST (home_geom);

-- ── 4. Create customer_profiles ───────────────────────────────────────────────
-- Lightweight table. saved_lat / saved_lng are the customer's last used
-- drop-off coordinates, pre-filled on new order creation.
--
-- Privacy mandate: the client app must wipe saved_lat / saved_lng from its
-- local cache the moment a delivery reaches a terminal state. This table is
-- the durable server-side record, but the values are considered PII under
-- Rwanda Law No. 058/2021 and must not persist in driver-visible storage.

CREATE TABLE IF NOT EXISTS customer_profiles (
    user_id    TEXT             NOT NULL,
    saved_lat  DOUBLE PRECISION NOT NULL DEFAULT 0,
    saved_lng  DOUBLE PRECISION NOT NULL DEFAULT 0,

    CONSTRAINT customer_profiles_pkey    PRIMARY KEY (user_id),
    CONSTRAINT customer_profiles_user_fk FOREIGN KEY (user_id) REFERENCES users (id)
        ON DELETE CASCADE
);

COMMIT;
