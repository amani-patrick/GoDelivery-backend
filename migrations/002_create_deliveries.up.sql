-- Migration 002: Deliveries
-- The central aggregate table. pickup/dropoff coordinates are stored as
-- plain DOUBLE PRECISION columns (not PostGIS geometry) so that the Go
-- repository layer can use standard pgx scanning without a PostGIS driver.
-- PostGIS geometry columns are added in 002b when spatial queries are needed.
--
-- Security design notes:
--   pickup_qr_code  — bcrypt hash of the physical QR token. Plaintext never stored.
--   delivery_pin    — bcrypt hash of the customer OTP. Plaintext never stored.
--   Both columns are NOT NULL because a delivery without tokens cannot transition
--   to IN_TRANSIT or DELIVERED; the application enforces this, the DB enforces
--   non-nullability as a second safety net.

BEGIN;

-- Ensure PostGIS is available (installed via postgis Docker image).
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS deliveries (
    id              TEXT            NOT NULL,
    merchant_id     TEXT            NOT NULL REFERENCES users (id),
    driver_id       TEXT                     REFERENCES users (id),  -- nullable until ASSIGNED
    customer_id     TEXT            NOT NULL REFERENCES users (id),
    current_state   TEXT            NOT NULL DEFAULT 'CREATED',

    -- Custody tokens stored as bcrypt hashes only
    pickup_qr_code  TEXT            NOT NULL,
    delivery_pin    TEXT            NOT NULL,

    -- Pickup location (WGS-84)
    pickup_lat      DOUBLE PRECISION NOT NULL,
    pickup_lng      DOUBLE PRECISION NOT NULL,

    -- Dropoff location (WGS-84)
    dropoff_lat     DOUBLE PRECISION NOT NULL,
    dropoff_lng     DOUBLE PRECISION NOT NULL,

    -- PostGIS geometry columns for spatial queries (nearest driver, route validation)
    pickup_geom     GEOMETRY(Point, 4326) GENERATED ALWAYS AS (
                        ST_SetSRID(ST_MakePoint(pickup_lng, pickup_lat), 4326)
                    ) STORED,
    dropoff_geom    GEOMETRY(Point, 4326) GENERATED ALWAYS AS (
                        ST_SetSRID(ST_MakePoint(dropoff_lng, dropoff_lat), 4326)
                    ) STORED,

    description     TEXT            NOT NULL DEFAULT '',
    weight_kg       DOUBLE PRECISION NOT NULL DEFAULT 0,

    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT deliveries_pkey PRIMARY KEY (id),
    CONSTRAINT deliveries_state_check CHECK (
        current_state IN (
            'CREATED', 'ASSIGNED', 'IN_TRANSIT',
            'DELIVERED', 'CANCELLED', 'DISPUTED'
        )
    ),
    CONSTRAINT deliveries_weight_positive CHECK (weight_kg >= 0)
);

-- Merchant dashboard: list orders by merchant + state
CREATE INDEX IF NOT EXISTS deliveries_merchant_state_idx
    ON deliveries (merchant_id, current_state);

-- Driver app: active orders per driver
CREATE INDEX IF NOT EXISTS deliveries_driver_state_idx
    ON deliveries (driver_id, current_state)
    WHERE driver_id IS NOT NULL;

-- Dispatcher query: all in-transit orders (anomaly monitoring)
CREATE INDEX IF NOT EXISTS deliveries_in_transit_idx
    ON deliveries (current_state)
    WHERE current_state = 'IN_TRANSIT';

-- Spatial index: nearest-pickup queries
CREATE INDEX IF NOT EXISTS deliveries_pickup_geom_idx
    ON deliveries USING GIST (pickup_geom);

-- Optimistic concurrency control: queries filter on updated_at
CREATE INDEX IF NOT EXISTS deliveries_updated_at_idx
    ON deliveries (updated_at);

COMMIT;
