-- Migration 003: Driver Profiles
-- Extends the users table for moto-driver-specific operational data.
-- current_lat / current_lng are the last known GPS position written by the
-- repository layer. The Redis SpatialIndex is the authoritative real-time
-- source; this table is a durable fallback for cold-start queries.

BEGIN;

CREATE TABLE IF NOT EXISTS driver_profiles (
    user_id          TEXT             NOT NULL,
    plate_number     TEXT             NOT NULL,
    is_online        BOOLEAN          NOT NULL DEFAULT FALSE,

    -- Last persisted position (Redis is the live authoritative source)
    current_lat      DOUBLE PRECISION NOT NULL DEFAULT 0,
    current_lng      DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- PostGIS geometry for spatial driver-search queries
    current_geom     GEOMETRY(Point, 4326) GENERATED ALWAYS AS (
                         ST_SetSRID(ST_MakePoint(current_lng, current_lat), 4326)
                     ) STORED,

    rating           DOUBLE PRECISION NOT NULL DEFAULT 5.0,
    total_deliveries INTEGER          NOT NULL DEFAULT 0,

    CONSTRAINT driver_profiles_pkey    PRIMARY KEY (user_id),
    CONSTRAINT driver_profiles_user_fk FOREIGN KEY (user_id) REFERENCES users (id)
        ON DELETE CASCADE,
    CONSTRAINT driver_profiles_rating_range CHECK (rating BETWEEN 0 AND 5),
    CONSTRAINT driver_profiles_deliveries_positive CHECK (total_deliveries >= 0)
);

-- Dispatcher: find all online drivers near a pickup point
CREATE INDEX IF NOT EXISTS driver_profiles_online_geom_idx
    ON driver_profiles USING GIST (current_geom)
    WHERE is_online = TRUE;

-- Unique constraint on plate to prevent double-registration
CREATE UNIQUE INDEX IF NOT EXISTS driver_profiles_plate_uniq
    ON driver_profiles (plate_number);

COMMIT;
