-- Migration 007: Delivery stacking, weight fraud, and premium delivery
--
-- Adds four columns to the deliveries table:
--
--   confirmed_weight_kg  — physical weight confirmed by the driver at pickup
-— weight fraud guard).
--                          0 means not yet confirmed.
--
--   priority_level       — 0 = standard, 1 = priority, 2 = premium.
--                          Premium orders bypass stacking and get a score boost
-.
--
--   stack_group_id       — UUID shared across all deliveries in a stacked route.
--                          NULL for solo deliveries.
--
--   stack_sequence       — position of this delivery within the stacked route.
--                          0 = solo, 1 = first drop, 2 = second drop, etc.
-— order stacking).

BEGIN;

ALTER TABLE deliveries
    ADD COLUMN IF NOT EXISTS confirmed_weight_kg DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS priority_level      INTEGER          NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stack_group_id      TEXT,
    ADD COLUMN IF NOT EXISTS stack_sequence      INTEGER          NOT NULL DEFAULT 0;

-- Constraint: priority_level must be 0, 1, or 2.
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'deliveries_priority_level_check'
    ) THEN
        ALTER TABLE deliveries
            ADD CONSTRAINT deliveries_priority_level_check
            CHECK (priority_level BETWEEN 0 AND 2);
    END IF;
END $$;

-- Index: dispatcher query — all deliveries in a stacked group
CREATE INDEX IF NOT EXISTS deliveries_stack_group_idx
    ON deliveries (stack_group_id)
    WHERE stack_group_id IS NOT NULL;

-- Index: premium order dashboard
CREATE INDEX IF NOT EXISTS deliveries_priority_idx
    ON deliveries (priority_level, current_state)
    WHERE priority_level > 0;

-- trust_signals table  — append-only fraud event log for all actor types
CREATE TABLE IF NOT EXISTS trust_signals (
    id          TEXT        NOT NULL,
    actor_id    TEXT        NOT NULL,
    actor_type  TEXT        NOT NULL, -- 'DRIVER' | 'MERCHANT' | 'CUSTOMER'
    signal_type TEXT        NOT NULL,
    severity    INTEGER     NOT NULL DEFAULT 1 CHECK (severity BETWEEN 1 AND 5),
    details     TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT trust_signals_pkey PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS trust_signals_actor_idx
    ON trust_signals (actor_id, created_at DESC);

CREATE INDEX IF NOT EXISTS trust_signals_type_idx
    ON trust_signals (signal_type, created_at DESC);

-- trust_score columns on all three profile tables 
ALTER TABLE driver_profiles
    ADD COLUMN IF NOT EXISTS trust_score DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS trust_level TEXT             NOT NULL DEFAULT 'GOOD';

ALTER TABLE business_profiles
    ADD COLUMN IF NOT EXISTS trust_score DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS trust_level TEXT             NOT NULL DEFAULT 'GOOD';

ALTER TABLE customer_profiles
    ADD COLUMN IF NOT EXISTS trust_score DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS trust_level TEXT             NOT NULL DEFAULT 'GOOD';

-- rating column is already on driver_profiles from migration 006.
-- Add UpdateRating support: index for fast rating queries.
CREATE INDEX IF NOT EXISTS driver_profiles_rating_idx
    ON driver_profiles (rating DESC)
    WHERE status = 'ACTIVE';

COMMIT;
