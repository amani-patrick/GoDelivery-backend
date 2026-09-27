-- Migration 004: Audit Events (Immutable Ledger)
--
-- Compliance: satisfies Rwanda Data Protection Law No. 058/2021 requirements
-- for an evidentiary audit trail. This table must be protected at the database
-- role level with INSERT-only grants — no UPDATE or DELETE privileges granted
-- to the application role.
--
-- Role grant (run as superuser after migration):
--   REVOKE UPDATE, DELETE ON audit_events FROM umurinzi_app;
--   GRANT INSERT, SELECT ON audit_events TO umurinzi_app;
--
-- The application never reads audit_events in the hot path; it is queried only
-- by dispatchers and compliance officers. No foreign keys are declared so that
-- audit records survive even if the referenced entity is hard-deleted (which
-- should never happen, but belt-and-suspenders).

BEGIN;

CREATE TABLE IF NOT EXISTS audit_events (
    id           TEXT        NOT NULL,
    entity_id    TEXT        NOT NULL,   -- delivery_id, user_id, etc.
    entity_type  TEXT        NOT NULL,   -- 'DELIVERY' | 'USER' | 'DRIVER'
    actor_id     TEXT        NOT NULL,   -- user_id of the actor, or 'SYSTEM'
    action       TEXT        NOT NULL,   -- e.g. 'DELIVERY_CREATED', 'HANDSHAKE_FAILED_PICKUP'
    old_state    TEXT,
    new_state    TEXT,
    metadata     TEXT,                   -- free-form JSON blob for extra context
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT audit_events_pkey PRIMARY KEY (id)
);

-- Primary forensic query: full lifecycle of a single delivery
CREATE INDEX IF NOT EXISTS audit_events_entity_time_idx
    ON audit_events (entity_id, created_at ASC);

-- Dispatcher query: all actions by a specific actor (driver misconduct review)
CREATE INDEX IF NOT EXISTS audit_events_actor_time_idx
    ON audit_events (actor_id, created_at DESC);

-- Security alert query: filter by action type across all entities
CREATE INDEX IF NOT EXISTS audit_events_action_idx
    ON audit_events (action);

-- Compliance query: time-bounded export for regulatory requests
CREATE INDEX IF NOT EXISTS audit_events_created_at_idx
    ON audit_events (created_at DESC);

-- Prevent any future accidental UPDATE or DELETE at the SQL level.
-- The application DB role should also have these privileges revoked.
-- (PostgreSQL does not support DENY; we rely on role-level REVOKE.)
-- This trigger provides a defence-in-depth check inside the DB itself.
CREATE OR REPLACE FUNCTION audit_events_immutability_guard()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION
        'audit_events is an immutable ledger — UPDATE and DELETE are forbidden (event_id: %)',
        OLD.id;
END;
$$;

CREATE TRIGGER audit_events_no_update
    BEFORE UPDATE ON audit_events
    FOR EACH ROW EXECUTE FUNCTION audit_events_immutability_guard();

CREATE TRIGGER audit_events_no_delete
    BEFORE DELETE ON audit_events
    FOR EACH ROW EXECUTE FUNCTION audit_events_immutability_guard();

COMMIT;
