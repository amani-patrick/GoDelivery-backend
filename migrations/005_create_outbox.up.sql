-- Migration 005: Transactional Outbox
--
-- The Outbox Pattern solves the dual-write problem between our primary
-- database and external payment networks (MTN MoMo, Airtel Money).
--
-- Problem: after marking a delivery DELIVERED, calling the payment API
-- directly risks a partial failure — the delivery is marked done but the
-- driver never gets paid (or vice versa) if the network times out.
--
-- Solution: write a payment_events row in the SAME Postgres transaction
-- as the state update. A background worker (OutboxWorker) polls
-- payment_events WHERE status = 'PENDING' and processes them with
-- at-least-once delivery semantics and exponential back-off.
--
-- Atomicity guarantee:
--   BEGIN
--     UPDATE deliveries SET current_state = 'DELIVERED' ...
--     INSERT INTO payment_events (delivery_id, ...) VALUES (...)
--   COMMIT
--
-- If the commit succeeds, both rows land together.
-- If it fails, both roll back together. No partial state ever persists.
--
-- At-least-once vs exactly-once:
--   The worker may process an event more than once (e.g. after a crash).
--   Payment API calls must therefore be idempotent on the provider side
--   (MTN MoMo and Airtel Money both support idempotency keys in their API).
--   The outbox_idempotency_key column carries the key we send to the provider.

BEGIN;

CREATE TABLE IF NOT EXISTS payment_events (
    id                      TEXT        NOT NULL,

    -- The delivery this payment is for
    delivery_id             TEXT        NOT NULL,

    -- The driver who receives the payout
    driver_id               TEXT        NOT NULL,

    -- Amount in Rwandan Francs (RWF). Stored as integer to avoid float rounding.
    amount_rwf              BIGINT      NOT NULL CHECK (amount_rwf > 0),

    -- Phone number of the driver's MoMo wallet (+2507XXXXXXXX)
    recipient_phone         TEXT        NOT NULL,

    -- Processing state
    status                  TEXT        NOT NULL DEFAULT 'PENDING',
    -- 'PENDING'    → not yet attempted
    -- 'PROCESSING' → worker has claimed it (locked row)
    -- 'SUCCEEDED'  → payment API confirmed
    -- 'FAILED'     → all retries exhausted

    -- Idempotency key sent to the payment provider API.
    -- Derived as: SHA256(delivery_id || driver_id) encoded as hex.
    -- This ensures the provider deduplicates even if we submit twice.
    outbox_idempotency_key  TEXT        NOT NULL,

    -- Retry bookkeeping
    attempt_count           INTEGER     NOT NULL DEFAULT 0,
    max_attempts            INTEGER     NOT NULL DEFAULT 5,
    last_attempted_at       TIMESTAMPTZ,
    next_retry_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Provider response (populated on success or terminal failure)
    provider_reference      TEXT,        -- MTN MoMo transaction ID
    failure_reason          TEXT,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT payment_events_pkey PRIMARY KEY (id),
    CONSTRAINT payment_events_status_check CHECK (
        status IN ('PENDING', 'PROCESSING', 'SUCCEEDED', 'FAILED')
    ),
    CONSTRAINT payment_events_idempotency_uniq UNIQUE (outbox_idempotency_key)
);

-- Worker poll query: SELECT ... WHERE status='PENDING' AND next_retry_at <= NOW()
-- ORDER BY next_retry_at ASC FOR UPDATE SKIP LOCKED
CREATE INDEX IF NOT EXISTS payment_events_worker_poll_idx
    ON payment_events (status, next_retry_at ASC)
    WHERE status IN ('PENDING', 'PROCESSING');

-- Delivery lookup: find all payment events for a given delivery (dispatcher view)
CREATE INDEX IF NOT EXISTS payment_events_delivery_idx
    ON payment_events (delivery_id);

COMMIT;
