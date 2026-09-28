-- Merchant Wait Bounty: tracks wait-time micro-fees charged when drivers arrive
-- before the merchant has marked the order ready.
CREATE TABLE IF NOT EXISTS merchant_wait_bounties (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id     TEXT NOT NULL REFERENCES deliveries(id),
    driver_id       TEXT NOT NULL,
    merchant_id     TEXT NOT NULL,
    wait_started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    wait_ended_at   TIMESTAMP WITH TIME ZONE,
    wait_minutes    FLOAT NOT NULL DEFAULT 0,
    bounty_rwf      FLOAT NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'ACCRUING', -- ACCRUING | SETTLED | WAIVED
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX idx_merchant_wait_bounties_delivery ON merchant_wait_bounties (delivery_id);
CREATE INDEX idx_merchant_wait_bounties_merchant ON merchant_wait_bounties (merchant_id);
-- Wait-bounty upserts with ON CONFLICT (delivery_id) — one bounty per delivery.
ALTER TABLE merchant_wait_bounties ADD CONSTRAINT uq_merchant_wait_bounties_delivery UNIQUE (delivery_id);

-- Merchant prep accuracy: rolling accuracy score so the system learns how well
-- merchants estimate their prep time (used to dynamically adjust JIT window).
ALTER TABLE driver_profiles ADD COLUMN IF NOT EXISTS avg_prep_accuracy FLOAT DEFAULT 1.0;

-- Dead Man's Switch: high-priority alerts when telemetry goes dark.
-- These are stored in Postgres for audit trail; Redis handles real-time detection.
CREATE TABLE IF NOT EXISTS dead_mans_switch_alerts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id     TEXT NOT NULL REFERENCES deliveries(id),
    driver_id       TEXT NOT NULL,
    last_known_lat  FLOAT NOT NULL,
    last_known_lng  FLOAT NOT NULL,
    silence_minutes FLOAT NOT NULL,
    resolution      TEXT NOT NULL DEFAULT 'OPEN', 
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    resolved_at     TIMESTAMP WITH TIME ZONE
);
CREATE INDEX idx_dms_alerts_driver ON dead_mans_switch_alerts (driver_id);
CREATE INDEX idx_dms_alerts_open ON dead_mans_switch_alerts (resolution) WHERE resolution = 'OPEN';

-- Multi-Drop Sequence: persists the optimised drop-off order for stacked deliveries.
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS optimised_sequence INT DEFAULT 0;

-- RRA Micro-Invoicing: immutable tax invoice ledger for Rwanda Revenue Authority compliance.
CREATE TABLE IF NOT EXISTS tax_invoices (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id     TEXT NOT NULL REFERENCES deliveries(id),
    driver_id       TEXT NOT NULL,
    merchant_id     TEXT NOT NULL,
    customer_id     TEXT NOT NULL,
    fare_rwf        FLOAT NOT NULL,
    vat_rwf         FLOAT NOT NULL,
    total_rwf       FLOAT NOT NULL,
    invoice_number  TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL DEFAULT 'PENDING', -- PENDING | SUBMITTED | CONFIRMED | FAILED
    rra_payload     JSONB,
    submitted_at    TIMESTAMP WITH TIME ZONE,
    confirmed_at    TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX idx_tax_invoices_delivery ON tax_invoices (delivery_id);
CREATE INDEX idx_tax_invoices_status ON tax_invoices (status) WHERE status = 'PENDING';
-- Invoice worker upserts with ON CONFLICT (delivery_id) — requires a unique
-- constraint so one delivery can never accumulate duplicate invoices.
ALTER TABLE tax_invoices ADD CONSTRAINT uq_tax_invoices_delivery UNIQUE (delivery_id);
