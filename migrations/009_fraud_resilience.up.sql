-- 009: Append-only ledger, weight fraud events, sequence tracking

-- Append-only immutable financial ledger 
CREATE TABLE ledger_entries (
    id              UUID PRIMARY KEY,
    delivery_id     UUID REFERENCES deliveries(id),
    actor_id        UUID NOT NULL,
    actor_type      VARCHAR(20) NOT NULL,   -- DRIVER | MERCHANT | PLATFORM
    entry_type      VARCHAR(30) NOT NULL,   -- FARE_EARNED | FINE_DISPLACEMENT | REFUND | ESCROW_HOLD | ESCROW_RELEASE
    amount_rwf      BIGINT NOT NULL,        -- positive = credit, negative = debit
    notes           TEXT,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
-- immutable: no UPDATE or DELETE allowed in application code
CREATE INDEX idx_ledger_actor ON ledger_entries (actor_id, created_at);

-- Weight discrepancy events 
CREATE TABLE weight_discrepancy_events (
    id              UUID PRIMARY KEY,
    delivery_id     UUID NOT NULL REFERENCES deliveries(id),
    driver_id       UUID NOT NULL,
    declared_kg     FLOAT NOT NULL,
    reported_kg     FLOAT,
    photo_url       TEXT,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',  -- PENDING | CONFIRMED | DISMISSED
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMP WITH TIME ZONE
);

-- Per-driver WebSocket sequence number tracker 
CREATE TABLE driver_sequence (
    driver_id       UUID PRIMARY KEY REFERENCES users(id),
    last_seq        BIGINT NOT NULL DEFAULT 0
);

-- Customer risk metrics table 
CREATE TABLE customer_risk (
    customer_id         UUID PRIMARY KEY REFERENCES users(id),
    total_orders        INT NOT NULL DEFAULT 0,
    completed_orders    INT NOT NULL DEFAULT 0,
    ghost_orders        INT NOT NULL DEFAULT 0,
    last_order_at       TIMESTAMP WITH TIME ZONE,
    risk_score          FLOAT NOT NULL DEFAULT 1.0,
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Surge price history for moving-average 
CREATE TABLE surge_demand_samples (
    id          BIGSERIAL PRIMARY KEY,
    sampled_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    region_key  VARCHAR(50) NOT NULL DEFAULT 'global',
    open_orders INT NOT NULL,
    online_drivers INT NOT NULL
);
CREATE INDEX idx_surge_samples_time ON surge_demand_samples (region_key, sampled_at);
