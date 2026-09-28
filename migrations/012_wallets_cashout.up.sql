-- 012: Wallet cashouts + wallet bootstrap
--
-- Money model (no external MoMo yet — provider is a stub interface):
--   * Order creation escrows the fare from the merchant wallet (balance→escrow),
--     guaranteeing the driver gets paid before any trip starts.
--   * Delivery releases escrow and instantly credits the driver (fare −
--     commission) plus the platform treasury (commission).
--   * Cashout is self-service and instant: debit wallet → cashout_requests row
--     → payout provider (STUB until MoMo integration lands). No admin queue.
--   * Admin approval applies to DRIVER REGISTRATION (verification), not to
--     money movement.

-- Ledger queries are correlated per delivery (delivery finance view) and per
-- actor ("my earnings"); delivery_id is nullable so a partial index.
CREATE INDEX idx_ledger_delivery ON ledger_entries (delivery_id) WHERE delivery_id IS NOT NULL;

-- The fare is locked at order creation (escrow amount) and must be immutable
-- through the delivery lifecycle, so it is persisted on the row rather than
-- recomputed at delivery time (surge could otherwise change the bill).
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS fare_rwf BIGINT NOT NULL DEFAULT 0;

-- Migration 001 shipped a role CHECK without ADMIN (the domain grew the role
-- later). Widen it so the platform-treasury ADMIN user can exist; registration
-- code independently blocks self-registration of privileged roles.
ALTER TABLE users DROP CONSTRAINT IF EXISTS users_role_check;
ALTER TABLE users ADD CONSTRAINT users_role_check
	CHECK (role = ANY (ARRAY['MERCHANT','DRIVER','CUSTOMER','DISPATCHER','ADMIN']));
--
-- Type note: users.id is TEXT — FK columns referencing it must be TEXT.

-- Every existing user gets a zero wallet row.
INSERT INTO wallets (user_id)
SELECT id FROM users
ON CONFLICT (user_id) DO NOTHING;

CREATE TABLE cashout_requests (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL REFERENCES users(id),
    amount_rwf      BIGINT NOT NULL CHECK (amount_rwf > 0),
    phone           VARCHAR(20) NOT NULL,          -- payout destination
    provider        VARCHAR(30) NOT NULL DEFAULT 'MOMO_STUB',
    status          VARCHAR(20) NOT NULL DEFAULT 'PROCESSING',
                    -- PROCESSING | PAID | FAILED
    provider_ref    TEXT,
    failure_reason  TEXT,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_cashout_user ON cashout_requests (user_id, created_at DESC);
CREATE INDEX idx_cashout_status ON cashout_requests (status, created_at DESC)
    WHERE status = 'PROCESSING';

-- Platform treasury wallet (commission + float accounting). users.role=ADMIN
-- is the system operator; the PLATFORM actor in ledger_entries maps here.
-- The ADMIN user itself is seeded below (no self-registration path exists).
INSERT INTO wallets (user_id)
SELECT id FROM users WHERE role = 'ADMIN'
ON CONFLICT (user_id) DO NOTHING;

INSERT INTO users (id, full_name, phone, email, role, password_hash, is_active, is_verified)
VALUES (
	'platform-treasury',
	'Platform Treasury',
	'+250700000000',
	'ops@umurinzi.rw',
	'ADMIN',
	-- bcrypt cost 12 hash of a long random secret; rotate via ops. Login is
	-- not exposed to this account in normal operation (it is an actor of
	-- record for ledger entries). Replace when admin auth is provisioned.
	'$2a$12$8XqYvLQ0e5z9W3nR7Tq4yeZ0fF1kGh2mJc6XbA4dS9pUw3iE5rO7u',
	true,
	true
)
ON CONFLICT (id) DO NOTHING;
