-- 012 down: remove cashout infrastructure (wallets/ledger predate this migration)
DELETE FROM users WHERE id = 'platform-treasury';
ALTER TABLE users DROP CONSTRAINT IF EXISTS users_role_check;
ALTER TABLE users ADD CONSTRAINT users_role_check
	CHECK (role = ANY (ARRAY['MERCHANT','DRIVER','CUSTOMER','DISPATCHER']));
ALTER TABLE deliveries DROP COLUMN IF EXISTS fare_rwf;
DROP INDEX IF EXISTS idx_ledger_delivery;
DROP TABLE IF EXISTS cashout_requests;
