-- Seed the platform treasury ADMIN user + wallet (safe to re-run).
-- This is the actor of record for PLATFORM ledger entries (commission) and
-- the wallet that receives them. There is no self-registration path for
-- ADMIN — privileged accounts are provisioned by ops scripts like this one.
-- NOTE: password_hash below is a placeholder bcrypt string; the account is
-- not intended for interactive login until ops assigns real credentials.

ALTER TABLE users DROP CONSTRAINT IF EXISTS users_role_check;
ALTER TABLE users ADD CONSTRAINT users_role_check
	CHECK (role = ANY (ARRAY['MERCHANT','DRIVER','CUSTOMER','DISPATCHER','ADMIN']));

INSERT INTO users (id, full_name, phone, email, role, password_hash, is_active, is_verified)
VALUES (
	'platform-treasury',
	'Platform Treasury',
	'+250700000000',
	'ops@umurinzi.rw',
	'ADMIN',
	'$2a$12$8XqYvLQ0e5z9W3nR7Tq4yeZ0fF1kGh2mJc6XbA4dS9pUw3iE5rO7u',
	true,
	true
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO wallets (user_id) VALUES ('platform-treasury')
ON CONFLICT (user_id) DO NOTHING;

SELECT id, role, is_active FROM users WHERE role = 'ADMIN';
