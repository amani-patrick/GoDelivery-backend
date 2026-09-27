BEGIN;
DROP INDEX IF EXISTS payment_events_delivery_idx;
DROP INDEX IF EXISTS payment_events_worker_poll_idx;
DROP TABLE IF EXISTS payment_events;
COMMIT;
