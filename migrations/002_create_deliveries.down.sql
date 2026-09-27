BEGIN;
DROP INDEX IF EXISTS deliveries_updated_at_idx;
DROP INDEX IF EXISTS deliveries_pickup_geom_idx;
DROP INDEX IF EXISTS deliveries_in_transit_idx;
DROP INDEX IF EXISTS deliveries_driver_state_idx;
DROP INDEX IF EXISTS deliveries_merchant_state_idx;
DROP TABLE IF EXISTS deliveries;
COMMIT;
