DROP TABLE IF EXISTS tax_invoices;
ALTER TABLE deliveries DROP COLUMN IF EXISTS optimised_sequence;
DROP TABLE IF EXISTS dead_mans_switch_alerts;
ALTER TABLE driver_profiles DROP COLUMN IF EXISTS avg_prep_accuracy;
DROP TABLE IF EXISTS merchant_wait_bounties;
