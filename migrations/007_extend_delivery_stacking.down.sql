BEGIN;

ALTER TABLE customer_profiles  DROP COLUMN IF EXISTS trust_level, DROP COLUMN IF EXISTS trust_score;
ALTER TABLE business_profiles  DROP COLUMN IF EXISTS trust_level, DROP COLUMN IF EXISTS trust_score;
ALTER TABLE driver_profiles    DROP COLUMN IF EXISTS trust_level, DROP COLUMN IF EXISTS trust_score;

DROP INDEX  IF EXISTS driver_profiles_rating_idx;
DROP INDEX  IF EXISTS trust_signals_type_idx;
DROP INDEX  IF EXISTS trust_signals_actor_idx;
DROP TABLE  IF EXISTS trust_signals;

DROP INDEX  IF EXISTS deliveries_priority_idx;
DROP INDEX  IF EXISTS deliveries_stack_group_idx;

ALTER TABLE deliveries
    DROP CONSTRAINT IF EXISTS deliveries_priority_level_check,
    DROP COLUMN     IF EXISTS stack_sequence,
    DROP COLUMN     IF EXISTS stack_group_id,
    DROP COLUMN     IF EXISTS priority_level,
    DROP COLUMN     IF EXISTS confirmed_weight_kg;

COMMIT;
