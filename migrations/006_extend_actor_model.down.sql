BEGIN;

-- Remove customer_profiles
DROP TABLE IF EXISTS customer_profiles;

-- Remove business_profiles
DROP INDEX  IF EXISTS business_profiles_geom_idx;
DROP INDEX  IF EXISTS business_profiles_tin_uniq;
DROP TABLE  IF EXISTS business_profiles;

-- Revert deliveries extensions
ALTER TABLE deliveries
    DROP CONSTRAINT IF EXISTS deliveries_package_category_check,
    DROP CONSTRAINT IF EXISTS deliveries_vehicle_type_required_check,
    DROP COLUMN IF EXISTS package_category,
    DROP COLUMN IF EXISTS vehicle_type_required;

-- Revert driver_profiles extensions
DROP INDEX  IF EXISTS driver_profiles_matching_idx;
DROP INDEX  IF EXISTS driver_profiles_status_idx;
DROP INDEX  IF EXISTS driver_profiles_nid_uniq;
ALTER TABLE driver_profiles
    DROP CONSTRAINT IF EXISTS driver_profiles_weight_positive,
    DROP CONSTRAINT IF EXISTS driver_profiles_vehicle_type_check,
    DROP CONSTRAINT IF EXISTS driver_profiles_status_check,
    DROP COLUMN IF EXISTS max_weight_kg,
    DROP COLUMN IF EXISTS vehicle_type,
    DROP COLUMN IF EXISTS license_number,
    DROP COLUMN IF EXISTS national_id,
    DROP COLUMN IF EXISTS status;

COMMIT;
