BEGIN;
DROP INDEX IF EXISTS driver_profiles_plate_uniq;
DROP INDEX IF EXISTS driver_profiles_online_geom_idx;
DROP TABLE IF EXISTS driver_profiles;
COMMIT;
