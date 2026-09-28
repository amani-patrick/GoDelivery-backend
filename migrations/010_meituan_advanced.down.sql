DROP TABLE IF EXISTS learned_shortcuts;
ALTER TABLE deliveries DROP COLUMN IF NOT EXISTS prep_time_minutes;
ALTER TABLE deliveries DROP COLUMN IF NOT EXISTS ready_at;
