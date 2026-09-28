-- 1. Split-Stage ETA (Elevator & Kitchen)
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS prep_time_minutes INT DEFAULT 0;
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS ready_at TIMESTAMP WITH TIME ZONE;

-- 2. Crowdsourced Path Learning (Blind Alley)
CREATE TABLE IF NOT EXISTS learned_shortcuts (
    id SERIAL PRIMARY KEY,
    start_lat FLOAT NOT NULL,
    start_lng FLOAT NOT NULL,
    end_lat FLOAT NOT NULL,
    end_lng FLOAT NOT NULL,
    avg_speed_kmh FLOAT NOT NULL,
    usage_count INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Basic index for spatial proximity lookups (using float bounding boxes for simplicity)
CREATE INDEX idx_learned_shortcuts_start ON learned_shortcuts (start_lat, start_lng);
CREATE INDEX idx_learned_shortcuts_end ON learned_shortcuts (end_lat, end_lng);
