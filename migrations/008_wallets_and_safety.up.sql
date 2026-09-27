CREATE TABLE wallets (
    user_id UUID PRIMARY KEY REFERENCES users(id),
    balance_rwf BIGINT NOT NULL DEFAULT 0,
    escrow_rwf BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE wallet_transactions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id),
    amount_rwf BIGINT NOT NULL,
    type VARCHAR(50) NOT NULL,
    reference_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE danger_zones (
    id UUID PRIMARY KEY,
    center GEOMETRY(Point, 4326) NOT NULL,
    radius_meters FLOAT NOT NULL DEFAULT 500.0,
    threat_level FLOAT NOT NULL DEFAULT 1.0,
    last_incident_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_danger_zones_center ON danger_zones USING GIST (center);

CREATE TABLE safe_hubs (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    geom GEOMETRY(Point, 4326) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_safe_hubs_geom ON safe_hubs USING GIST (geom);
