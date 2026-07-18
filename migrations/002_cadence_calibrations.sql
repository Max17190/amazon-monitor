CREATE TABLE IF NOT EXISTS credential_cadence_calibrations (
    credential_key TEXT NOT NULL,
    marketplace_id TEXT NOT NULL,
    region TEXT NOT NULL,
    direct_route BOOLEAN NOT NULL,
    batch_size INTEGER NOT NULL CHECK (batch_size BETWEEN 1 AND 20),
    interval_seconds DOUBLE PRECISION NOT NULL CHECK (interval_seconds > 0),
    clean_observations INTEGER NOT NULL DEFAULT 0,
    rate_limit_count INTEGER NOT NULL DEFAULT 0,
    network_error_count INTEGER NOT NULL DEFAULT 0,
    validated_at DOUBLE PRECISION NOT NULL,
    invalidated_at DOUBLE PRECISION,
    PRIMARY KEY (credential_key, marketplace_id, region, direct_route, batch_size)
);

ALTER TABLE credential_governor
    ADD COLUMN IF NOT EXISTS recovery_floor_seconds DOUBLE PRECISION
    NOT NULL DEFAULT 5;

ALTER TABLE credential_governor
    ADD COLUMN IF NOT EXISTS last_rate_limited_at DOUBLE PRECISION
    NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS credential_cadence_calibrations_lookup_idx
    ON credential_cadence_calibrations (
        credential_key, marketplace_id, region, direct_route, batch_size, validated_at DESC
    );
