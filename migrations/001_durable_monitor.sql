CREATE TABLE IF NOT EXISTS monitor_schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS credential_governor (
    credential_key TEXT PRIMARY KEY,
    interval_seconds DOUBLE PRECISION NOT NULL,
    next_request_at DOUBLE PRECISION NOT NULL DEFAULT 0,
    blocked_until DOUBLE PRECISION NOT NULL DEFAULT 0,
    generation BIGINT NOT NULL DEFAULT 0,
    consecutive_429 INTEGER NOT NULL DEFAULT 0,
    success_streak INTEGER NOT NULL DEFAULT 0,
    half_open_pending BOOLEAN NOT NULL DEFAULT FALSE,
    lease_owner TEXT,
    lease_expires_at DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS credential_governor_lease_idx
    ON credential_governor (lease_expires_at);

CREATE TABLE IF NOT EXISTS product_states (
    monitor_id TEXT NOT NULL,
    marketplace_id TEXT NOT NULL,
    asin TEXT NOT NULL,
    seller_policy_hash TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'UNKNOWN'
        CHECK (state IN (
            'UNKNOWN',
            'OUT_OF_STOCK_CANDIDATE',
            'OUT_OF_STOCK',
            'BUYABLE_UNCONFIRMED',
            'IN_STOCK_CONFIRMED',
            'SUPPRESSED'
        )),
    stock_epoch BIGINT NOT NULL DEFAULT 0,
    oos_streak INTEGER NOT NULL DEFAULT 0,
    oos_candidate_since TIMESTAMPTZ,
    armed_for_restock BOOLEAN NOT NULL DEFAULT FALSE,
    last_sequence BIGINT NOT NULL DEFAULT 0,
    last_observed_at TIMESTAMPTZ,
    last_evidence_hash TEXT,
    last_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    primed BOOLEAN NOT NULL DEFAULT FALSE,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (monitor_id, marketplace_id, asin, seller_policy_hash)
);

CREATE INDEX IF NOT EXISTS product_states_state_idx
    ON product_states (monitor_id, state);

CREATE TABLE IF NOT EXISTS stock_transitions (
    transition_id UUID PRIMARY KEY,
    monitor_id TEXT NOT NULL,
    marketplace_id TEXT NOT NULL,
    asin TEXT NOT NULL,
    seller_policy_hash TEXT NOT NULL,
    stock_epoch BIGINT NOT NULL,
    signal_type TEXT NOT NULL,
    confirmed BOOLEAN NOT NULL,
    evidence_hash TEXT NOT NULL,
    evidence JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
        monitor_id,
        marketplace_id,
        asin,
        seller_policy_hash,
        stock_epoch,
        signal_type
    )
);

CREATE INDEX IF NOT EXISTS stock_transitions_scope_idx
    ON stock_transitions (monitor_id, marketplace_id, asin, created_at DESC);

CREATE TABLE IF NOT EXISTS stock_verification_jobs (
    job_id UUID PRIMARY KEY,
    monitor_id TEXT NOT NULL,
    marketplace_id TEXT NOT NULL,
    asin TEXT NOT NULL,
    seller_policy_hash TEXT NOT NULL,
    source_sequence BIGINT NOT NULL,
    evidence JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'leased', 'complete', 'expired', 'failed')),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL,
    leased_by TEXT,
    leased_until TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
        monitor_id,
        marketplace_id,
        asin,
        seller_policy_hash,
        source_sequence
    )
);

CREATE INDEX IF NOT EXISTS stock_verification_jobs_due_idx
    ON stock_verification_jobs (next_attempt_at, expires_at)
    WHERE status IN ('pending', 'leased');

CREATE UNIQUE INDEX IF NOT EXISTS stock_verification_jobs_active_scope_idx
    ON stock_verification_jobs (
        monitor_id, marketplace_id, asin, seller_policy_hash
    )
    WHERE status IN ('pending', 'leased');

CREATE TABLE IF NOT EXISTS alert_events (
    alert_id UUID PRIMARY KEY,
    transition_id UUID NOT NULL REFERENCES stock_transitions (transition_id),
    payload_version INTEGER NOT NULL DEFAULT 1,
    payload JSONB NOT NULL,
    lifecycle TEXT NOT NULL DEFAULT 'accepted'
        CHECK (lifecycle IN (
            'accepted',
            'partially_delivered',
            'delivered',
            'dead_lettered',
            'suppressed'
        )),
    trace_context TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (transition_id, payload_version)
);

CREATE TABLE IF NOT EXISTS alert_deliveries (
    delivery_id UUID PRIMARY KEY,
    alert_id UUID NOT NULL REFERENCES alert_events (alert_id) ON DELETE CASCADE,
    target_id TEXT NOT NULL,
    target_kind TEXT NOT NULL CHECK (target_kind IN ('discord', 'generic')),
    payload_version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending',
            'leased',
            'retry_scheduled',
            'succeeded',
            'dead_lettered',
            'suppressed'
        )),
    attempts INTEGER NOT NULL DEFAULT 0,
    previous_backoff_seconds DOUBLE PRECISION,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    leased_by TEXT,
    leased_until TIMESTAMPTZ,
    first_attempt_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    last_error_class TEXT,
    last_http_status INTEGER,
    remote_request_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (alert_id, target_id, payload_version)
);

CREATE INDEX IF NOT EXISTS alert_deliveries_due_idx
    ON alert_deliveries (next_attempt_at, created_at)
    WHERE status IN ('pending', 'retry_scheduled', 'leased');

CREATE INDEX IF NOT EXISTS alert_deliveries_target_idx
    ON alert_deliveries (target_id, status);

CREATE TABLE IF NOT EXISTS alert_target_circuits (
    target_id TEXT PRIMARY KEY,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    open_until TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS alert_target_circuits_open_idx
    ON alert_target_circuits (open_until)
    WHERE open_until IS NOT NULL;

CREATE TABLE IF NOT EXISTS alert_delivery_attempts (
    attempt_id BIGSERIAL PRIMARY KEY,
    delivery_id UUID NOT NULL REFERENCES alert_deliveries (delivery_id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    error_class TEXT,
    http_status INTEGER,
    duration_ms DOUBLE PRECISION NOT NULL,
    retry_after_seconds DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (delivery_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS alert_dead_letters (
    dead_letter_id BIGSERIAL PRIMARY KEY,
    delivery_id UUID NOT NULL UNIQUE REFERENCES alert_deliveries (delivery_id) ON DELETE CASCADE,
    reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    target_id TEXT NOT NULL,
    replay_count INTEGER NOT NULL DEFAULT 0,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS alert_dead_letters_expiry_idx
    ON alert_dead_letters (expires_at);
