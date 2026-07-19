CREATE INDEX IF NOT EXISTS alert_deliveries_active_lease_idx
    ON alert_deliveries (leased_until, target_id)
    WHERE status = 'leased';
