/*
These tables are private monitor infrastructure. They are intentionally
inaccessible through Supabase/PostgREST API roles. The application connects
as the owning database role, which continues to bypass RLS.
*/
ALTER TABLE public.monitor_schema_migrations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.credential_governor ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.credential_cadence_calibrations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.product_states ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.stock_transitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.stock_verification_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.alert_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.alert_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.alert_target_circuits ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.alert_delivery_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.alert_dead_letters ENABLE ROW LEVEL SECURITY;

DO $monitor_security$
DECLARE
    api_role TEXT;
    monitor_table TEXT;
    monitor_sequence TEXT;
BEGIN
    FOR api_role IN
        SELECT rolname
        FROM pg_roles
        WHERE rolname IN ('anon', 'authenticated')
    LOOP
        FOREACH monitor_table IN ARRAY ARRAY[
            'monitor_schema_migrations',
            'credential_governor',
            'credential_cadence_calibrations',
            'product_states',
            'stock_transitions',
            'stock_verification_jobs',
            'alert_events',
            'alert_deliveries',
            'alert_target_circuits',
            'alert_delivery_attempts',
            'alert_dead_letters'
        ]
        LOOP
            EXECUTE format(
                'REVOKE ALL PRIVILEGES ON TABLE public.%I FROM %I',
                monitor_table,
                api_role
            );
        END LOOP;

        FOREACH monitor_sequence IN ARRAY ARRAY[
            'alert_delivery_attempts_attempt_id_seq',
            'alert_dead_letters_dead_letter_id_seq'
        ]
        LOOP
            EXECUTE format(
                'REVOKE ALL PRIVILEGES ON SEQUENCE public.%I FROM %I',
                monitor_sequence,
                api_role
            );
        END LOOP;

        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA public '
            'REVOKE ALL PRIVILEGES ON TABLES FROM %I',
            api_role
        );
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA public '
            'REVOKE ALL PRIVILEGES ON SEQUENCES FROM %I',
            api_role
        );
    END LOOP;
END;
$monitor_security$;
