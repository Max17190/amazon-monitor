import json
import hashlib
import logging
import os
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional
from uuid import UUID, uuid4

from credential_governor import CredentialLeaseFenceLost


try:
    import asyncpg
except ImportError:  # pragma: no cover - exercised only before dependencies install
    asyncpg = None


MIGRATION_LOCK_ID = 0x415A4D4F4E
DELIVERY_CLAIM_LOCK_ID = 0x414C455254
DEFAULT_MIGRATIONS_DIR = Path(__file__).with_name("migrations")


class DurableStoreError(RuntimeError):
    pass


class StateConflict(DurableStoreError):
    pass


@dataclass(frozen=True)
class ScopeKey:
    monitor_id: str
    marketplace_id: str
    asin: str
    seller_policy_hash: str


@dataclass(frozen=True)
class TransitionWrite:
    transition_id: UUID
    stock_epoch: int
    signal_type: str
    confirmed: bool
    evidence_hash: str
    evidence: Mapping[str, Any]
    detected_at: datetime


@dataclass(frozen=True)
class AlertWrite:
    alert_id: UUID
    payload: Mapping[str, Any]
    payload_version: int = 1
    trace_context: Optional[str] = None


@dataclass(frozen=True)
class TargetWrite:
    target_id: str
    target_kind: str
    delivery_id: UUID


def utc_now():
    return datetime.now(timezone.utc)


def time_ns_epoch():
    return int(utc_now().timestamp() * 1_000_000)


def _jsonable(value):
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if hasattr(value, "value"):
        return value.value
    return value


def _datetime_value(value):
    if value is None or isinstance(value, datetime):
        return value
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


class PostgresStore:
    """Postgres authority for state, transitions, and alert delivery work."""

    def __init__(self, pool, migrations_dir=None):
        self.pool = pool
        self.migrations_dir = Path(migrations_dir or DEFAULT_MIGRATIONS_DIR)

    @classmethod
    async def connect(cls, dsn=None, min_size=1, max_size=10, migrations_dir=None):
        if asyncpg is None:
            raise DurableStoreError(
                "asyncpg is required for durable mode; install requirements.txt"
            )
        dsn = dsn or os.getenv("DATABASE_URL")
        if not dsn:
            raise DurableStoreError("DATABASE_URL is required")
        pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=max(1, int(min_size)),
            max_size=max(1, int(max_size)),
            command_timeout=10,
        )
        return cls(pool, migrations_dir=migrations_dir)

    async def close(self):
        await self.pool.close()

    async def ping(self):
        try:
            async with self.pool.acquire() as connection:
                return bool(await connection.fetchval("SELECT TRUE"))
        except Exception:
            logging.exception("Postgres health check failed")
            return False

    async def migrate(self):
        migration_files = sorted(self.migrations_dir.glob("*.sql"))
        async with self.pool.acquire() as connection:
            await connection.execute(
                "SELECT pg_advisory_lock($1::bigint)", MIGRATION_LOCK_ID
            )
            try:
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS monitor_schema_migrations (
                        version INTEGER PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                    )
                    """
                )
                applied = {
                    row["version"]
                    for row in await connection.fetch(
                        "SELECT version FROM monitor_schema_migrations"
                    )
                }
                for path in migration_files:
                    prefix = path.name.split("_", 1)[0]
                    try:
                        version = int(prefix)
                    except ValueError as exc:
                        raise DurableStoreError(
                            f"migration filename must start with a number: {path.name}"
                        ) from exc
                    if version in applied:
                        continue
                    sql = path.read_text(encoding="utf-8")
                    async with connection.transaction():
                        await connection.execute(sql)
                        await connection.execute(
                            """
                            INSERT INTO monitor_schema_migrations (version)
                            VALUES ($1)
                            ON CONFLICT (version) DO NOTHING
                            """,
                            version,
                        )
            finally:
                await connection.execute(
                    "SELECT pg_advisory_unlock($1::bigint)", MIGRATION_LOCK_ID
                )

    async def load_product_state(self, scope):
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT *
                FROM product_states
                WHERE monitor_id = $1
                  AND marketplace_id = $2
                  AND asin = $3
                  AND seller_policy_hash = $4
                """,
                scope.monitor_id,
                scope.marketplace_id,
                scope.asin,
                scope.seller_policy_hash,
            )
        if not row:
            return None
        value = dict(row)
        return {
            "scope_key": (
                f"{scope.monitor_id}:{scope.marketplace_id}:"
                f"{scope.asin}:{scope.seller_policy_hash}"
            ),
            "state": value["state"],
            "last_sequence": value["last_sequence"],
            "last_observed_at": (
                value["last_observed_at"].isoformat()
                if value["last_observed_at"]
                else None
            ),
            "last_evidence_hash": value["last_evidence_hash"],
            "seller_policy_hash": scope.seller_policy_hash,
            "strong_oos_count": value["oos_streak"],
            "last_strong_oos_at": (
                value["oos_candidate_since"].isoformat()
                if value["oos_candidate_since"]
                else None
            ),
            "epoch": value["stock_epoch"],
            "armed_for_restock": value["armed_for_restock"],
            "initialized": value["primed"],
            "version": value["version"],
            "last_evidence": value["last_evidence"],
        }

    async def commit_stock_decision(
        self,
        scope,
        state_record,
        expected_version,
        transition=None,
        alert=None,
        targets=(),
        evidence=None,
        lease_credential_key=None,
        lease_owner=None,
    ):
        """Atomically persist state plus an optional transition and deliveries.

        The caller computes a decision from `load_product_state`. Optimistic
        version matching forces a retry when another replica wins the race.
        """
        state = _jsonable(state_record)
        targets = tuple(targets)
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                if lease_credential_key is not None or lease_owner is not None:
                    if not lease_credential_key or not lease_owner:
                        raise ValueError(
                            "lease_credential_key and lease_owner are required together"
                        )
                    # Lock the credential row through this state transaction.
                    # A successor cannot acquire it between this check and the
                    # durable state/outbox commit, while an expired leader is
                    # rejected before it can mutate monitor state.
                    lease = await connection.fetchrow(
                        """
                        SELECT credential_key
                        FROM credential_governor
                        WHERE credential_key = $1
                          AND lease_owner = $2
                          AND lease_expires_at > EXTRACT(EPOCH FROM clock_timestamp())
                        FOR UPDATE
                        """,
                        lease_credential_key,
                        lease_owner,
                    )
                    if lease is None:
                        raise CredentialLeaseFenceLost(
                            "TVSS credential leader lease is no longer held"
                        )
                if expected_version is None:
                    inserted = await connection.fetchrow(
                        """
                        INSERT INTO product_states (
                            monitor_id, marketplace_id, asin, seller_policy_hash,
                            state, stock_epoch, oos_streak, oos_candidate_since,
                            last_sequence, last_observed_at, last_evidence_hash,
                            last_evidence, primed, armed_for_restock, version,
                            updated_at
                        )
                        VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8,
                            $9, $10, $11, $12::jsonb, $13, $14, 1,
                            clock_timestamp()
                        )
                        ON CONFLICT DO NOTHING
                        RETURNING version
                        """,
                        scope.monitor_id,
                        scope.marketplace_id,
                        scope.asin,
                        scope.seller_policy_hash,
                        state["state"],
                        int(state.get("epoch", 0)),
                        int(state.get("strong_oos_count", 0)),
                        _datetime_value(state.get("last_strong_oos_at")),
                        int(state.get("last_sequence", 0)),
                        _datetime_value(state.get("last_observed_at")),
                        state.get("last_evidence_hash"),
                        json.dumps(_jsonable(evidence or {})),
                        bool(state.get("initialized", False)),
                        bool(state.get("armed_for_restock", False)),
                    )
                    if inserted is None:
                        raise StateConflict("product state was concurrently created")
                    next_version = int(inserted["version"])
                else:
                    updated = await connection.fetchrow(
                        """
                        UPDATE product_states
                        SET state = $5,
                            stock_epoch = $6,
                            oos_streak = $7,
                            oos_candidate_since = $8,
                            last_sequence = $9,
                            last_observed_at = $10,
                            last_evidence_hash = $11,
                            last_evidence = $12::jsonb,
                            primed = $13,
                            armed_for_restock = $14,
                            version = version + 1,
                            updated_at = clock_timestamp()
                        WHERE monitor_id = $1
                          AND marketplace_id = $2
                          AND asin = $3
                          AND seller_policy_hash = $4
                          AND version = $15
                        RETURNING version
                        """,
                        scope.monitor_id,
                        scope.marketplace_id,
                        scope.asin,
                        scope.seller_policy_hash,
                        state["state"],
                        int(state.get("epoch", 0)),
                        int(state.get("strong_oos_count", 0)),
                        _datetime_value(state.get("last_strong_oos_at")),
                        int(state.get("last_sequence", 0)),
                        _datetime_value(state.get("last_observed_at")),
                        state.get("last_evidence_hash"),
                        json.dumps(_jsonable(evidence or {})),
                        bool(state.get("initialized", False)),
                        bool(state.get("armed_for_restock", False)),
                        int(expected_version),
                    )
                    if updated is None:
                        raise StateConflict("product state version changed")
                    next_version = int(updated["version"])

                created_transition = False
                if transition is not None:
                    inserted_transition = await connection.fetchrow(
                        """
                        INSERT INTO stock_transitions (
                            transition_id, monitor_id, marketplace_id, asin,
                            seller_policy_hash, stock_epoch, signal_type,
                            confirmed, evidence_hash, evidence, detected_at
                        )
                        VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11
                        )
                        ON CONFLICT (
                            monitor_id, marketplace_id, asin, seller_policy_hash,
                            stock_epoch, signal_type
                        ) DO NOTHING
                        RETURNING transition_id
                        """,
                        transition.transition_id,
                        scope.monitor_id,
                        scope.marketplace_id,
                        scope.asin,
                        scope.seller_policy_hash,
                        transition.stock_epoch,
                        transition.signal_type,
                        transition.confirmed,
                        transition.evidence_hash,
                        json.dumps(_jsonable(transition.evidence)),
                        transition.detected_at,
                    )
                    created_transition = inserted_transition is not None

                if created_transition and alert is not None:
                    await connection.execute(
                        """
                        INSERT INTO alert_events (
                            alert_id, transition_id, payload_version, payload,
                            trace_context
                        )
                        VALUES ($1, $2, $3, $4::jsonb, $5)
                        """,
                        alert.alert_id,
                        transition.transition_id,
                        alert.payload_version,
                        json.dumps(_jsonable(alert.payload)),
                        alert.trace_context,
                    )
                    for target in targets:
                        await connection.execute(
                            """
                            INSERT INTO alert_deliveries (
                                delivery_id, alert_id, target_id, target_kind,
                                payload_version
                            )
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (alert_id, target_id, payload_version)
                            DO NOTHING
                            """,
                            target.delivery_id,
                            alert.alert_id,
                            target.target_id,
                            target.target_kind,
                            alert.payload_version,
                        )
                return {
                    "version": next_version,
                    "transition_created": created_transition,
                    "deliveries_created": len(targets)
                    if created_transition and alert is not None
                    else 0,
                }

    async def enqueue_verification(
        self,
        scope,
        source_sequence,
        evidence,
        ttl_seconds=30.0,
    ):
        job_id = uuid4()
        expires_at = utc_now() + timedelta(seconds=float(ttl_seconds))
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                INSERT INTO stock_verification_jobs (
                    job_id, monitor_id, marketplace_id, asin,
                    seller_policy_hash, source_sequence, evidence, expires_at
                )
                SELECT $1, $2, $3, $4, $5, $6, $7::jsonb, $8
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM stock_verification_jobs
                    WHERE monitor_id = $2
                      AND marketplace_id = $3
                      AND asin = $4
                      AND seller_policy_hash = $5
                      AND created_at > clock_timestamp()
                          - ($9::double precision * INTERVAL '1 second')
                )
                ON CONFLICT DO NOTHING
                RETURNING job_id
                """,
                job_id,
                scope.monitor_id,
                scope.marketplace_id,
                scope.asin,
                scope.seller_policy_hash,
                int(source_sequence),
                json.dumps(_jsonable(evidence)),
                expires_at,
                float(ttl_seconds),
            )
        return row["job_id"] if row else None

    async def enqueue_system_alert(
        self,
        monitor_id,
        marketplace_id,
        signal_type,
        payload,
        targets,
    ):
        transition_id = uuid4()
        alert_id = uuid4()
        detected_at = utc_now()
        epoch = time_ns_epoch()
        payload = dict(_jsonable(payload))
        payload.update(
            {
                "alert_id": str(alert_id),
                "transition_id": str(transition_id),
                "stock_epoch": epoch,
            }
        )
        evidence_hash = hashlib.sha256(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO stock_transitions (
                        transition_id, monitor_id, marketplace_id, asin,
                        seller_policy_hash, stock_epoch, signal_type,
                        confirmed, evidence_hash, evidence, detected_at
                    )
                    VALUES (
                        $1, $2, $3, 'SYSTEM', 'system', $4, $5,
                        TRUE, $6, $7::jsonb, $8
                    )
                    """,
                    transition_id,
                    monitor_id,
                    marketplace_id,
                    epoch,
                    signal_type,
                    evidence_hash,
                    json.dumps(payload),
                    detected_at,
                )
                await connection.execute(
                    """
                    INSERT INTO alert_events (
                        alert_id, transition_id, payload_version, payload
                    )
                    VALUES ($1, $2, 1, $3::jsonb)
                    """,
                    alert_id,
                    transition_id,
                    json.dumps(payload),
                )
                for target in targets:
                    await connection.execute(
                        """
                        INSERT INTO alert_deliveries (
                            delivery_id, alert_id, target_id, target_kind,
                            payload_version
                        )
                        VALUES ($1, $2, $3, $4, 1)
                        ON CONFLICT DO NOTHING
                        """,
                        target.delivery_id,
                        alert_id,
                        target.target_id,
                        target.target_kind,
                    )
        return alert_id

    async def claim_verification_jobs(
        self,
        worker_id,
        monitor_id,
        marketplace_id,
        seller_policy_hash,
        limit=1,
        lease_seconds=30.0,
    ):
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                WITH due AS (
                    SELECT job_id
                    FROM stock_verification_jobs
                    WHERE (
                        status = 'pending'
                        OR (status = 'leased' AND leased_until < clock_timestamp())
                    )
                      AND monitor_id = $2
                      AND marketplace_id = $3
                      AND seller_policy_hash = $4
                      AND next_attempt_at <= clock_timestamp()
                      AND expires_at > clock_timestamp()
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT $1
                )
                UPDATE stock_verification_jobs AS job
                SET status = 'leased',
                    leased_by = $5,
                    leased_until = clock_timestamp()
                        + ($6::double precision * INTERVAL '1 second'),
                    attempts = job.attempts + 1,
                    updated_at = clock_timestamp()
                FROM due
                WHERE job.job_id = due.job_id
                RETURNING job.*
                """,
                int(limit),
                str(monitor_id),
                str(marketplace_id),
                str(seller_policy_hash),
                str(worker_id),
                float(lease_seconds),
            )
            await connection.execute(
                """
                UPDATE stock_verification_jobs
                SET status = 'expired',
                    leased_by = NULL,
                    leased_until = NULL,
                    updated_at = clock_timestamp()
                WHERE status IN ('pending', 'leased')
                  AND expires_at <= clock_timestamp()
                """
            )
        return [dict(row) for row in rows]

    async def finish_verification(
        self,
        job_id,
        worker_id,
        success,
        retryable=False,
    ):
        """Finish a verification only while this worker still owns its lease.

        A lease can expire while an Amazon request is in flight.  The job may
        then be reclaimed by another worker, so an old response must not alter
        the newer owner's job state.
        """
        status = "complete" if success else ("pending" if retryable else "failed")
        async with self.pool.acquire() as connection:
            result = await connection.execute(
                """
                UPDATE stock_verification_jobs
                SET status = $2,
                    next_attempt_at = CASE
                        WHEN $2 = 'pending'
                        THEN clock_timestamp() + INTERVAL '5 seconds'
                        ELSE next_attempt_at
                    END,
                    leased_by = NULL,
                    leased_until = NULL,
                    updated_at = clock_timestamp()
                WHERE job_id = $1
                  AND status = 'leased'
                  AND leased_by = $3
                  AND leased_until > clock_timestamp()
                """,
                job_id,
                status,
                str(worker_id),
            )
        return result.endswith("1")

    async def claim_deliveries(
        self,
        worker_id,
        limit,
        lease_seconds,
        global_limit=32,
        per_target_limit=2,
    ):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock($1::bigint)",
                    DELIVERY_CLAIM_LOCK_ID,
                )
                active_rows = await connection.fetch(
                    """
                    SELECT target_id, COUNT(*) AS count
                    FROM alert_deliveries
                    WHERE status = 'leased'
                      AND leased_until > clock_timestamp()
                    GROUP BY target_id
                    """
                )
                active_by_target = {
                    row["target_id"]: int(row["count"])
                    for row in active_rows
                }
                available_global = max(
                    0,
                    int(global_limit) - sum(active_by_target.values()),
                )
                claim_count = min(int(limit), available_global)
                if claim_count <= 0:
                    return []
                candidates = await connection.fetch(
                    """
                    SELECT delivery.delivery_id, delivery.target_id
                    FROM alert_deliveries AS delivery
                    WHERE (
                        delivery.status IN ('pending', 'retry_scheduled')
                        OR (
                            delivery.status = 'leased'
                            AND delivery.leased_until < clock_timestamp()
                        )
                    )
                      AND delivery.next_attempt_at <= clock_timestamp()
                      AND NOT EXISTS (
                          SELECT 1
                          FROM alert_target_circuits AS circuit
                          WHERE circuit.target_id = delivery.target_id
                            AND circuit.open_until > clock_timestamp()
                      )
                    ORDER BY delivery.next_attempt_at, delivery.created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT $1
                    """,
                    max(claim_count * 8, claim_count),
                )
                chosen = []
                for candidate in candidates:
                    target_id = candidate["target_id"]
                    target_active = active_by_target.get(target_id, 0)
                    if target_active >= int(per_target_limit):
                        continue
                    chosen.append(candidate["delivery_id"])
                    active_by_target[target_id] = target_active + 1
                    if len(chosen) >= claim_count:
                        break
                if not chosen:
                    return []
                rows = await connection.fetch(
                    """
                    UPDATE alert_deliveries AS delivery
                    SET status = 'leased',
                        leased_by = $2,
                        leased_until = clock_timestamp()
                            + ($3::double precision * INTERVAL '1 second'),
                        attempts = delivery.attempts + 1,
                        first_attempt_at = COALESCE(
                            delivery.first_attempt_at, clock_timestamp()
                        ),
                        last_attempt_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    FROM alert_events AS alert
                    WHERE delivery.delivery_id = ANY($1::uuid[])
                      AND alert.alert_id = delivery.alert_id
                    RETURNING
                        delivery.*,
                        alert.payload,
                        alert.created_at AS alert_created_at,
                        alert.trace_context
                    """,
                    chosen,
                    str(worker_id),
                    float(lease_seconds),
                )
        return [dict(row) for row in rows]

    async def mark_delivery_succeeded(
        self,
        delivery_id,
        duration_ms,
        remote_request_id=None,
    ):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    UPDATE alert_deliveries
                    SET status = 'succeeded',
                        leased_by = NULL,
                        leased_until = NULL,
                        delivered_at = clock_timestamp(),
                        remote_request_id = $2,
                        last_error_class = NULL,
                        last_http_status = NULL,
                        updated_at = clock_timestamp()
                    WHERE delivery_id = $1
                    RETURNING alert_id, attempts, target_id
                    """,
                    delivery_id,
                    remote_request_id,
                )
                if row:
                    await connection.execute(
                        """
                        INSERT INTO alert_target_circuits (
                            target_id, consecutive_failures, open_until
                        )
                        VALUES ($1, 0, NULL)
                        ON CONFLICT (target_id) DO UPDATE
                        SET consecutive_failures = 0,
                            open_until = NULL,
                            updated_at = clock_timestamp()
                        """,
                        row["target_id"],
                    )
                    await self._record_attempt(
                        connection,
                        delivery_id,
                        row["attempts"],
                        "succeeded",
                        duration_ms,
                    )
                    await self._refresh_alert_lifecycle(connection, row["alert_id"])

    async def reschedule_delivery(
        self,
        delivery_id,
        delay_seconds,
        duration_ms,
        error_class,
        http_status=None,
        retry_after_seconds=None,
        previous_backoff_seconds=None,
        circuit_failure_threshold=5,
        circuit_open_seconds=60.0,
    ):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    UPDATE alert_deliveries
                    SET status = 'retry_scheduled',
                        next_attempt_at = clock_timestamp()
                            + ($2::double precision * INTERVAL '1 second'),
                        previous_backoff_seconds = $3,
                        leased_by = NULL,
                        leased_until = NULL,
                        last_error_class = $4,
                        last_http_status = $5,
                        updated_at = clock_timestamp()
                    WHERE delivery_id = $1
                    RETURNING alert_id, attempts, target_id
                    """,
                    delivery_id,
                    float(delay_seconds),
                    previous_backoff_seconds,
                    error_class,
                    http_status,
                )
                if row:
                    await connection.execute(
                        """
                        INSERT INTO alert_target_circuits (
                            target_id, consecutive_failures, open_until
                        )
                        VALUES ($1, 1, NULL)
                        ON CONFLICT (target_id) DO UPDATE
                        SET consecutive_failures =
                                alert_target_circuits.consecutive_failures + 1,
                            open_until = CASE
                                WHEN alert_target_circuits.consecutive_failures
                                        + 1 >= $2
                                THEN GREATEST(
                                    COALESCE(
                                        alert_target_circuits.open_until,
                                        clock_timestamp()
                                    ),
                                    clock_timestamp()
                                        + (
                                            $3::double precision
                                            * INTERVAL '1 second'
                                        )
                                )
                                ELSE alert_target_circuits.open_until
                            END,
                            updated_at = clock_timestamp()
                        """,
                        row["target_id"],
                        int(circuit_failure_threshold),
                        float(circuit_open_seconds),
                    )
                    await self._record_attempt(
                        connection,
                        delivery_id,
                        row["attempts"],
                        "retry_scheduled",
                        duration_ms,
                        error_class=error_class,
                        http_status=http_status,
                        retry_after_seconds=retry_after_seconds,
                    )
                    await self._refresh_alert_lifecycle(connection, row["alert_id"])

    async def dead_letter_delivery(
        self,
        delivery_id,
        reason,
        duration_ms,
        error_class=None,
        http_status=None,
        retention_days=30,
    ):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    UPDATE alert_deliveries AS delivery
                    SET status = 'dead_lettered',
                        leased_by = NULL,
                        leased_until = NULL,
                        last_error_class = $2,
                        last_http_status = $3,
                        updated_at = clock_timestamp()
                    FROM alert_events AS alert
                    WHERE delivery.delivery_id = $1
                      AND alert.alert_id = delivery.alert_id
                    RETURNING
                        delivery.alert_id,
                        delivery.attempts,
                        delivery.target_id,
                        alert.payload
                    """,
                    delivery_id,
                    error_class,
                    http_status,
                )
                if not row:
                    return
                await self._record_attempt(
                    connection,
                    delivery_id,
                    row["attempts"],
                    "dead_lettered",
                    duration_ms,
                    error_class=error_class,
                    http_status=http_status,
                )
                await connection.execute(
                    """
                    INSERT INTO alert_dead_letters (
                        delivery_id, reason, payload, target_id, expires_at
                    )
                    VALUES (
                        $1, $2, $3::jsonb, $4,
                        clock_timestamp() + ($5::integer * INTERVAL '1 day')
                    )
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET reason = EXCLUDED.reason,
                        dead_lettered_at = clock_timestamp(),
                        expires_at = EXCLUDED.expires_at
                    """,
                    delivery_id,
                    reason,
                    json.dumps(_jsonable(row["payload"])),
                    row["target_id"],
                    int(retention_days),
                )
                await self._refresh_alert_lifecycle(connection, row["alert_id"])

    async def list_dead_letters(self, limit=100):
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT dead_letter_id, delivery_id, reason, target_id,
                       replay_count, dead_lettered_at, expires_at
                FROM alert_dead_letters
                ORDER BY dead_lettered_at DESC
                LIMIT $1
                """,
                int(limit),
            )
        return [dict(row) for row in rows]

    async def replay_delivery(self, delivery_id):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    UPDATE alert_deliveries
                    SET status = 'pending',
                        attempts = 0,
                        previous_backoff_seconds = NULL,
                        next_attempt_at = clock_timestamp(),
                        leased_by = NULL,
                        leased_until = NULL,
                        last_error_class = NULL,
                        last_http_status = NULL,
                        updated_at = clock_timestamp()
                    WHERE delivery_id = $1
                      AND status = 'dead_lettered'
                    RETURNING alert_id
                    """,
                    delivery_id,
                )
                if row is None:
                    return False
                await connection.execute(
                    """
                    UPDATE alert_dead_letters
                    SET replay_count = replay_count + 1
                    WHERE delivery_id = $1
                    """,
                    delivery_id,
                )
                await self._refresh_alert_lifecycle(
                    connection,
                    row["alert_id"],
                )
                return True

    async def suppress_delivery(self, delivery_id):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    UPDATE alert_deliveries
                    SET status = 'suppressed',
                        leased_by = NULL,
                        leased_until = NULL,
                        updated_at = clock_timestamp()
                    WHERE delivery_id = $1
                      AND status <> 'succeeded'
                    RETURNING alert_id
                    """,
                    delivery_id,
                )
                if row is None:
                    return False
                await self._refresh_alert_lifecycle(
                    connection,
                    row["alert_id"],
                )
                return True

    async def delivery_backlog(self):
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE status IN ('pending', 'retry_scheduled', 'leased')
                    ) AS active,
                    COUNT(*) FILTER (
                        WHERE status = 'dead_lettered'
                    ) AS dead_lettered,
                    EXTRACT(
                        EPOCH FROM (
                            clock_timestamp() - MIN(created_at) FILTER (
                                WHERE status IN (
                                    'pending', 'retry_scheduled', 'leased'
                                )
                            )
                        )
                    ) AS oldest_age_seconds
                FROM alert_deliveries
                """
            )
            open_circuits = await connection.fetchval(
                """
                SELECT COUNT(*)
                FROM alert_target_circuits
                WHERE open_until > clock_timestamp()
                """
            )
        result = dict(row)
        result["open_circuits"] = int(open_circuits or 0)
        return result

    async def cleanup_expired_dead_letters(self):
        async with self.pool.acquire() as connection:
            result = await connection.execute(
                """
                DELETE FROM alert_dead_letters
                WHERE expires_at <= clock_timestamp()
                """
            )
        return int(result.rsplit(" ", 1)[-1])

    @staticmethod
    async def _record_attempt(
        connection,
        delivery_id,
        attempt_number,
        outcome,
        duration_ms,
        error_class=None,
        http_status=None,
        retry_after_seconds=None,
    ):
        await connection.execute(
            """
            INSERT INTO alert_delivery_attempts (
                delivery_id, attempt_number, outcome, error_class,
                http_status, duration_ms, retry_after_seconds
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (delivery_id, attempt_number) DO NOTHING
            """,
            delivery_id,
            int(attempt_number),
            outcome,
            error_class,
            http_status,
            float(duration_ms),
            retry_after_seconds,
        )

    @staticmethod
    async def _refresh_alert_lifecycle(connection, alert_id):
        await connection.execute(
            """
            UPDATE alert_events AS alert
            SET lifecycle = CASE
                    WHEN NOT EXISTS (
                        SELECT 1
                        FROM alert_deliveries
                        WHERE alert_id = alert.alert_id
                          AND status <> 'suppressed'
                    ) THEN 'suppressed'
                    WHEN NOT EXISTS (
                        SELECT 1
                        FROM alert_deliveries
                        WHERE alert_id = alert.alert_id
                          AND status NOT IN ('succeeded', 'suppressed')
                    ) THEN 'delivered'
                    WHEN EXISTS (
                        SELECT 1
                        FROM alert_deliveries
                        WHERE alert_id = alert.alert_id
                          AND status = 'dead_lettered'
                    ) THEN 'dead_lettered'
                    WHEN EXISTS (
                        SELECT 1
                        FROM alert_deliveries
                        WHERE alert_id = alert.alert_id
                          AND status = 'succeeded'
                    ) THEN 'partially_delivered'
                    ELSE 'accepted'
                END,
                updated_at = clock_timestamp()
            WHERE alert.alert_id = $1
            """,
            alert_id,
        )


async def connect_and_migrate(dsn=None, pool_max_size=40):
    store = await PostgresStore.connect(
        dsn=dsn,
        min_size=1,
        max_size=max(2, int(pool_max_size)),
    )
    try:
        await store.migrate()
    except Exception:
        await store.close()
        raise
    return store
