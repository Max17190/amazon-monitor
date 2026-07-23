"""Durable, credential-wide request pacing for TVSS clients.

The governor deliberately keys its state by credential rather than network
route.  A proxy must not create another request budget for the same account.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from typing import AsyncIterator, Callable, Optional, Protocol


class RequestClass(str, Enum):
    POLL = "poll"
    CONFIRM = "confirm"
    CANARY = "canary"


class HalfOpenPollRequired(RuntimeError):
    """A cooldown may be exited only by one batch poll."""


class CredentialLeaseFenceLost(RuntimeError):
    """The caller no longer owns the credential lease needed for work."""


@dataclass(frozen=True)
class Permit:
    credential_key: str
    request_class: RequestClass
    scheduled_at: float
    wait_seconds: float
    generation: int
    half_open_probe: bool
    lease_owner: Optional[str] = None
    borrowed: bool = False


@dataclass(frozen=True)
class GovernorSnapshot:
    credential_key: str
    interval_seconds: float
    next_request_at: float
    blocked_until: float
    generation: int
    consecutive_429: int
    success_streak: int
    half_open_pending: bool
    recovery_floor_seconds: float
    last_rate_limited_at: float


class CredentialGovernor(Protocol):
    async def acquire_leader(self, credential_key: str, owner_id: str, ttl_seconds: float) -> bool: ...
    async def renew_leader(self, credential_key: str, owner_id: str, ttl_seconds: float) -> bool: ...
    async def release_leader(self, credential_key: str, owner_id: str) -> bool: ...
    async def ensure_leader(self, credential_key: str, owner_id: str) -> None: ...
    async def acquire_permit(
        self,
        credential_key: str,
        request_class: RequestClass,
        owner_id: Optional[str] = None,
    ) -> Permit: ...
    async def acquire_borrowed_confirmation_permit(
        self,
        credential_key: str,
        owner_id: Optional[str] = None,
    ) -> Permit: ...
    async def record_result(
        self,
        permit: Permit,
        status: Optional[int],
        retry_after_seconds: Optional[float] = None,
        owner_id: Optional[str] = None,
    ) -> GovernorSnapshot: ...
    async def raise_interval_floor(
        self, credential_key: str, interval_seconds: float
    ) -> GovernorSnapshot: ...
    async def set_interval_floor(
        self,
        credential_key: str,
        interval_seconds: float,
        *,
        allow_lower: bool = False,
    ) -> GovernorSnapshot: ...


DEFAULT_INTERVAL_SECONDS = 5.0
BASE_429_COOLDOWN_SECONDS = 900.0
MAX_429_COOLDOWN_SECONDS = 3600.0
MAX_INTERVAL_SECONDS = 300.0
RECOVERY_SUCCESS_COUNT = 120
RECOVERY_DECREMENT_SECONDS = 0.25
PRODUCTION_MIN_INTERVAL_SECONDS = 5.0
CALIBRATION_VALIDITY_SECONDS = 24 * 60 * 60
CALIBRATION_REQUIRED_OBSERVATIONS = 120


@dataclass(frozen=True)
class CalibrationKey:
    """The route-specific identity of a safely calibrated request budget.

    The credential hash remains the primary budget key.  Region, direct route,
    and batch size are included because changing any of them changes the
    observed load and latency envelope.  No raw credential material belongs in
    this type.
    """

    credential_key: str
    marketplace_id: str
    region: str
    direct_route: bool
    batch_size: int

    def __post_init__(self) -> None:
        if not self.credential_key or not self.marketplace_id or not self.region:
            raise ValueError("calibration key requires credential, marketplace, and region")
        if not 1 <= int(self.batch_size) <= 20:
            raise ValueError("calibration batch_size must be between 1 and 20")


@dataclass(frozen=True)
class CalibrationSnapshot:
    key: CalibrationKey
    interval_seconds: float
    clean_observations: int
    rate_limit_count: int
    network_error_count: int
    validated_at: float
    invalidated_at: Optional[float] = None

    def is_valid(self, now: float, validity_seconds: float = CALIBRATION_VALIDITY_SECONDS) -> bool:
        return (
            self.key.direct_route
            and self.interval_seconds < PRODUCTION_MIN_INTERVAL_SECONDS
            and self.clean_observations >= CALIBRATION_REQUIRED_OBSERVATIONS
            and self.rate_limit_count == 0
            and self.network_error_count == 0
            and self.invalidated_at is None
            and now >= self.validated_at
            and now - self.validated_at <= float(validity_seconds)
        )


def production_interval_for_calibration(
    requested_interval_seconds: float,
    calibration: Optional[CalibrationSnapshot],
    *,
    now: float,
    validity_seconds: float = CALIBRATION_VALIDITY_SECONDS,
) -> float:
    """Fail closed to the five-second production floor without a valid record."""
    requested = float(requested_interval_seconds)
    if requested >= PRODUCTION_MIN_INTERVAL_SECONDS:
        return requested
    if (
        calibration is not None
        and calibration.is_valid(now, validity_seconds)
        # A calibration at 1 second may safely authorize 2 seconds, but must
        # never be reused to increase pressure at 0.5 seconds.
        and requested >= calibration.interval_seconds
    ):
        return requested
    return PRODUCTION_MIN_INTERVAL_SECONDS


def stable_credential_key(credential_material: str, salt: str = "") -> str:
    """Return a non-reversible identifier suitable for durable state keys.

    Callers must pass the raw credential only to this function.  The value is
    never logged or retained by either governor implementation.
    """
    if not credential_material:
        raise ValueError("credential_material is required")
    digest = hashlib.sha256()
    digest.update(salt.encode("utf-8"))
    digest.update(b"\0")
    digest.update(credential_material.encode("utf-8"))
    return "tvss-" + digest.hexdigest()


@dataclass
class _State:
    interval_seconds: float
    recovery_floor_seconds: float = DEFAULT_INTERVAL_SECONDS
    next_request_at: float = 0.0
    blocked_until: float = 0.0
    generation: int = 0
    consecutive_429: int = 0
    success_streak: int = 0
    half_open_pending: bool = False
    lease_owner: Optional[str] = None
    lease_expires_at: float = 0.0
    last_rate_limited_at: float = 0.0


def _snapshot(key: str, state: _State) -> GovernorSnapshot:
    return GovernorSnapshot(
        credential_key=key,
        interval_seconds=state.interval_seconds,
        next_request_at=state.next_request_at,
        blocked_until=state.blocked_until,
        generation=state.generation,
        consecutive_429=state.consecutive_429,
        success_streak=state.success_streak,
        half_open_pending=state.half_open_pending,
        recovery_floor_seconds=state.recovery_floor_seconds,
        last_rate_limited_at=state.last_rate_limited_at,
    )


def _reserve(
    key: str,
    state: _State,
    request_class: RequestClass,
    now: float,
    owner_id: Optional[str] = None,
) -> Permit:
    scheduled_at = max(now, state.next_request_at, state.blocked_until)
    if state.half_open_pending and request_class is not RequestClass.POLL:
        raise HalfOpenPollRequired(
            "credential cooldown requires a batch poll before other traffic"
        )
    half_open = state.half_open_pending and scheduled_at >= state.blocked_until
    if half_open:
        state.half_open_pending = False
    state.next_request_at = scheduled_at + state.interval_seconds
    return Permit(
        credential_key=key,
        request_class=request_class,
        scheduled_at=scheduled_at,
        wait_seconds=max(0.0, scheduled_at - now),
        generation=state.generation,
        half_open_probe=half_open,
        lease_owner=owner_id,
    )


def _borrow_confirmation(
    key: str,
    state: _State,
    now: float,
    owner_id: Optional[str] = None,
) -> Permit:
    """Use the next cadence slot now and move the following slot later."""
    if state.half_open_pending or now < state.blocked_until:
        return _reserve(
            key,
            state,
            RequestClass.CONFIRM,
            now,
            owner_id,
        )
    state.next_request_at = max(state.next_request_at, now) + state.interval_seconds
    return Permit(
        credential_key=key,
        request_class=RequestClass.CONFIRM,
        scheduled_at=now,
        wait_seconds=0.0,
        generation=state.generation,
        half_open_probe=False,
        lease_owner=owner_id,
        borrowed=True,
    )


def _record(
    key: str,
    state: _State,
    permit: Permit,
    status: Optional[int],
    retry_after_seconds: Optional[float],
    now: float,
    cooldown_seconds: float,
    max_cooldown_seconds: float,
    max_interval_seconds: float,
    recovery_success_count: int,
    recovery_decrement_seconds: float,
) -> GovernorSnapshot:
    # A 429 invalidates older queued permits.  Their eventual response must
    # never undo the newer cooldown or accelerate recovery.
    if permit.generation != state.generation:
        return _snapshot(key, state)

    if status == 429:
        state.last_rate_limited_at = max(state.last_rate_limited_at, now)
        state.recovery_floor_seconds = max(
            state.recovery_floor_seconds,
            PRODUCTION_MIN_INTERVAL_SECONDS,
        )
        state.consecutive_429 += 1
        state.success_streak = 0
        state.interval_seconds = min(
            state.interval_seconds * 2.0,
            max_interval_seconds,
        )
        retry_after = max(0.0, float(retry_after_seconds or 0.0))
        backoff = max(
            retry_after,
            min(
                max_cooldown_seconds,
                cooldown_seconds
                * (2 ** min(state.consecutive_429 - 1, 2)),
            ),
        )
        state.blocked_until = max(state.blocked_until, now + backoff)
        state.next_request_at = max(state.next_request_at, state.blocked_until)
        state.generation += 1
        state.half_open_pending = True
        return _snapshot(key, state)

    if status is not None and 200 <= status < 300 and permit.request_class is RequestClass.POLL:
        state.consecutive_429 = 0
        recovery_floor = state.recovery_floor_seconds
        if state.interval_seconds > recovery_floor:
            state.success_streak += 1
            if state.success_streak >= recovery_success_count:
                state.interval_seconds = max(
                    recovery_floor,
                    state.interval_seconds - recovery_decrement_seconds,
                )
                state.success_streak = 0
        else:
            state.success_streak = 0
    return _snapshot(key, state)


class InMemoryCredentialGovernor:
    """Deterministic implementation for tests and single-process development."""

    def __init__(
        self,
        clock: Callable[[], float] = time.time,
        base_interval: float = DEFAULT_INTERVAL_SECONDS,
        cooldown_seconds: float = BASE_429_COOLDOWN_SECONDS,
        max_cooldown_seconds: float = MAX_429_COOLDOWN_SECONDS,
        max_interval_seconds: float = MAX_INTERVAL_SECONDS,
        recovery_success_count: int = RECOVERY_SUCCESS_COUNT,
        recovery_decrement_seconds: float = RECOVERY_DECREMENT_SECONDS,
    ):
        self._clock = clock
        self._base_interval = float(base_interval)
        self._cooldown_seconds = float(cooldown_seconds)
        self._max_cooldown_seconds = float(max_cooldown_seconds)
        self._max_interval_seconds = float(max_interval_seconds)
        self._recovery_success_count = int(recovery_success_count)
        self._recovery_decrement_seconds = float(recovery_decrement_seconds)
        self._states: dict[str, _State] = {}
        self._lock = asyncio.Lock()

    def _state(self, key: str) -> _State:
        return self._states.setdefault(
            key,
            _State(
                interval_seconds=self._base_interval,
                recovery_floor_seconds=self._base_interval,
            ),
        )

    async def acquire_leader(self, credential_key: str, owner_id: str, ttl_seconds: float) -> bool:
        async with self._lock:
            state, now = self._state(credential_key), self._clock()
            if state.lease_owner not in (None, owner_id) and state.lease_expires_at > now:
                return False
            state.lease_owner, state.lease_expires_at = owner_id, now + float(ttl_seconds)
            return True

    async def renew_leader(self, credential_key: str, owner_id: str, ttl_seconds: float) -> bool:
        async with self._lock:
            state, now = self._state(credential_key), self._clock()
            if state.lease_owner != owner_id or state.lease_expires_at <= now:
                return False
            state.lease_expires_at = now + float(ttl_seconds)
            return True

    async def release_leader(self, credential_key: str, owner_id: str) -> bool:
        async with self._lock:
            state = self._state(credential_key)
            if state.lease_owner != owner_id:
                return False
            state.lease_owner, state.lease_expires_at = None, 0.0
            return True

    @staticmethod
    def _require_leader(state: _State, owner_id: str, now: float) -> None:
        if state.lease_owner != owner_id or state.lease_expires_at <= now:
            raise CredentialLeaseFenceLost(
                "TVSS credential leader lease is no longer held"
            )

    async def ensure_leader(self, credential_key: str, owner_id: str) -> None:
        async with self._lock:
            self._require_leader(
                self._state(credential_key), owner_id, self._clock()
            )

    async def acquire_permit(
        self,
        credential_key: str,
        request_class: RequestClass,
        owner_id: Optional[str] = None,
    ) -> Permit:
        async with self._lock:
            state, now = self._state(credential_key), self._clock()
            if owner_id is not None:
                self._require_leader(state, owner_id, now)
            return _reserve(credential_key, state, request_class, now, owner_id)

    async def acquire_borrowed_confirmation_permit(
        self,
        credential_key: str,
        owner_id: Optional[str] = None,
    ) -> Permit:
        async with self._lock:
            state, now = self._state(credential_key), self._clock()
            if owner_id is not None:
                self._require_leader(state, owner_id, now)
            return _borrow_confirmation(
                credential_key,
                state,
                now,
                owner_id,
            )

    async def record_result(
        self,
        permit: Permit,
        status: Optional[int],
        retry_after_seconds: Optional[float] = None,
        owner_id: Optional[str] = None,
    ) -> GovernorSnapshot:
        async with self._lock:
            state = self._state(permit.credential_key)
            fence_owner = owner_id or permit.lease_owner
            if fence_owner is not None:
                self._require_leader(state, fence_owner, self._clock())
            return _record(
                permit.credential_key,
                state,
                permit,
                status,
                retry_after_seconds,
                self._clock(),
                self._cooldown_seconds,
                self._max_cooldown_seconds,
                self._max_interval_seconds,
                self._recovery_success_count,
                self._recovery_decrement_seconds,
            )

    async def snapshot(self, credential_key: str) -> GovernorSnapshot:
        async with self._lock:
            return _snapshot(credential_key, self._state(credential_key))

    async def raise_interval_floor(
        self, credential_key: str, interval_seconds: float
    ) -> GovernorSnapshot:
        return await self.set_interval_floor(
            credential_key, interval_seconds, allow_lower=False
        )

    async def set_interval_floor(
        self,
        credential_key: str,
        interval_seconds: float,
        *,
        allow_lower: bool = False,
    ) -> GovernorSnapshot:
        floor = float(interval_seconds)
        async with self._lock:
            self._base_interval = (
                floor if allow_lower else max(self._base_interval, floor)
            )
            state = self._state(credential_key)
            if allow_lower:
                state.recovery_floor_seconds = floor
                state.interval_seconds = floor
                state.success_streak = 0
            else:
                state.recovery_floor_seconds = max(
                    state.recovery_floor_seconds, floor
                )
                state.interval_seconds = max(state.interval_seconds, floor)
            return _snapshot(credential_key, state)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS credential_governor (
    credential_key TEXT PRIMARY KEY,
    interval_seconds DOUBLE PRECISION NOT NULL,
    next_request_at DOUBLE PRECISION NOT NULL,
    blocked_until DOUBLE PRECISION NOT NULL,
    generation BIGINT NOT NULL,
    consecutive_429 INTEGER NOT NULL,
    success_streak INTEGER NOT NULL,
    half_open_pending BOOLEAN NOT NULL,
    lease_owner TEXT,
    lease_expires_at DOUBLE PRECISION NOT NULL,
    recovery_floor_seconds DOUBLE PRECISION NOT NULL DEFAULT 5,
    last_rate_limited_at DOUBLE PRECISION NOT NULL DEFAULT 0
)
"""


CALIBRATION_SCHEMA_SQL = """
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
)
"""


class InMemoryCadenceCalibrationStore:
    """Small deterministic calibration authority used by focused tests."""

    def __init__(self, clock: Callable[[], float] = time.time):
        self._clock = clock
        self._records: dict[CalibrationKey, CalibrationSnapshot] = {}

    async def load(self, key: CalibrationKey) -> Optional[CalibrationSnapshot]:
        return self._records.get(key)

    async def record_validation(
        self,
        key: CalibrationKey,
        interval_seconds: float,
        clean_observations: int,
        rate_limit_count: int = 0,
        network_error_count: int = 0,
        *,
        validated_at: Optional[float] = None,
    ) -> CalibrationSnapshot:
        snapshot = CalibrationSnapshot(
            key=key,
            interval_seconds=float(interval_seconds),
            clean_observations=int(clean_observations),
            rate_limit_count=int(rate_limit_count),
            network_error_count=int(network_error_count),
            validated_at=self._clock() if validated_at is None else float(validated_at),
        )
        self._records[key] = snapshot
        return snapshot

    async def invalidate_credential(
        self, credential_key: str, marketplace_id: str, *, now: Optional[float] = None
    ) -> int:
        invalidated_at = self._clock() if now is None else float(now)
        changed = 0
        for key, value in tuple(self._records.items()):
            if key.credential_key == credential_key and key.marketplace_id == marketplace_id:
                self._records[key] = CalibrationSnapshot(
                    **{**value.__dict__, "invalidated_at": invalidated_at}
                )
                changed += 1
        return changed


class PostgresCadenceCalibrationStore:
    """Durable calibration records; callers retain production lease control."""

    def __init__(self, pool, clock: Callable[[], float] = time.time):
        self._pool = pool
        self._clock = clock

    async def initialize(self) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(CALIBRATION_SCHEMA_SQL)

    @staticmethod
    def _snapshot(row) -> CalibrationSnapshot:
        key = CalibrationKey(
            credential_key=row["credential_key"], marketplace_id=row["marketplace_id"],
            region=row["region"], direct_route=bool(row["direct_route"]),
            batch_size=int(row["batch_size"]),
        )
        return CalibrationSnapshot(
            key=key, interval_seconds=float(row["interval_seconds"]),
            clean_observations=int(row["clean_observations"]),
            rate_limit_count=int(row["rate_limit_count"]),
            network_error_count=int(row["network_error_count"]),
            validated_at=float(row["validated_at"]),
            invalidated_at=(None if row["invalidated_at"] is None else float(row["invalidated_at"])),
        )

    async def load(self, key: CalibrationKey) -> Optional[CalibrationSnapshot]:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT * FROM credential_cadence_calibrations WHERE credential_key=$1 AND marketplace_id=$2 AND region=$3 AND direct_route=$4 AND batch_size=$5",
                key.credential_key, key.marketplace_id, key.region, key.direct_route, key.batch_size,
            )
        return None if row is None else self._snapshot(row)

    async def record_validation(
        self, key: CalibrationKey, interval_seconds: float, clean_observations: int,
        rate_limit_count: int = 0, network_error_count: int = 0, *,
        validated_at: Optional[float] = None,
    ) -> CalibrationSnapshot:
        validated = self._clock() if validated_at is None else float(validated_at)
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """INSERT INTO credential_cadence_calibrations
                (credential_key, marketplace_id, region, direct_route, batch_size, interval_seconds,
                 clean_observations, rate_limit_count, network_error_count, validated_at, invalidated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULL)
                ON CONFLICT (credential_key, marketplace_id, region, direct_route, batch_size) DO UPDATE SET
                  interval_seconds=EXCLUDED.interval_seconds, clean_observations=EXCLUDED.clean_observations,
                  rate_limit_count=EXCLUDED.rate_limit_count, network_error_count=EXCLUDED.network_error_count,
                  validated_at=EXCLUDED.validated_at, invalidated_at=NULL
                RETURNING *""",
                key.credential_key, key.marketplace_id, key.region, key.direct_route, key.batch_size,
                float(interval_seconds), int(clean_observations), int(rate_limit_count),
                int(network_error_count), validated,
            )
        return self._snapshot(row)

    async def invalidate_credential(
        self, credential_key: str, marketplace_id: str, *, now: Optional[float] = None
    ) -> int:
        invalidated = self._clock() if now is None else float(now)
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                "UPDATE credential_cadence_calibrations SET invalidated_at=$3 WHERE credential_key=$1 AND marketplace_id=$2 AND invalidated_at IS NULL",
                credential_key, marketplace_id, invalidated,
            )
        return int(str(result).rsplit(" ", 1)[-1])

    async def activate_for_leader(
        self,
        key: CalibrationKey,
        requested_interval_seconds: float,
        owner_id: str,
        *,
        validity_seconds: float = CALIBRATION_VALIDITY_SECONDS,
    ) -> tuple[float, Optional[float]]:
        """Atomically bind a fresh calibration to the current poll leader."""
        requested = float(requested_interval_seconds)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                governor = await connection.fetchrow(
                    """
                    SELECT recovery_floor_seconds, last_rate_limited_at
                    FROM credential_governor
                    WHERE credential_key=$1
                      AND lease_owner=$2
                      AND lease_expires_at
                          > EXTRACT(EPOCH FROM clock_timestamp())
                    FOR UPDATE
                    """,
                    key.credential_key,
                    owner_id,
                )
                if governor is None:
                    raise CredentialLeaseFenceLost(
                        "TVSS credential leader lease is no longer held"
                    )
                calibration = await connection.fetchrow(
                    """
                    SELECT *
                    FROM credential_cadence_calibrations
                    WHERE credential_key=$1
                      AND marketplace_id=$2
                      AND region=$3
                      AND direct_route=$4
                      AND batch_size=$5
                    FOR UPDATE
                    """,
                    key.credential_key,
                    key.marketplace_id,
                    key.region,
                    key.direct_route,
                    key.batch_size,
                )
                now = self._clock()
                snapshot = (
                    None
                    if calibration is None
                    else self._snapshot(calibration)
                )
                valid = (
                    requested < PRODUCTION_MIN_INTERVAL_SECONDS
                    and snapshot is not None
                    and snapshot.is_valid(now, validity_seconds)
                    and requested >= snapshot.interval_seconds
                    and snapshot.validated_at
                        > float(governor["last_rate_limited_at"])
                )
                effective = (
                    requested
                    if valid or requested >= PRODUCTION_MIN_INTERVAL_SECONDS
                    else PRODUCTION_MIN_INTERVAL_SECONDS
                )
                await connection.execute(
                    """
                    UPDATE credential_governor
                    SET recovery_floor_seconds=$2,
                        interval_seconds=CASE
                            WHEN $3::boolean
                            THEN $2
                            ELSE GREATEST(interval_seconds, $2)
                        END,
                        success_streak=CASE
                            WHEN $3::boolean THEN 0
                            ELSE success_streak
                        END
                    WHERE credential_key=$1
                    """,
                    key.credential_key,
                    effective,
                    valid,
                )
        age = (
            max(0.0, now - snapshot.validated_at)
            if snapshot is not None
            else None
        )
        return effective, age


class PostgresCredentialGovernor:
    """Postgres implementation using an injected asyncpg-compatible pool.

    State changes take a row lock, making permit reservation and leader lease
    ownership atomic across replicas.
    """

    def __init__(
        self,
        pool,
        clock: Callable[[], float] = time.time,
        base_interval: float = DEFAULT_INTERVAL_SECONDS,
        cooldown_seconds: float = BASE_429_COOLDOWN_SECONDS,
        max_cooldown_seconds: float = MAX_429_COOLDOWN_SECONDS,
        max_interval_seconds: float = MAX_INTERVAL_SECONDS,
        recovery_success_count: int = RECOVERY_SUCCESS_COUNT,
        recovery_decrement_seconds: float = RECOVERY_DECREMENT_SECONDS,
    ):
        self._pool = pool
        self._clock = clock
        self._base_interval = float(base_interval)
        self._cooldown_seconds = float(cooldown_seconds)
        self._max_cooldown_seconds = float(max_cooldown_seconds)
        self._max_interval_seconds = float(max_interval_seconds)
        self._recovery_success_count = int(recovery_success_count)
        self._recovery_decrement_seconds = float(recovery_decrement_seconds)

    async def initialize(self) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(SCHEMA_SQL)

    @asynccontextmanager
    async def _locked_state(self, credential_key: str) -> AsyncIterator[tuple[object, _State]]:
        async with self._pool.acquire() as connection:
            transaction = connection.transaction()
            async with transaction:
                row = await connection.fetchrow(
                    "SELECT * FROM credential_governor WHERE credential_key = $1 FOR UPDATE", credential_key
                )
                if row is None:
                    # A missing row has no row lock to serialize competing
                    # replicas.  Let the unique key elect the creator, then
                    # lock whichever row won before reading or mutating it.
                    await connection.execute(
                        "INSERT INTO credential_governor (credential_key, interval_seconds, next_request_at, blocked_until, generation, consecutive_429, success_streak, half_open_pending, lease_expires_at, recovery_floor_seconds, last_rate_limited_at) VALUES ($1, $2, 0, 0, 0, 0, 0, FALSE, 0, $2, 0) ON CONFLICT (credential_key) DO NOTHING",
                        credential_key, self._base_interval,
                    )
                    row = await connection.fetchrow(
                        "SELECT * FROM credential_governor WHERE credential_key = $1 FOR UPDATE", credential_key
                    )
                state = _State(
                    interval_seconds=float(row["interval_seconds"]), next_request_at=float(row["next_request_at"]),
                    blocked_until=float(row["blocked_until"]), generation=int(row["generation"]),
                    consecutive_429=int(row["consecutive_429"]), success_streak=int(row["success_streak"]),
                    half_open_pending=bool(row["half_open_pending"]), lease_owner=row["lease_owner"],
                    lease_expires_at=float(row["lease_expires_at"]),
                    recovery_floor_seconds=float(row["recovery_floor_seconds"]),
                    last_rate_limited_at=float(row["last_rate_limited_at"]),
                )
                yield connection, state
                await connection.execute(
                    "UPDATE credential_governor SET interval_seconds=$2, next_request_at=$3, blocked_until=$4, generation=$5, consecutive_429=$6, success_streak=$7, half_open_pending=$8, lease_owner=$9, lease_expires_at=$10, recovery_floor_seconds=$11, last_rate_limited_at=$12 WHERE credential_key=$1",
                    credential_key, state.interval_seconds, state.next_request_at, state.blocked_until,
                    state.generation, state.consecutive_429, state.success_streak, state.half_open_pending,
                    state.lease_owner, state.lease_expires_at,
                    state.recovery_floor_seconds,
                    state.last_rate_limited_at,
                )

    async def acquire_leader(self, credential_key: str, owner_id: str, ttl_seconds: float) -> bool:
        async with self._locked_state(credential_key) as (_, state):
            now = self._clock()
            if state.lease_owner not in (None, owner_id) and state.lease_expires_at > now:
                return False
            state.lease_owner, state.lease_expires_at = owner_id, now + float(ttl_seconds)
            return True

    async def renew_leader(self, credential_key: str, owner_id: str, ttl_seconds: float) -> bool:
        async with self._locked_state(credential_key) as (_, state):
            now = self._clock()
            if state.lease_owner != owner_id or state.lease_expires_at <= now:
                return False
            state.lease_expires_at = now + float(ttl_seconds)
            return True

    async def release_leader(self, credential_key: str, owner_id: str) -> bool:
        async with self._locked_state(credential_key) as (_, state):
            if state.lease_owner != owner_id:
                return False
            state.lease_owner, state.lease_expires_at = None, 0.0
            return True

    @staticmethod
    def _require_leader(state: _State, owner_id: str, now: float) -> None:
        if state.lease_owner != owner_id or state.lease_expires_at <= now:
            raise CredentialLeaseFenceLost(
                "TVSS credential leader lease is no longer held"
            )

    async def ensure_leader(self, credential_key: str, owner_id: str) -> None:
        async with self._locked_state(credential_key) as (_, state):
            self._require_leader(state, owner_id, self._clock())

    async def acquire_permit(
        self,
        credential_key: str,
        request_class: RequestClass,
        owner_id: Optional[str] = None,
    ) -> Permit:
        async with self._locked_state(credential_key) as (_, state):
            now = self._clock()
            if owner_id is not None:
                self._require_leader(state, owner_id, now)
            return _reserve(credential_key, state, request_class, now, owner_id)

    async def acquire_borrowed_confirmation_permit(
        self,
        credential_key: str,
        owner_id: Optional[str] = None,
    ) -> Permit:
        async with self._locked_state(credential_key) as (_, state):
            now = self._clock()
            if owner_id is not None:
                self._require_leader(state, owner_id, now)
            return _borrow_confirmation(
                credential_key,
                state,
                now,
                owner_id,
            )

    async def record_result(
        self,
        permit: Permit,
        status: Optional[int],
        retry_after_seconds: Optional[float] = None,
        owner_id: Optional[str] = None,
    ) -> GovernorSnapshot:
        async with self._locked_state(permit.credential_key) as (_, state):
            fence_owner = owner_id or permit.lease_owner
            if fence_owner is not None:
                self._require_leader(state, fence_owner, self._clock())
            return _record(
                permit.credential_key,
                state,
                permit,
                status,
                retry_after_seconds,
                self._clock(),
                self._cooldown_seconds,
                self._max_cooldown_seconds,
                self._max_interval_seconds,
                self._recovery_success_count,
                self._recovery_decrement_seconds,
            )

    async def snapshot(self, credential_key: str) -> GovernorSnapshot:
        async with self._locked_state(credential_key) as (_, state):
            return _snapshot(credential_key, state)

    async def raise_interval_floor(
        self, credential_key: str, interval_seconds: float
    ) -> GovernorSnapshot:
        return await self.set_interval_floor(
            credential_key, interval_seconds, allow_lower=False
        )

    async def set_interval_floor(
        self,
        credential_key: str,
        interval_seconds: float,
        *,
        allow_lower: bool = False,
    ) -> GovernorSnapshot:
        floor = float(interval_seconds)
        self._base_interval = (
            floor if allow_lower else max(self._base_interval, floor)
        )
        async with self._locked_state(credential_key) as (_, state):
            if allow_lower:
                state.recovery_floor_seconds = floor
                state.interval_seconds = floor
                state.success_streak = 0
            else:
                state.recovery_floor_seconds = max(
                    state.recovery_floor_seconds, floor
                )
                state.interval_seconds = max(state.interval_seconds, floor)
            return _snapshot(credential_key, state)


def new_owner_id() -> str:
    """Create an opaque process owner identifier for a leader lease."""
    return uuid.uuid4().hex
