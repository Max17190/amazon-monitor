"""Durable, at-least-once alert delivery core.

Persistence is intentionally injected. Production uses the shared Postgres
outbox while deterministic tests use an in-memory protocol implementation.
"""

from __future__ import annotations

import asyncio
import json
import inspect
import logging
import random
import time
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import Any, Awaitable, Callable, Iterable, Mapping, Protocol

import aiohttp

from observability import DeliveryHealth, DeliveryMetrics


DEFAULT_CONNECT_TIMEOUT_SECONDS = 1.0
DEFAULT_READ_TIMEOUT_SECONDS = 2.0
DEFAULT_ATTEMPT_TIMEOUT_SECONDS = 3.0
DEFAULT_MAX_ATTEMPTS = 10
DEFAULT_MAX_AGE_SECONDS = 15 * 60
DEFAULT_MAX_BACKOFF_SECONDS = 60.0
DEFAULT_CIRCUIT_FAILURE_THRESHOLD = 5
DEFAULT_CIRCUIT_OPEN_SECONDS = 60.0


class DeliveryStatus(str, Enum):
    PENDING = "pending"
    LEASED = "leased"
    SUCCEEDED = "succeeded"
    DEAD_LETTERED = "dead_lettered"


class ErrorClass(str, Enum):
    NETWORK = "network"
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"
    UPSTREAM = "upstream"
    TERMINAL = "terminal"


@dataclass(frozen=True)
class RetryDecision:
    retryable: bool
    error_class: ErrorClass


@dataclass(frozen=True)
class DeliveryTarget:
    target_id: str
    url: str
    kind: str = "generic"
    required: bool = True


@dataclass
class AlertDelivery:
    """A leased outbox row. `delivery_id` is stable across attempts."""

    delivery_id: str
    alert_id: str
    target: DeliveryTarget
    payload: Mapping[str, Any]
    created_at: float
    attempts: int = 0
    next_attempt_at: float = 0.0
    leased_until: float | None = None
    status: DeliveryStatus = DeliveryStatus.PENDING
    trace_context: str | None = None
    claimed_at: float | None = None

    @property
    def idempotency_key(self) -> str:
        return f"{self.alert_id}:{self.delivery_id}"


@dataclass(frozen=True)
class DeliveryAttempt:
    success: bool
    status_code: int | None = None
    retry_after_seconds: float | None = None
    exception: BaseException | None = None


class OutboxRepository(Protocol):
    async def claim_due(
        self,
        *,
        limit: int,
        now: float,
        lease_seconds: float,
        preferred_delivery_ids: Iterable[str] | None = None,
    ) -> list[AlertDelivery]: ...
    async def succeed(
        self, delivery_id: str, *, delivered_at: float,
        status_code: int | None, duration_seconds: float,
    ) -> None: ...
    async def retry(
        self, delivery_id: str, *, attempts: int, next_attempt_at: float, error_class: ErrorClass,
        status_code: int | None, detail: str | None, duration_seconds: float,
        retry_after_seconds: float | None,
    ) -> None: ...
    async def dead_letter(
        self, delivery_id: str, *, attempts: int, error_class: ErrorClass, status_code: int | None,
        detail: str | None, duration_seconds: float,
    ) -> None: ...


class DeliverySender(Protocol):
    async def send(self, delivery: AlertDelivery) -> DeliveryAttempt: ...


class OutboxWakeup:
    """Coalesce local commits and cross-replica notifications into one wakeup.

    The commit path calls :meth:`wake` only after its transaction commits.  A
    worker consumes the accumulated IDs atomically with clearing the event, so
    commits arriving during a delivery drain cause another immediate pass.
    """

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._preferred_delivery_ids: set[str] = set()
        self._woken_at: dict[str, float] = {}

    def wake(self, delivery_ids: Iterable[str] = ()) -> None:
        now = time.time()
        for value in delivery_ids:
            delivery_id = str(value)
            self._preferred_delivery_ids.add(delivery_id)
            self._woken_at.setdefault(delivery_id, now)
        self._event.set()

    notify = wake

    def take_preferred_delivery_ids(self) -> tuple[str, ...]:
        """Return and clear coalesced preferred IDs without blocking."""
        preferred = tuple(sorted(self._preferred_delivery_ids))
        self._preferred_delivery_ids.clear()
        self._event.clear()
        return preferred

    async def wait(self) -> tuple[str, ...]:
        """Wait until a local or database notification is received."""
        await self._event.wait()
        return self.take_preferred_delivery_ids()

    def pop_wake_time(self, delivery_id: str) -> float | None:
        return self._woken_at.pop(str(delivery_id), None)


class PostgresOutboxNotificationAdapter:
    """Small adapter for a Postgres LISTEN callback supplied by runtime code.

    The durable runtime owns the database connection and calls
    :meth:`on_notification` from its listener. Payloads may be a delivery ID,
    a JSON array of IDs, or an object containing ``delivery_ids``. Invalid
    payloads still wake the worker so the one-second fallback is not the only
    recovery mechanism.
    """

    def __init__(self, wakeup: OutboxWakeup) -> None:
        self.wakeup = wakeup

    def on_notification(self, payload: str | bytes | None) -> None:
        delivery_ids: Iterable[str] = ()
        if payload:
            try:
                decoded = json.loads(payload)
                if isinstance(decoded, str):
                    delivery_ids = (decoded,)
                elif isinstance(decoded, list):
                    delivery_ids = (value for value in decoded if isinstance(value, str))
                elif isinstance(decoded, dict):
                    values = decoded.get("delivery_ids", ())
                    if isinstance(values, list):
                        delivery_ids = (value for value in values if isinstance(value, str))
            except (TypeError, ValueError):
                if isinstance(payload, bytes):
                    payload = payload.decode("utf-8", errors="ignore")
                delivery_ids = (payload,) if isinstance(payload, str) and payload else ()
        self.wakeup.wake(delivery_ids)


def _delivery_span(delivery: AlertDelivery):
    if not delivery.trace_context:
        return nullcontext(None)
    try:
        from opentelemetry import propagate, trace

        parent = propagate.extract(
            {"traceparent": delivery.trace_context}
        )
        tracer = trace.get_tracer("amazon-monitor")
        return tracer.start_as_current_span(
            "alert.deliver",
            context=parent,
            attributes={
                "alert.id": delivery.alert_id,
                "alert.delivery_id": delivery.delivery_id,
                "alert.target_id": delivery.target.target_id,
            },
        )
    except Exception:
        return nullcontext(None)


def _span_traceparent(span):
    if span is None:
        return None
    context = span.get_span_context()
    if not context.is_valid:
        return None
    return (
        f"00-{context.trace_id:032x}-{context.span_id:016x}-"
        f"{int(context.trace_flags) & 0xFF:02x}"
    )


def classify_response(status_code: int | None, exception: BaseException | None = None) -> RetryDecision:
    """Map transport/HTTP outcomes to the stable retry taxonomy."""
    if exception is not None:
        if isinstance(exception, asyncio.TimeoutError):
            return RetryDecision(True, ErrorClass.TIMEOUT)
        if isinstance(exception, aiohttp.ClientError):
            return RetryDecision(True, ErrorClass.NETWORK)
        return RetryDecision(False, ErrorClass.TERMINAL)
    if status_code in (408, 425, 429):
        return RetryDecision(True, ErrorClass.RATE_LIMITED if status_code == 429 else ErrorClass.UPSTREAM)
    if status_code is not None and 500 <= status_code <= 599:
        return RetryDecision(True, ErrorClass.UPSTREAM)
    return RetryDecision(False, ErrorClass.TERMINAL)


def parse_retry_after(raw: str | None, *, now: datetime | None = None) -> float | None:
    """Parse Retry-After delta seconds or HTTP date, returning a nonnegative delay."""
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        pass
    try:
        date = parsedate_to_datetime(raw)
        if date.tzinfo is None:
            date = date.replace(tzinfo=UTC)
        current = now or datetime.now(UTC)
        return max(0.0, (date - current).total_seconds())
    except (TypeError, ValueError, IndexError):
        return None


def decorrelated_jitter(previous_delay: float, *, base: float = 0.5, cap: float = DEFAULT_MAX_BACKOFF_SECONDS,
                        rng: Callable[[float, float], float] = random.uniform) -> float:
    """AWS-style decorrelated jitter, bounded to avoid a synchronized retry herd."""
    return min(cap, rng(base, max(base, previous_delay * 3)))


def _detected_timestamp(delivery: AlertDelivery) -> float:
    value = delivery.payload.get("detected_at")
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    return delivery.created_at


@dataclass
class CircuitBreaker:
    failure_threshold: int = DEFAULT_CIRCUIT_FAILURE_THRESHOLD
    open_seconds: float = DEFAULT_CIRCUIT_OPEN_SECONDS
    failures: int = 0
    open_until: float = 0.0

    def allow(self, now: float) -> bool:
        return now >= self.open_until

    def record_success(self) -> None:
        self.failures = 0
        self.open_until = 0.0

    def record_failure(self, now: float) -> bool:
        self.failures += 1
        if self.failures >= self.failure_threshold:
            self.open_until = now + self.open_seconds
            return True
        return False


class GenericWebhookSender:
    """Generic JSON sender with explicit phase timeouts and idempotency headers."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        connect_timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
        read_timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
        attempt_timeout_seconds: float = DEFAULT_ATTEMPT_TIMEOUT_SECONDS,
    ):
        self.session = session
        self.timeout = aiohttp.ClientTimeout(
            total=float(attempt_timeout_seconds),
            connect=float(connect_timeout_seconds),
            sock_read=float(read_timeout_seconds),
        )

    async def send(self, delivery: AlertDelivery) -> DeliveryAttempt:
        headers = {
            "Idempotency-Key": delivery.idempotency_key,
            "X-Alert-Id": delivery.alert_id,
            "X-Alert-Delivery-Id": delivery.delivery_id,
        }
        if delivery.trace_context:
            headers["traceparent"] = delivery.trace_context
        try:
            async with self.session.post(delivery.target.url, json=dict(delivery.payload), headers=headers,
                                         timeout=self.timeout) as response:
                if 200 <= response.status < 300:
                    return DeliveryAttempt(True, status_code=response.status)
                return DeliveryAttempt(
                    False,
                    status_code=response.status,
                    retry_after_seconds=parse_retry_after(response.headers.get("Retry-After")),
                )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            return DeliveryAttempt(False, exception=exc)


class AlertDeliveryWorker:
    """Claims durable rows and delivers them with bounded global/per-target work."""

    def __init__(self, repository: OutboxRepository, sender: DeliverySender, *, metrics: DeliveryMetrics | None = None,
                 health: DeliveryHealth | None = None, concurrency: int = 32, per_target_concurrency: int = 2,
                 lease_seconds: float = 30.0, max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                 max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS, clock: Callable[[], float] = time.time,
                 rng: Callable[[float, float], float] = random.uniform,
                 max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
                 circuit_failure_threshold: int = DEFAULT_CIRCUIT_FAILURE_THRESHOLD,
                 circuit_open_seconds: float = DEFAULT_CIRCUIT_OPEN_SECONDS,
                 wakeup: OutboxWakeup | None = None,
                 fallback_poll_seconds: float = 1.0):
        self.repository, self.sender = repository, sender
        self.metrics = metrics or DeliveryMetrics()
        self.health = health or DeliveryHealth(repository_ready=True)
        if concurrency < 1 or per_target_concurrency < 1:
            raise ValueError("delivery concurrency limits must be at least one")
        if fallback_poll_seconds <= 0:
            raise ValueError("fallback poll interval must be positive")
        self.claim_limit = concurrency
        self.concurrency = asyncio.Semaphore(concurrency)
        self.per_target_concurrency = per_target_concurrency
        self.lease_seconds, self.max_attempts, self.max_age_seconds = lease_seconds, max_attempts, max_age_seconds
        self.clock, self.rng = clock, rng
        self.max_backoff_seconds = float(max_backoff_seconds)
        self.circuit_failure_threshold = int(circuit_failure_threshold)
        self.circuit_open_seconds = float(circuit_open_seconds)
        self.wakeup = wakeup or OutboxWakeup()
        self.fallback_poll_seconds = float(fallback_poll_seconds)
        self._target_limits: dict[str, asyncio.Semaphore] = {}
        self._breakers: dict[str, CircuitBreaker] = {}
        self._stopping = asyncio.Event()
        self._claim_supports_preferred_ids: bool | None = None

    def stop(self) -> None:
        self.health.stopping = True
        self._stopping.set()
        self.wakeup.wake()

    def wake(self, delivery_ids: Iterable[str] = ()) -> None:
        """Schedule an immediate, preferred delivery drain after outbox commit."""
        self.wakeup.wake(delivery_ids)

    def _supports_preferred_claims(self) -> bool:
        if self._claim_supports_preferred_ids is not None:
            return self._claim_supports_preferred_ids
        try:
            parameters = inspect.signature(self.repository.claim_due).parameters.values()
            self._claim_supports_preferred_ids = any(
                parameter.name == "preferred_delivery_ids"
                or parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in parameters
            )
        except (TypeError, ValueError):
            # Conservative compatibility path for opaque repository adapters.
            self._claim_supports_preferred_ids = False
        return self._claim_supports_preferred_ids

    async def _claim_due(
        self,
        *,
        limit: int,
        now: float,
        preferred_delivery_ids: Iterable[str] = (),
    ) -> list[AlertDelivery]:
        preferred = tuple(preferred_delivery_ids)
        kwargs: dict[str, Any] = {
            "limit": limit,
            "now": now,
            "lease_seconds": self.lease_seconds,
        }
        if preferred and self._supports_preferred_claims():
            kwargs["preferred_delivery_ids"] = preferred
        return await self.repository.claim_due(**kwargs)

    async def run_once(
        self,
        *,
        limit: int | None = None,
        preferred_delivery_ids: Iterable[str] = (),
    ) -> int:
        if self._stopping.is_set():
            return 0
        now = self.clock()
        self.health.repository_ready = True
        claimed = await self._claim_due(
            limit=limit or self.claim_limit,
            now=now,
            preferred_delivery_ids=preferred_delivery_ids,
        )
        self.metrics.set_gauge("alert_outbox_claimed", len(claimed))
        await asyncio.gather(*(self._deliver(row) for row in claimed))
        return len(claimed)

    async def _drain(self, *, preferred_delivery_ids: Iterable[str] = ()) -> int:
        """Drain all due work before waiting, without reusing priority IDs."""
        total = 0
        preferred = tuple(preferred_delivery_ids)
        while not self._stopping.is_set():
            claimed = await self.run_once(preferred_delivery_ids=preferred)
            total += claimed
            preferred = ()
            if not claimed:
                return total
        return total

    async def run(self, *, poll_interval_seconds: float | None = None) -> None:
        """Run on commit notifications with a bounded missed-notification scan.

        ``poll_interval_seconds`` remains accepted for older callers, but its
        default is now a one-second fallback rather than the old 250 ms idle
        delay.
        """
        fallback_seconds = (
            self.fallback_poll_seconds
            if poll_interval_seconds is None
            else float(poll_interval_seconds)
        )
        if fallback_seconds <= 0:
            raise ValueError("poll interval must be positive")
        self.health.worker_running = True
        preferred = self.wakeup.take_preferred_delivery_ids()
        try:
            while not self._stopping.is_set():
                try:
                    await self._drain(preferred_delivery_ids=preferred)
                    preferred = ()
                    self.health.repository_ready = True
                    self.health.last_error = None
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.health.repository_ready = False
                    self.health.last_error = type(exc).__name__
                    self.metrics.increment(
                        "alert_worker_error_total",
                        labels={"class": type(exc).__name__},
                    )
                    logging.exception("Alert delivery worker iteration failed")
                if self._stopping.is_set():
                    break
                try:
                    preferred = await asyncio.wait_for(
                        self.wakeup.wait(), timeout=fallback_seconds
                    )
                except asyncio.TimeoutError:
                    # A fallback scan recovers rows after process crashes or a
                    # missed LISTEN/NOTIFY event.
                    preferred = ()
        finally:
            self.health.worker_running = False

    async def _deliver(self, delivery: AlertDelivery) -> None:
        now = self.clock()
        age = max(0.0, now - delivery.created_at)
        labels = {"target": delivery.target.target_id, "kind": delivery.target.kind}
        self.metrics.observe("alert_queue_age_seconds", age, labels)
        breaker = self._breakers.setdefault(
            delivery.target.target_id,
            CircuitBreaker(
                failure_threshold=self.circuit_failure_threshold,
                open_seconds=self.circuit_open_seconds,
            ),
        )
        if not breaker.allow(now):
            self.metrics.set_gauge(
                "alert_target_circuit_open",
                1,
                labels={"target": delivery.target.target_id},
            )
            await self._retry(
                delivery,
                ErrorClass.UPSTREAM,
                None,
                "circuit_open",
                breaker.open_until,
                duration_seconds=0.0,
                retry_after_seconds=None,
            )
            return
        target_limit = self._target_limits.setdefault(delivery.target.target_id, asyncio.Semaphore(self.per_target_concurrency))
        async with self.concurrency, target_limit:
            started = time.monotonic()
            if delivery.claimed_at is not None:
                self.metrics.observe(
                    "alert_claim_to_attempt_seconds",
                    max(0.0, self.clock() - delivery.claimed_at),
                    labels=labels,
                )
            original_trace_context = delivery.trace_context
            try:
                with _delivery_span(delivery) as span:
                    delivery.trace_context = (
                        _span_traceparent(span) or original_trace_context
                    )
                    try:
                        attempt = await self.sender.send(delivery)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        attempt = DeliveryAttempt(False, exception=exc)
            finally:
                delivery.trace_context = original_trace_context
            duration_seconds = time.monotonic() - started
            self.metrics.observe("alert_delivery_attempt_seconds", duration_seconds, labels)
        if attempt.success:
            breaker.record_success()
            self.metrics.set_gauge(
                "alert_target_circuit_open",
                0,
                labels={"target": delivery.target.target_id},
            )
            await self.repository.succeed(
                delivery.delivery_id,
                delivered_at=self.clock(),
                status_code=attempt.status_code,
                duration_seconds=duration_seconds,
            )
            self.metrics.increment("alert_delivery_total", labels={**labels, "outcome": "success"})
            self.metrics.observe(
                "alert_detect_to_target_success_seconds",
                max(0.0, self.clock() - delivery.created_at),
                labels=labels,
            )
            self.metrics.observe(
                "alert_detect_to_accept_seconds",
                max(0.0, self.clock() - _detected_timestamp(delivery)),
                labels=labels,
            )
            logging.info(
                "alert_delivery %s",
                json.dumps(
                    {
                        "alert_id": delivery.alert_id,
                        "delivery_id": delivery.delivery_id,
                        "target_id": delivery.target.target_id,
                        "queue_age_seconds": round(age, 6),
                        "outcome": "success",
                        "status_code": attempt.status_code,
                    },
                    separators=(",", ":"),
                ),
            )
            return
        decision = classify_response(attempt.status_code, attempt.exception)
        if decision.retryable:
            opened = breaker.record_failure(self.clock())
            if opened:
                self.metrics.set_gauge(
                    "alert_target_circuit_open",
                    1,
                    labels={"target": delivery.target.target_id},
                )
        detail = (
            type(attempt.exception).__name__
            if attempt.exception is not None
            else None
        )
        if not decision.retryable or delivery.attempts + 1 >= self.max_attempts or age >= self.max_age_seconds:
            await self.repository.dead_letter(delivery.delivery_id, attempts=delivery.attempts + 1,
                                              error_class=decision.error_class, status_code=attempt.status_code,
                                              detail=detail, duration_seconds=duration_seconds)
            self.metrics.increment("alert_delivery_total", labels={**labels, "outcome": "dead_letter", "error_class": decision.error_class.value})
            logging.warning(
                "alert_delivery %s",
                json.dumps(
                    {
                        "alert_id": delivery.alert_id,
                        "delivery_id": delivery.delivery_id,
                        "target_id": delivery.target.target_id,
                        "queue_age_seconds": round(age, 6),
                        "outcome": "dead_letter",
                        "error_class": decision.error_class.value,
                        "status_code": attempt.status_code,
                    },
                    separators=(",", ":"),
                ),
            )
            return
        previous = max(0.5, delivery.next_attempt_at - now) if delivery.next_attempt_at else 0.5
        delay = max(
            attempt.retry_after_seconds or 0.0,
            decorrelated_jitter(
                previous,
                cap=self.max_backoff_seconds,
                rng=self.rng,
            ),
        )
        if now + delay >= delivery.created_at + self.max_age_seconds:
            await self.repository.dead_letter(
                delivery.delivery_id,
                attempts=delivery.attempts + 1,
                error_class=decision.error_class,
                status_code=attempt.status_code,
                detail=detail,
                duration_seconds=duration_seconds,
            )
            self.metrics.increment(
                "alert_delivery_total",
                labels={
                    **labels,
                    "outcome": "dead_letter",
                    "error_class": decision.error_class.value,
                },
            )
            return
        await self._retry(
            delivery,
            decision.error_class,
            attempt.status_code,
            detail,
            now + delay,
            duration_seconds=duration_seconds,
            retry_after_seconds=attempt.retry_after_seconds,
        )
        logging.info(
            "alert_delivery %s",
            json.dumps(
                {
                    "alert_id": delivery.alert_id,
                    "delivery_id": delivery.delivery_id,
                    "target_id": delivery.target.target_id,
                    "queue_age_seconds": round(age, 6),
                    "outcome": "retry",
                    "error_class": decision.error_class.value,
                    "status_code": attempt.status_code,
                    "retry_in_seconds": round(delay, 6),
                },
                separators=(",", ":"),
            ),
        )

    async def _retry(self, delivery: AlertDelivery, error_class: ErrorClass, status_code: int | None,
                     detail: str | None, next_attempt_at: float,
                     duration_seconds: float,
                     retry_after_seconds: float | None) -> None:
        await self.repository.retry(delivery.delivery_id, attempts=delivery.attempts + 1,
                                    next_attempt_at=next_attempt_at, error_class=error_class,
                                    status_code=status_code, detail=detail,
                                    duration_seconds=duration_seconds,
                                    retry_after_seconds=retry_after_seconds)
        self.metrics.increment("alert_delivery_total", labels={"target": delivery.target.target_id,
                               "kind": delivery.target.kind, "outcome": "retry", "error_class": error_class.value})
