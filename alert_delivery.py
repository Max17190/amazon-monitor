"""Durable, at-least-once alert delivery core.

Persistence is intentionally injected. Production uses the shared Postgres
outbox while deterministic tests use an in-memory protocol implementation.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import Any, Awaitable, Callable, Mapping, Protocol

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
    async def claim_due(self, *, limit: int, now: float, lease_seconds: float) -> list[AlertDelivery]: ...
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
                 circuit_open_seconds: float = DEFAULT_CIRCUIT_OPEN_SECONDS):
        self.repository, self.sender = repository, sender
        self.metrics = metrics or DeliveryMetrics()
        self.health = health or DeliveryHealth(repository_ready=True)
        if concurrency < 1 or per_target_concurrency < 1:
            raise ValueError("delivery concurrency limits must be at least one")
        self.claim_limit = concurrency
        self.concurrency = asyncio.Semaphore(concurrency)
        self.per_target_concurrency = per_target_concurrency
        self.lease_seconds, self.max_attempts, self.max_age_seconds = lease_seconds, max_attempts, max_age_seconds
        self.clock, self.rng = clock, rng
        self.max_backoff_seconds = float(max_backoff_seconds)
        self.circuit_failure_threshold = int(circuit_failure_threshold)
        self.circuit_open_seconds = float(circuit_open_seconds)
        self._target_limits: dict[str, asyncio.Semaphore] = {}
        self._breakers: dict[str, CircuitBreaker] = {}
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        self.health.stopping = True
        self._stopping.set()

    async def run_once(self, *, limit: int | None = None) -> int:
        if self._stopping.is_set():
            return 0
        now = self.clock()
        self.health.repository_ready = True
        claimed = await self.repository.claim_due(limit=limit or self.claim_limit, now=now,
                                                  lease_seconds=self.lease_seconds)
        self.metrics.set_gauge("alert_outbox_claimed", len(claimed))
        await asyncio.gather(*(self._deliver(row) for row in claimed))
        return len(claimed)

    async def run(self, *, poll_interval_seconds: float = 0.25) -> None:
        self.health.worker_running = True
        try:
            while not self._stopping.is_set():
                try:
                    await self.run_once()
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
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=poll_interval_seconds)
                except asyncio.TimeoutError:
                    pass
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
