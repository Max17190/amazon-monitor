from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from contextlib import asynccontextmanager, contextmanager, suppress
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from uuid import UUID, uuid4

import aiohttp
import discord
from aiohttp import web
from discord import Embed, Webhook

from alert_delivery import (
    AlertDelivery,
    AlertDeliveryWorker,
    DeliveryAttempt,
    DeliveryTarget,
    ErrorClass,
    GenericWebhookSender,
    OutboxWakeup,
    PostgresOutboxNotificationAdapter,
)
from amazon_tvss import (
    BatchObservation,
    ObservationStatus,
    TVSSClient,
    TVSSConfigError,
    TVSSRateLimitError,
)
from credential_governor import (
    CALIBRATION_VALIDITY_SECONDS,
    CalibrationKey,
    CredentialLeaseFenceLost,
    PostgresCadenceCalibrationStore,
    PostgresCredentialGovernor,
    new_owner_id,
    stable_credential_key,
)
from durable_store import (
    AlertWrite,
    BatchCommitResult,
    BatchStockDecision,
    PostgresStore,
    ScopeKey,
    StateConflict,
    TargetWrite,
    TransitionWrite,
    VerificationWrite,
    connect_and_migrate,
)
from observability import DeliveryHealth, DeliveryMetrics, HealthStatus
from stock_state import (
    DecisionKind,
    EvidenceSource,
    SellerPolicy,
    StockEvidence,
    StockState,
    StockStateRecord,
    advance_state,
)


PRODUCTION_MIN_INTERVAL_SECONDS = 5.0
DEFAULT_LEASE_TTL_SECONDS = 30.0
DEFAULT_LEASE_RENEW_SECONDS = 10.0
# A fast-alert confirmation is intentionally deferred for up to twelve
# successful five-second polls. Keep the default comfortably beyond that
# window so the durable job can still be claimed when its slot arrives.
DEFAULT_VERIFICATION_TTL_SECONDS = 90.0
DEFAULT_METRICS_PORT = 9090


class CredentialLeaseLost(RuntimeError):
    pass


@dataclass
class ConfirmationThrottle:
    every_polls: int = 12
    completed_polls: int = 0

    def __post_init__(self):
        self.every_polls = max(1, int(self.every_polls))

    def note_poll(self):
        self.completed_polls += 1

    def due(self, fast_alert):
        return not fast_alert or self.completed_polls >= self.every_polls

    def consumed(self):
        self.completed_polls = 0


def _utc_now():
    return datetime.now(timezone.utc)


def _jsonable(value):
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if hasattr(value, "value"):
        return value.value
    return value


def _json_mapping(value):
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value or {})


def _scope_name(monitor_id, marketplace_id, asin, policy_hash):
    return f"{monitor_id}:{marketplace_id}:{asin}:{policy_hash}"


def _parse_bool(value, default=False):
    if value is None:
        return bool(default)
    return str(value).strip().lower() not in ("0", "false", "no", "off")


def validate_durable_configuration(config, env=None):
    env = os.environ if env is None else env
    if not env.get("DATABASE_URL"):
        raise TVSSConfigError("DATABASE_URL is required for durable monitor mode")
    monitor_id = str(env.get("MONITOR_ID", "")).strip()
    if not monitor_id:
        raise TVSSConfigError("MONITOR_ID is required for durable monitor mode")
    if config.poll_interval_seconds <= 0:
        raise TVSSConfigError("POLL_INTERVAL_SECONDS must be positive")
    lease_ttl = float(
        env.get("TVSS_LEADER_LEASE_SECONDS", DEFAULT_LEASE_TTL_SECONDS)
    )
    lease_renew = float(
        env.get("TVSS_LEADER_RENEW_SECONDS", DEFAULT_LEASE_RENEW_SECONDS)
    )
    if lease_renew <= 0 or lease_ttl <= lease_renew:
        raise TVSSConfigError(
            "TVSS leader lease must be longer than its positive renewal cadence"
        )
    if int(env.get("STOCK_OOS_REARM_COUNT", "2")) < 2:
        raise TVSSConfigError(
            "STOCK_OOS_REARM_COUNT must be at least 2"
        )
    for name in (
        "ALERT_WORKER_CONCURRENCY",
        "ALERT_TARGET_CONCURRENCY",
        "ALERT_MAX_ATTEMPTS",
    ):
        if int(env.get(name, "1")) < 1:
            raise TVSSConfigError(f"{name} must be at least 1")
    for name, default in (
        ("ALERT_CONNECT_TIMEOUT_SECONDS", "1"),
        ("ALERT_READ_TIMEOUT_SECONDS", "2"),
        ("ALERT_ATTEMPT_TIMEOUT_SECONDS", "3"),
        ("ALERT_MAX_AGE_SECONDS", "900"),
    ):
        if float(env.get(name, default)) <= 0:
            raise TVSSConfigError(f"{name} must be positive")
    return monitor_id


class PostgresOutboxRepository:
    """Adapter from durable Postgres rows to the delivery worker protocol."""

    def __init__(
        self,
        store,
        targets,
        worker_id,
        global_concurrency=32,
        per_target_concurrency=2,
        circuit_failure_threshold=5,
        circuit_open_seconds=60.0,
        dead_letter_retention_days=30,
        metrics=None,
        wakeup=None,
    ):
        self.store = store
        self.targets = targets
        self.worker_id = worker_id
        self.global_concurrency = int(global_concurrency)
        self.per_target_concurrency = int(per_target_concurrency)
        self.circuit_failure_threshold = int(circuit_failure_threshold)
        self.circuit_open_seconds = float(circuit_open_seconds)
        self.dead_letter_retention_days = int(dead_letter_retention_days)
        self.metrics = metrics
        self.wakeup = wakeup

    async def claim_due(
        self,
        *,
        limit,
        now,
        lease_seconds,
        preferred_delivery_ids=None,
    ):
        wake_times = {}
        if self.wakeup is not None:
            wake_times = {
                str(value): self.wakeup.pop_wake_time(str(value))
                for value in (preferred_delivery_ids or ())
            }
        rows = await self.store.claim_deliveries(
            self.worker_id,
            limit=limit,
            lease_seconds=lease_seconds,
            global_limit=self.global_concurrency,
            per_target_limit=self.per_target_concurrency,
            preferred_delivery_ids=preferred_delivery_ids,
        )
        claimed = []
        for row in rows:
            configured = self.targets.get(row["target_id"])
            target = DeliveryTarget(
                target_id=row["target_id"],
                url=configured.url if configured else "",
                kind=row["target_kind"],
            )
            created_at = row["alert_created_at"]
            if isinstance(created_at, datetime):
                created_at = created_at.timestamp()
            claimed.append(
                AlertDelivery(
                    delivery_id=str(row["delivery_id"]),
                    alert_id=str(row["alert_id"]),
                    target=target,
                    payload=_json_mapping(row["payload"]),
                    created_at=float(created_at),
                    attempts=max(0, int(row["attempts"]) - 1),
                    next_attempt_at=(
                        time.time()
                        + float(row.get("previous_backoff_seconds") or 0.0)
                    ),
                    trace_context=row.get("trace_context"),
                    claimed_at=time.time(),
                )
            )
            if self.metrics is not None:
                woken_at = (
                    wake_times.get(str(row["delivery_id"]))
                )
                if woken_at is not None:
                    self.metrics.observe(
                        "alert_wake_to_claim_seconds",
                        max(0.0, time.time() - woken_at),
                        labels={"target": row["target_id"]},
                    )
        return claimed

    async def succeed(
        self,
        delivery_id,
        *,
        delivered_at,
        status_code,
        duration_seconds,
    ):
        await self.store.mark_delivery_succeeded(
            UUID(delivery_id),
            duration_ms=float(duration_seconds) * 1000.0,
            remote_request_id=str(status_code) if status_code else None,
        )

    async def retry(
        self,
        delivery_id,
        *,
        attempts,
        next_attempt_at,
        error_class,
        status_code,
        detail,
        duration_seconds,
        retry_after_seconds,
    ):
        delay = max(0.0, float(next_attempt_at) - time.time())
        await self.store.reschedule_delivery(
            UUID(delivery_id),
            delay_seconds=delay,
            duration_ms=float(duration_seconds) * 1000.0,
            error_class=error_class.value,
            http_status=status_code,
            retry_after_seconds=retry_after_seconds,
            previous_backoff_seconds=delay,
            circuit_failure_threshold=self.circuit_failure_threshold,
            circuit_open_seconds=self.circuit_open_seconds,
        )

    async def dead_letter(
        self,
        delivery_id,
        *,
        attempts,
        error_class,
        status_code,
        detail,
        duration_seconds,
    ):
        await self.store.dead_letter_delivery(
            UUID(delivery_id),
            reason=detail or error_class.value,
            duration_ms=float(duration_seconds) * 1000.0,
            error_class=error_class.value,
            http_status=status_code,
            retention_days=self.dead_letter_retention_days,
        )


class DurableDeliverySender:
    """Deliver generic JSON or Discord embeds from the same durable row."""

    def __init__(self, session, targets):
        self.session = session
        self.targets = targets
        self.attempt_timeout_seconds = float(
            os.getenv("ALERT_ATTEMPT_TIMEOUT_SECONDS", "3")
        )
        self.generic = GenericWebhookSender(
            session,
            connect_timeout_seconds=float(
                os.getenv("ALERT_CONNECT_TIMEOUT_SECONDS", "1")
            ),
            read_timeout_seconds=float(
                os.getenv("ALERT_READ_TIMEOUT_SECONDS", "2")
            ),
            attempt_timeout_seconds=float(
                self.attempt_timeout_seconds
            ),
        )
        self.discord_webhooks = {
            target_id: Webhook.from_url(target.url, session=self.session)
            for target_id, target in targets.items()
            if target.kind == "discord"
        }

    async def send(self, delivery):
        configured = self.targets.get(delivery.target.target_id)
        if configured is None or not configured.url:
            return DeliveryAttempt(
                False,
                exception=ValueError(
                    f"target {delivery.target.target_id!r} is no longer configured"
                ),
            )
        if configured.kind == "generic":
            return await self.generic.send(delivery)

        embed = self._discord_embed(delivery.payload)
        content = f"<@&{configured.role_id}>" if configured.role_id else None
        try:
            webhook = self.discord_webhooks[delivery.target.target_id]
            await asyncio.wait_for(
                webhook.send(content=content, embed=embed, wait=False),
                timeout=self.attempt_timeout_seconds,
            )
            return DeliveryAttempt(True, status_code=204)
        except discord.HTTPException as exc:
            return DeliveryAttempt(
                False,
                status_code=getattr(exc, "status", None),
                retry_after_seconds=float(
                    getattr(exc, "retry_after", 0.0) or 0.0
                ),
            )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            return DeliveryAttempt(False, exception=exc)

    @staticmethod
    def _discord_embed(payload):
        groups = payload.get("groups") or []
        group_text = ", ".join(groups)
        title = "Amazon Stock Monitor"
        if group_text:
            title = f"{title}: {group_text}"
        embed = Embed(title=title, color=discord.Color.purple())
        link = payload.get("link") or ""
        name = payload.get("title") or payload.get("asin") or "Restock"
        linked_name = f"[{name}]({link})" if link else name
        confirmation = "confirmed" if payload.get("confirmed") else "speculative"
        embed.add_field(
            name="Product Details",
            value=(
                f"**{linked_name}**\n"
                f"**SKU:** {payload.get('asin', 'N/A')}\n"
                f"**Price:** {payload.get('price') or 'MSRP'}\n"
                f"**Seller:** {payload.get('seller') or 'Unconfirmed'}\n"
                f"**Signal:** {payload.get('signal') or 'restock'} "
                f"({confirmation})"
            ),
            inline=False,
        )
        if payload.get("image"):
            embed.set_thumbnail(url=payload["image"])
        embed.set_footer(
            text=(
                "Amazon Stock Monitor | "
                f"alert={payload.get('alert_id', 'unknown')}"
            )
        )
        return embed


@dataclass
class RuntimeStatus:
    leader_owned: bool = False
    standby_healthy: bool = True
    stopping: bool = False
    last_poll_success_at: float = 0.0
    last_lease_renewal_at: float = 0.0


class OperationsServer:
    def __init__(self, store, metrics, delivery_health, runtime_status, port):
        self.store = store
        self.metrics = metrics
        self.delivery_health = delivery_health
        self.runtime_status = runtime_status
        self.port = int(port)
        self.runner = None

    async def start(self):
        app = web.Application()
        app.router.add_get("/health/live", self.live)
        app.router.add_get("/health/ready", self.ready)
        app.router.add_get("/metrics", self.prometheus)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "0.0.0.0", self.port)
        await site.start()
        logging.info("Operations server listening on port %s", self.port)

    async def close(self):
        if self.runner is not None:
            await self.runner.cleanup()

    async def live(self, request):
        status = 503 if self.runtime_status.stopping else 200
        return web.json_response(
            {"status": "stopping" if status == 503 else "live"},
            status=status,
        )

    async def ready(self, request):
        database_ready = await self.store.ping()
        delivery_ready = self.delivery_health.status is HealthStatus.READY
        role_ready = (
            self.runtime_status.leader_owned
            or self.runtime_status.standby_healthy
        )
        ready = database_ready and delivery_ready and role_ready
        return web.json_response(
            {
                "status": "ready" if ready else "not_ready",
                "database": database_ready,
                "delivery": self.delivery_health.snapshot(),
                "leader_owned": self.runtime_status.leader_owned,
                "standby_healthy": self.runtime_status.standby_healthy,
                "last_poll_success_at": self.runtime_status.last_poll_success_at,
            },
            status=200 if ready else 503,
        )

    async def prometheus(self, request):
        backlog = await self.store.delivery_backlog()
        self.metrics.set_gauge("alert_outbox_active", backlog.get("active") or 0)
        self.metrics.set_gauge(
            "alert_dead_letter_total", backlog.get("dead_lettered") or 0
        )
        self.metrics.set_gauge(
            "alert_outbox_oldest_age_seconds",
            backlog.get("oldest_age_seconds") or 0,
        )
        self.metrics.set_gauge(
            "alert_target_circuits_open",
            backlog.get("open_circuits") or 0,
        )
        if self.runtime_status.last_poll_success_at:
            self.metrics.set_gauge(
                "tvss_poll_freshness_seconds",
                max(
                    0.0,
                    time.time() - self.runtime_status.last_poll_success_at,
                ),
            )
        return web.Response(
            text=self._render_metrics(),
            content_type="text/plain",
            charset="utf-8",
        )

    def _render_metrics(self):
        lines = []
        for (name, labels), value in sorted(self.metrics.counters.items()):
            lines.append(f"{name}{self._format_labels(labels)} {value}")
        for (name, labels), value in sorted(self.metrics.gauges.items()):
            lines.append(f"{name}{self._format_labels(labels)} {value}")
        for (name, labels), values in sorted(self.metrics.histograms.items()):
            if not values:
                continue
            label_text = self._format_labels(labels)
            lines.append(f"{name}_count{label_text} {len(values)}")
            lines.append(f"{name}_sum{label_text} {sum(values)}")
        return "\n".join(lines) + "\n"

    @staticmethod
    def _format_labels(labels):
        if not labels:
            return ""
        body = ",".join(
            f'{key}="{value.replace(chr(34), chr(92) + chr(34))}"'
            for key, value in labels
        )
        return "{" + body + "}"


def configure_tracing():
    endpoint = str(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")).strip()
    if not endpoint:
        return None
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        provider = TracerProvider(
            resource=Resource.create({"service.name": "amazon-monitor"})
        )
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
        )
        trace.set_tracer_provider(provider)
        return trace.get_tracer("amazon-monitor")
    except Exception:
        logging.exception("Failed to configure OTLP tracing")
        return None


@contextmanager
def trace_span(tracer, name, attributes=None):
    if tracer is None:
        yield None
        return
    with tracer.start_as_current_span(name, attributes=attributes or {}) as span:
        yield span


def _traceparent(span):
    if span is None:
        return None
    context = span.get_span_context()
    if not context.is_valid:
        return None
    flags = int(context.trace_flags) & 0xFF
    return (
        f"00-{context.trace_id:032x}-{context.span_id:016x}-{flags:02x}"
    )


class DurableStockCoordinator:
    def __init__(
        self,
        store,
        config,
        monitor_id,
        marketplace_id,
        domain,
        asin_groups,
        asin_targets,
        metrics,
        tracer=None,
        outbox_wakeup=None,
    ):
        self.store = store
        self.config = config
        self.monitor_id = monitor_id
        self.marketplace_id = marketplace_id
        self.domain = domain
        self.asin_groups = asin_groups
        self.asin_targets = asin_targets
        self.metrics = metrics
        self.tracer = tracer
        self.outbox_wakeup = outbox_wakeup
        self.policy = SellerPolicy(
            require_amazon_seller=config.require_amazon_seller
        )

    def scope(self, asin):
        return ScopeKey(
            self.monitor_id,
            self.marketplace_id,
            asin,
            self.policy.fingerprint,
        )

    def scope_name(self, asin):
        return _scope_name(
            self.monitor_id,
            self.marketplace_id,
            asin,
            self.policy.fingerprint,
        )

    def confirmation_ttl_seconds(self):
        """Return a confirmation TTL that survives the fast-alert cadence.

        Fast alerts deliberately defer full-product requests. A configured
        short TTL must not expire a job before its throttled request slot is
        reached, while non-fast-alert monitors retain their configured TTL.
        """
        configured_ttl = float(
            os.getenv(
                "STOCK_CONFIRM_TTL_SECONDS",
                str(DEFAULT_VERIFICATION_TTL_SECONDS),
            )
        )
        if not self.config.fast_alert:
            return configured_ttl
        every_polls = max(
            1,
            int(os.getenv("FAST_ALERT_CONFIRM_EVERY_POLLS", "12")),
        )
        poll_interval = max(
            float(self.config.poll_interval_seconds),
            PRODUCTION_MIN_INTERVAL_SECONDS,
        )
        # Include one poll interval of scheduling and request-time slack.
        minimum_ttl = (every_polls + 1) * poll_interval
        return max(configured_ttl, minimum_ttl)

    async def process(
        self,
        evidence,
        product_data,
        lease_credential_key=None,
        lease_owner=None,
    ):
        scope = self.scope(product_data["asin"])
        for _ in range(4):
            stored = await self.store.load_product_state(scope)
            expected_version = stored.get("version") if stored else None
            if stored:
                state_payload = dict(stored)
                state_payload.pop("version", None)
                state_payload.pop("last_evidence", None)
                record = StockStateRecord.from_record(state_payload)
            else:
                record = StockStateRecord(scope_key=evidence.scope_key)

            decision = advance_state(
                record,
                evidence,
                self.policy,
                poll_interval=timedelta(
                    seconds=self.config.poll_interval_seconds
                ),
                oos_rearm_count=int(
                    os.getenv("STOCK_OOS_REARM_COUNT", "2")
                ),
            )
            labels = {
                "source": evidence.source.value.lower(),
                "state": decision.classification.state.value.lower(),
                "decision": decision.kind.value.lower(),
            }
            self.metrics.increment("stock_observation_total", labels=labels)
            logging.debug(
                "stock_observation %s",
                json.dumps(
                    {
                        "asin": product_data["asin"],
                        "request_class": evidence.source.value.lower(),
                        "classified_state": decision.classification.state.value,
                        "classifier_reason": decision.classification.reason,
                        "evidence_hash": decision.classification.evidence_hash,
                        "decision": decision.kind.value,
                        "accepted": decision.accepted,
                    },
                    separators=(",", ":"),
                ),
            )
            if not decision.accepted:
                return decision

            transition = None
            alert = None
            target_writes = ()
            event = decision.event
            speculative = (
                self.config.fast_alert
                and evidence.source is EvidenceSource.BATCH
                and decision.classification.state
                is StockState.BUYABLE_UNCONFIRMED
                and record.armed_for_restock
            )
            if speculative:
                candidate_epoch = record.epoch + 1
                transition_id = uuid4()
                transition = TransitionWrite(
                    transition_id=transition_id,
                    stock_epoch=candidate_epoch,
                    signal_type="offer_detected",
                    confirmed=False,
                    evidence_hash=decision.classification.evidence_hash,
                    evidence=_jsonable(asdict(evidence)),
                    detected_at=evidence.observed_at,
                )
                alert, target_writes = self._alert_writes(
                    transition, product_data, confirmed=False
                )
            elif event is not None:
                transition = TransitionWrite(
                    transition_id=UUID(event.transition_id),
                    stock_epoch=event.epoch,
                    signal_type="restock_confirmed",
                    confirmed=True,
                    evidence_hash=event.evidence_hash,
                    evidence=_jsonable(asdict(evidence)),
                    detected_at=event.opened_at,
                )
                alert, target_writes = self._alert_writes(
                    transition, product_data, confirmed=True
                )

            try:
                persist_started = time.monotonic()
                with trace_span(
                    self.tracer,
                    "stock.persist",
                    {
                        "stock.asin": product_data["asin"],
                        "stock.decision": decision.kind.value,
                    },
                ) as span:
                    if alert is not None:
                        alert = replace(
                            alert,
                            trace_context=_traceparent(span),
                        )
                    commit_kwargs = {
                        "transition": transition,
                        "alert": alert,
                        "targets": target_writes,
                        "evidence": asdict(evidence),
                    }
                    if lease_credential_key is not None and lease_owner is not None:
                        commit_kwargs.update(
                            lease_credential_key=lease_credential_key,
                            lease_owner=lease_owner,
                        )
                    persisted = await self.store.commit_stock_decision(
                        scope,
                        decision.next_record.to_record(),
                        expected_version,
                        **commit_kwargs,
                    )
                self.metrics.observe(
                    "alert_detect_to_persist_seconds",
                    time.monotonic() - persist_started,
                    labels={
                        "source": evidence.source.value.lower(),
                        "transition": str(transition is not None).lower(),
                    },
                )
                if persisted.get("delivery_ids") and self.outbox_wakeup is not None:
                    wake_started = time.monotonic()
                    self.outbox_wakeup.wake(
                        str(value) for value in persisted["delivery_ids"]
                    )
                    self.metrics.observe(
                        "alert_commit_to_wake_seconds",
                        time.monotonic() - wake_started,
                    )
                if persisted["transition_created"]:
                    self.metrics.increment(
                        "stock_transition_total",
                        labels={
                            "signal": transition.signal_type,
                            "confirmed": str(transition.confirmed).lower(),
                        },
                    )
                    logging.info(
                        "stock_transition %s",
                        json.dumps(
                            {
                                "asin": product_data["asin"],
                                "transition_id": str(transition.transition_id),
                                "stock_epoch": transition.stock_epoch,
                                "confirmed": transition.confirmed,
                                "evidence_hash": transition.evidence_hash,
                                "classifier_reason": decision.classification.reason,
                            },
                            separators=(",", ":"),
                        ),
                    )
                elif transition is not None:
                    self.metrics.increment(
                        "stock_transition_deduped_total",
                        labels={"signal": transition.signal_type},
                    )

                if (
                    evidence.source is EvidenceSource.BATCH
                    and decision.classification.state
                    is StockState.BUYABLE_UNCONFIRMED
                    and decision.previous_state
                    is not StockState.IN_STOCK_CONFIRMED
                ):
                    await self.store.enqueue_verification(
                        scope,
                        evidence.sequence,
                        asdict(evidence),
                        ttl_seconds=self.confirmation_ttl_seconds(),
                    )
                return decision
            except StateConflict:
                self.metrics.increment("stock_state_conflict_total")
                continue
        raise StateConflict("stock state changed repeatedly during observation")

    async def process_batch(
        self,
        observations,
        lease_credential_key=None,
        lease_owner=None,
    ):
        """Classify a polling batch from one state snapshot and commit once.

        ``observations`` is an iterable of ``(StockEvidence, product_data)``.
        The return value preserves caller order and includes the typed commit
        result used by the low-latency outbox path.
        """
        batch_started = time.monotonic()
        entries = tuple(observations)
        if not entries:
            return (), BatchCommitResult({}, (), (), ())
        scopes = [self.scope(product["asin"]) for _, product in entries]
        if len(set(scopes)) != len(scopes):
            raise ValueError("a batch may contain an ASIN only once")
        rearm_count = int(os.getenv("STOCK_OOS_REARM_COUNT", "2"))
        confirm_ttl = self.confirmation_ttl_seconds()
        for _ in range(4):
            stored_by_scope = await self.store.load_product_states(scopes)
            prepared = []
            # Buyable batch observations are evaluated first.  They do not
            # depend on another ASIN, but ordering them first minimizes the
            # time before a fast alert reaches the durable transaction.
            indexed = list(enumerate(entries))
            indexed.sort(
                key=lambda item: not (
                    item[1][0].source is EvidenceSource.BATCH
                    and bool(item[1][0].offer_id)
                )
            )
            results = [None] * len(entries)
            for index, (evidence, product_data) in indexed:
                scope = self.scope(product_data["asin"])
                stored = stored_by_scope.get(scope)
                expected_version = stored.get("version") if stored else None
                if stored:
                    state_payload = dict(stored)
                    state_payload.pop("version", None)
                    state_payload.pop("last_evidence", None)
                    record = StockStateRecord.from_record(state_payload)
                else:
                    record = StockStateRecord(scope_key=evidence.scope_key)
                decision = advance_state(
                    record,
                    evidence,
                    self.policy,
                    poll_interval=timedelta(seconds=self.config.poll_interval_seconds),
                    oos_rearm_count=rearm_count,
                )
                results[index] = decision
                self.metrics.increment(
                    "stock_observation_total",
                    labels={
                        "source": evidence.source.value.lower(),
                        "state": decision.classification.state.value.lower(),
                        "decision": decision.kind.value.lower(),
                    },
                )
                if not decision.accepted:
                    continue
                transition = None
                alert = None
                target_writes = ()
                speculative = (
                    self.config.fast_alert
                    and evidence.source is EvidenceSource.BATCH
                    and decision.classification.state is StockState.BUYABLE_UNCONFIRMED
                    and record.armed_for_restock
                )
                if speculative:
                    transition = TransitionWrite(
                        transition_id=uuid4(),
                        stock_epoch=record.epoch + 1,
                        signal_type="offer_detected",
                        confirmed=False,
                        evidence_hash=decision.classification.evidence_hash,
                        evidence=_jsonable(asdict(evidence)),
                        detected_at=evidence.observed_at,
                    )
                    alert, target_writes = self._alert_writes(
                        transition, product_data, confirmed=False
                    )
                elif decision.event is not None:
                    event = decision.event
                    transition = TransitionWrite(
                        transition_id=UUID(event.transition_id),
                        stock_epoch=event.epoch,
                        signal_type="restock_confirmed",
                        confirmed=True,
                        evidence_hash=event.evidence_hash,
                        evidence=_jsonable(asdict(evidence)),
                        detected_at=event.opened_at,
                    )
                    alert, target_writes = self._alert_writes(
                        transition, product_data, confirmed=True
                    )
                if alert is not None:
                    # A batch-level span can be added by the caller without
                    # changing the persistence contract.
                    alert = replace(alert, trace_context=None)
                verification = None
                if (
                    evidence.source is EvidenceSource.BATCH
                    and decision.classification.state is StockState.BUYABLE_UNCONFIRMED
                    and decision.previous_state is not StockState.IN_STOCK_CONFIRMED
                ):
                    verification = VerificationWrite(
                        source_sequence=evidence.sequence,
                        evidence=asdict(evidence),
                        ttl_seconds=confirm_ttl,
                    )
                prepared.append(
                    (index, transition, BatchStockDecision(
                        scope=scope,
                        state_record=decision.next_record.to_record(),
                        expected_version=expected_version,
                        evidence=asdict(evidence),
                        transition=transition,
                        alert=alert,
                        targets=tuple(target_writes),
                        verification=verification,
                    ))
                )
            if not prepared:
                return tuple(results), BatchCommitResult({}, (), (), ())
            try:
                persist_started = time.monotonic()
                result = await self.store.commit_stock_decisions(
                    [item[2] for item in prepared],
                    lease_credential_key=lease_credential_key,
                    lease_owner=lease_owner,
                )
                self.metrics.observe(
                    "stock_response_to_commit_seconds",
                    time.monotonic() - batch_started,
                    labels={"batch_size": str(len(entries))},
                )
                self.metrics.observe(
                    "alert_detect_to_persist_seconds",
                    time.monotonic() - persist_started,
                    labels={"source": "batch", "transition": str(bool(result.transition_ids)).lower()},
                )
                if result.delivery_ids and self.outbox_wakeup is not None:
                    wake_started = time.monotonic()
                    self.outbox_wakeup.wake(
                        str(value) for value in result.delivery_ids
                    )
                    self.metrics.observe(
                        "alert_commit_to_wake_seconds",
                        time.monotonic() - wake_started,
                    )
                created = set(result.transition_ids)
                for _, transition, _ in prepared:
                    if transition is None:
                        continue
                    if transition.transition_id in created:
                        self.metrics.increment(
                            "stock_transition_total",
                            labels={
                                "signal": transition.signal_type,
                                "confirmed": str(transition.confirmed).lower(),
                            },
                        )
                    else:
                        self.metrics.increment(
                            "stock_transition_deduped_total",
                            labels={"signal": transition.signal_type},
                        )
                return tuple(results), result
            except StateConflict:
                self.metrics.increment("stock_state_conflict_total")
                continue
        raise StateConflict("stock state changed repeatedly during batch observation")

    def _alert_writes(self, transition, product_data, confirmed):
        alert_id = uuid4()
        groups = list(self.asin_groups.get(product_data["asin"], ()))
        images = product_data.get("images") or []
        payload = {
            "asin": product_data["asin"],
            "title": product_data.get("title") or "Restock detected",
            "in_stock": True,
            "price": product_data.get("price"),
            "link": product_data.get("link")
            or f"https://www.{self.domain}/dp/{product_data['asin']}",
            "image": images[0] if images else None,
            "seller": product_data.get("seller"),
            "seller_verified": bool(product_data.get("seller_verified")),
            "source": product_data.get("source") or "tvss",
            "signal": transition.signal_type,
            "group": groups[0] if groups else None,
            "groups": groups,
            "ts": transition.detected_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "detected_at": transition.detected_at.isoformat(),
            "persisted_at": _utc_now().isoformat(),
            "alert_id": str(alert_id),
            "transition_id": str(transition.transition_id),
            "stock_epoch": transition.stock_epoch,
            "confirmed": bool(confirmed),
        }
        alert = AlertWrite(alert_id=alert_id, payload=payload)
        targets = tuple(
            TargetWrite(
                target_id=target.name,
                target_kind=target.kind,
                delivery_id=uuid4(),
            )
            for target in self.asin_targets.get(product_data["asin"], ())
        )
        return alert, targets

    async def enqueue_system_alert(self, signal, title, message):
        unique_targets = {}
        for targets in self.asin_targets.values():
            for target in targets:
                unique_targets[target.name] = target
        payload = {
            "asin": "SYSTEM",
            "title": title,
            "in_stock": False,
            "price": None,
            "link": "",
            "image": None,
            "seller": "monitor",
            "seller_verified": True,
            "source": "monitor",
            "signal": signal,
            "group": "monitor",
            "groups": ["monitor"],
            "ts": _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "detected_at": _utc_now().isoformat(),
            "persisted_at": _utc_now().isoformat(),
            "confirmed": True,
            "message": message,
        }
        targets = tuple(
            TargetWrite(
                target_id=target.name,
                target_kind=target.kind,
                delivery_id=uuid4(),
            )
            for target in unique_targets.values()
        )
        return await self.store.enqueue_system_alert(
            self.monitor_id,
            self.marketplace_id,
            signal,
            payload,
            targets,
        )


def _build_group_maps(config, webhook_targets):
    asin_groups = {}
    asin_targets = {}
    for group in config.groups:
        for asin in group.asins:
            asin_groups.setdefault(asin, [])
            if group.name not in asin_groups[asin]:
                asin_groups[asin].append(group.name)
            target_map = {
                target.name: target
                for target in asin_targets.setdefault(asin, [])
            }
            for name in group.webhook_names:
                target_map[name] = webhook_targets[name]
            asin_targets[asin] = list(target_map.values())
    return asin_groups, asin_targets


def _batch_product_payload(client, asin, observation):
    return {
        "asin": asin,
        "title": "Restock detected",
        "in_stock": observation.status is ObservationStatus.IN_STOCK,
        "link": f"https://www.{client.domain}/dp/{asin}",
        "images": [],
        "price": observation.price,
        "source": "tvss-batch",
        "offerId": observation.offer_id,
        "seller": None,
        "seller_verified": False,
    }


def _batch_evidence(coordinator, asin, observation, sequence, observed_at):
    return StockEvidence(
        scope_key=coordinator.scope_name(asin),
        sequence=sequence,
        observed_at=observed_at,
        source=EvidenceSource.BATCH,
        response_complete=bool(observation.response_complete),
        offer_id=(
            str(observation.offer_id)
            if observation.offer_id not in (None, "")
            else None
        ),
        offer_explicitly_null=observation.offer_explicitly_null,
        availability_condition=(
            str(observation.availability_condition)
            if observation.availability_condition is not None
            else None
        ),
        price=str(observation.price) if observation.price is not None else None,
    )


def _full_evidence(coordinator, product, sequence, observed_at):
    availability = product.get("availability") or {}
    return StockEvidence(
        scope_key=coordinator.scope_name(product["asin"]),
        sequence=sequence,
        observed_at=observed_at,
        source=EvidenceSource.FULL_PRODUCT,
        response_complete=bool(product.get("response_complete")),
        offer_id=product.get("offerId") or None,
        buyable_signals=tuple(product.get("buyable_signals") or ()),
        availability_condition=availability.get("availabilityCondition"),
        availability_status=availability.get("status"),
        primary_message=availability.get("primaryMessage"),
        secondary_message=availability.get("secondaryMessage"),
        sold_by_amazon=product.get("soldByAmazon"),
        seller_id=product.get("seller_id"),
        seller_name=product.get("seller"),
        price=product.get("price"),
    )


async def _poll_credential(
    client,
    session,
    store,
    coordinator,
    asins,
    worker_id,
    metrics,
    runtime_status,
    stop_event,
    calibration_store=None,
):
    auth_failure_started = None
    auth_grace = float(os.getenv("AUTH_FAILURE_GRACE_SECONDS", "30"))
    confirmation_throttle = ConfirmationThrottle(
        os.getenv("FAST_ALERT_CONFIRM_EVERY_POLLS", "12")
    )
    while not stop_event.is_set() and runtime_status.leader_owned:
        try:
            with trace_span(coordinator.tracer, "tvss.batch_poll"):
                batch = await client.batch_products(session, asins)
            request_timing = getattr(batch, "timing", None)
            if request_timing is not None:
                metrics.observe(
                    "tvss_request_duration_seconds",
                    max(0.0, request_timing.request_wall_ms / 1000.0),
                    labels={"request_class": "poll"},
                )
                logging.debug(
                    "tvss_request %s",
                    json.dumps(
                        {
                            "credential_hash": client.credential_key[:17],
                            "request_class": "poll",
                            "error_class": None,
                            "status_code": request_timing.status,
                            "request_seconds": round(
                                request_timing.request_wall_ms / 1000.0,
                                6,
                            ),
                            "cadence_wait_seconds": round(
                                request_timing.cadence_wait_ms / 1000.0,
                                6,
                            ),
                            "route_id": request_timing.route_id,
                            "attempts": request_timing.attempts,
                        },
                        separators=(",", ":"),
                    ),
                )
                metrics.observe(
                    "tvss_request_cadence_wait_seconds",
                    max(0.0, request_timing.cadence_wait_ms / 1000.0),
                    labels={"request_class": "poll"},
                )
            observed_at = _utc_now()
            sequence_base = time.time_ns()
            observations = tuple(
                (
                    _batch_evidence(
                        coordinator,
                        asin,
                        batch[asin],
                        sequence_base + index,
                        observed_at,
                    ),
                    _batch_product_payload(client, asin, batch[asin]),
                )
                for index, asin in enumerate(asins)
            )
            await coordinator.process_batch(
                observations,
                lease_credential_key=client.credential_key,
                lease_owner=client.credential_owner_id,
            )
            runtime_status.last_poll_success_at = time.time()
            metrics.set_gauge("tvss_poll_freshness_seconds", 0.0)
            auth_failure_started = None
            confirmation_throttle.note_poll()
        except CredentialLeaseFenceLost:
            # Do not accept a stale response or persist it after another
            # replica has acquired this credential.
            raise CredentialLeaseLost(
                "TVSS credential lease was fenced during poll work"
            )
        except TVSSRateLimitError as exc:
            if calibration_store is not None:
                await calibration_store.invalidate_credential(
                    client.credential_key, client.marketplace_id
                )
            snapshot = await client.durable_governor.raise_interval_floor(
                client.credential_key, PRODUCTION_MIN_INTERVAL_SECONDS
            )
            coordinator.config = replace(
                coordinator.config,
                poll_interval_seconds=max(
                    coordinator.config.poll_interval_seconds,
                    PRODUCTION_MIN_INTERVAL_SECONDS,
                ),
            )
            metrics.increment("tvss_rate_limit_total")
            metrics.increment("tvss_calibration_invalidated_total")
            metrics.set_gauge(
                "tvss_effective_calibrated_interval_seconds",
                coordinator.config.poll_interval_seconds,
            )
            metrics.set_gauge(
                "tvss_credential_cooldown_seconds",
                max(0.0, snapshot.blocked_until - time.time()),
            )
            metrics.set_gauge(
                "tvss_adaptive_interval_seconds",
                snapshot.interval_seconds,
            )
            logging.warning(
                "tvss_request %s",
                json.dumps(
                    {
                        "credential_hash": client.credential_key[:17],
                        "request_class": "poll",
                        "error_class": "rate_limited",
                        "retry_after_seconds": exc.retry_after,
                        "cooldown_until": snapshot.blocked_until,
                        "adaptive_interval_seconds": snapshot.interval_seconds,
                    },
                    separators=(",", ":"),
                ),
            )
        except TVSSConfigError:
            auth_failure_started = auth_failure_started or time.monotonic()
            logging.exception("TVSS authentication failure")
            if time.monotonic() - auth_failure_started >= auth_grace:
                await coordinator.enqueue_system_alert(
                    "tvss_auth_expired",
                    "TVSS authentication expired",
                    "Refresh the Amazon TVSS credentials and restart the monitor.",
                )
                stop_event.set()
                raise
        except asyncio.CancelledError:
            raise
        except Exception:
            metrics.increment(
                "tvss_poll_failure_total", labels={"class": "unexpected"}
            )
            logging.exception("Durable batch poll failed")

        if stop_event.is_set() or not runtime_status.leader_owned:
            break
        if not confirmation_throttle.due(coordinator.config.fast_alert):
            continue
        jobs = await store.claim_verification_jobs(
            worker_id,
            coordinator.monitor_id,
            coordinator.marketplace_id,
            coordinator.policy.fingerprint,
            limit=1,
            lease_seconds=float(
                os.getenv(
                    "STOCK_CONFIRM_LEASE_SECONDS",
                    str(DEFAULT_VERIFICATION_TTL_SECONDS),
                )
            ),
        )
        if not jobs:
            continue
        confirmation_throttle.consumed()
        job = jobs[0]
        try:
            confirmation_started = time.monotonic()
            product = await client.product(session, job["asin"])
            evidence = _full_evidence(
                coordinator,
                product,
                time.time_ns(),
                _utc_now(),
            )
            await coordinator.process(
                evidence,
                product,
                lease_credential_key=client.credential_key,
                lease_owner=client.credential_owner_id,
            )
            await store.finish_verification(
                job["job_id"], worker_id, success=True
            )
            created_at = job.get("created_at")
            if isinstance(created_at, datetime):
                metrics.observe(
                    "stock_confirmation_latency_seconds",
                    max(0.0, (_utc_now() - created_at).total_seconds()),
                )
            metrics.observe(
                "stock_confirmation_request_seconds",
                time.monotonic() - confirmation_started,
            )
        except asyncio.CancelledError:
            raise
        except CredentialLeaseFenceLost:
            raise CredentialLeaseLost(
                "TVSS credential lease was fenced during confirmation work"
            )
        except TVSSRateLimitError:
            if calibration_store is not None:
                await calibration_store.invalidate_credential(
                    client.credential_key, client.marketplace_id
                )
            await client.durable_governor.raise_interval_floor(
                client.credential_key, PRODUCTION_MIN_INTERVAL_SECONDS
            )
            coordinator.config = replace(
                coordinator.config,
                poll_interval_seconds=max(
                    coordinator.config.poll_interval_seconds,
                    PRODUCTION_MIN_INTERVAL_SECONDS,
                ),
            )
            metrics.increment("tvss_rate_limit_total")
            metrics.increment("tvss_calibration_invalidated_total")
            metrics.set_gauge(
                "tvss_effective_calibrated_interval_seconds",
                coordinator.config.poll_interval_seconds,
            )
            await store.finish_verification(
                job["job_id"],
                worker_id,
                success=False,
                retryable=True,
            )
        except Exception:
            logging.exception(
                "Full-product confirmation failed for ASIN %s", job["asin"]
            )
            await store.finish_verification(
                job["job_id"],
                worker_id,
                success=False,
                retryable=True,
            )


async def _leader_supervisor(
    governor,
    credential_key,
    owner_id,
    client,
    session,
    store,
    coordinator,
    asins,
    metrics,
    runtime_status,
    stop_event,
    calibration_store=None,
    calibration_key=None,
    requested_interval_seconds=PRODUCTION_MIN_INTERVAL_SECONDS,
    calibration_validity_seconds=CALIBRATION_VALIDITY_SECONDS,
):
    ttl = float(
        os.getenv("TVSS_LEADER_LEASE_SECONDS", str(DEFAULT_LEASE_TTL_SECONDS))
    )
    renew_every = float(
        os.getenv(
            "TVSS_LEADER_RENEW_SECONDS",
            str(DEFAULT_LEASE_RENEW_SECONDS),
        )
    )
    poll_task = None
    refresh_timeout = renew_every

    async def refresh_interval():
        nonlocal refresh_timeout
        if calibration_store is None or calibration_key is None:
            refresh_timeout = renew_every
            return PRODUCTION_MIN_INTERVAL_SECONDS
        interval, age = await calibration_store.activate_for_leader(
            calibration_key,
            requested_interval_seconds,
            owner_id,
            validity_seconds=calibration_validity_seconds,
        )
        coordinator.config = replace(
            coordinator.config,
            poll_interval_seconds=interval,
        )
        metrics.set_gauge(
            "tvss_effective_calibrated_interval_seconds", interval
        )
        metrics.set_gauge(
            "tvss_calibration_age_seconds",
            age if age is not None else -1,
        )
        if interval < PRODUCTION_MIN_INTERVAL_SECONDS:
            client.disable_proxy_fallback()
            client.proxy_pool.allow_network_fallback = False
            refresh_timeout = max(
                0.05,
                min(
                    renew_every,
                    calibration_validity_seconds - float(age or 0.0),
                ),
            )
        else:
            refresh_timeout = renew_every
        return interval

    try:
        while not stop_event.is_set():
            if not runtime_status.leader_owned:
                acquired = await governor.acquire_leader(
                    credential_key, owner_id, ttl
                )
                if acquired:
                    await refresh_interval()
                    runtime_status.leader_owned = True
                    runtime_status.last_lease_renewal_at = time.time()
                    metrics.set_gauge("tvss_lease_state", 1)
                    logging.info(
                        "Acquired TVSS credential leader lease key=%s",
                        credential_key[:17],
                    )
                    poll_task = asyncio.create_task(
                        _poll_credential(
                            client,
                            session,
                            store,
                            coordinator,
                            asins,
                            owner_id,
                            metrics,
                            runtime_status,
                            stop_event,
                            calibration_store,
                        ),
                        name="durable-tvss-poller",
                    )
            else:
                renewed = await governor.renew_leader(
                    credential_key, owner_id, ttl
                )
                if not renewed:
                    runtime_status.leader_owned = False
                    metrics.set_gauge("tvss_lease_state", 0)
                    metrics.increment("tvss_lease_loss_total")
                    logging.error("Lost TVSS credential leader lease")
                    if poll_task is not None:
                        poll_task.cancel()
                        await asyncio.gather(poll_task, return_exceptions=True)
                        poll_task = None
                    stop_event.set()
                    raise CredentialLeaseLost(
                        "TVSS credential leader lease was lost"
                    )
                else:
                    runtime_status.last_lease_renewal_at = time.time()
                    await refresh_interval()

            if poll_task is not None and poll_task.done():
                error = poll_task.exception()
                poll_task = None
                runtime_status.leader_owned = False
                metrics.set_gauge("tvss_lease_state", 0)
                if error is not None:
                    logging.error(
                        "TVSS poll leader stopped with error: %s", error
                    )
                    stop_event.set()
                    raise error
                if not stop_event.is_set():
                    stop_event.set()
                    raise RuntimeError(
                        "TVSS poll leader stopped unexpectedly"
                    )
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=refresh_timeout
                )
            except asyncio.TimeoutError:
                pass
    finally:
        runtime_status.leader_owned = False
        if poll_task is not None:
            poll_task.cancel()
            await asyncio.gather(poll_task, return_exceptions=True)
        await governor.release_leader(credential_key, owner_id)


async def _maintenance_loop(store, metrics, stop_event):
    while not stop_event.is_set():
        try:
            removed = await store.cleanup_expired_dead_letters()
            if removed:
                metrics.increment(
                    "alert_dead_letter_expired_total",
                    value=removed,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            metrics.increment(
                "monitor_maintenance_error_total",
                labels={"task": "dead_letter_cleanup"},
            )
            logging.exception("Dead-letter retention cleanup failed")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=3600.0)
        except asyncio.TimeoutError:
            pass


async def _outbox_notification_listener(
    store,
    wakeup,
    stop_event,
    reconnect_initial_seconds=1.0,
    reconnect_max_seconds=30.0,
    reconnect_wait=None,
):
    """Keep the outbox LISTEN subscription alive across connection loss."""
    adapter = PostgresOutboxNotificationAdapter(wakeup)
    reconnect_wait = reconnect_wait or _wait_for_stop
    backoff = max(0.0, float(reconnect_initial_seconds))
    max_backoff = max(backoff, float(reconnect_max_seconds))
    while not stop_event.is_set():
        connection_lost = asyncio.Event()
        try:
            async with store.pool.acquire() as connection:
                listener_registered = False
                termination_listener_registered = False

                def receive(_connection, _pid, _channel, payload):
                    adapter.on_notification(payload)

                def connection_terminated(_connection):
                    connection_lost.set()

                try:
                    await connection.add_listener(
                        "alert_outbox_ready",
                        receive,
                    )
                    listener_registered = True
                    connection.add_termination_listener(connection_terminated)
                    termination_listener_registered = True
                    backoff = max(0.0, float(reconnect_initial_seconds))

                    waiters = (
                        asyncio.create_task(stop_event.wait()),
                        asyncio.create_task(connection_lost.wait()),
                    )
                    try:
                        await asyncio.wait(
                            waiters,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    finally:
                        for waiter in waiters:
                            waiter.cancel()
                        await asyncio.gather(
                            *waiters,
                            return_exceptions=True,
                        )
                finally:
                    if listener_registered:
                        with suppress(Exception):
                            await connection.remove_listener(
                                "alert_outbox_ready",
                                receive,
                            )
                    if termination_listener_registered:
                        with suppress(Exception):
                            connection.remove_termination_listener(
                                connection_terminated
                            )
        except asyncio.CancelledError:
            raise
        except Exception:
            if not stop_event.is_set():
                logging.exception("Outbox notification listener disconnected")

        if stop_event.is_set():
            break
        await reconnect_wait(stop_event, backoff)
        backoff = min(max_backoff, max(0.001, backoff * 2))


async def _wait_for_stop(stop_event, seconds):
    """Wait for either a reconnect delay or a clean shutdown request."""
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def _connection_warmer(session, targets, metrics, stop_event):
    interval = float(os.getenv("ALERT_CONNECTION_WARM_SECONDS", "90"))
    if interval <= 0:
        return
    urls = []
    for target_id, target in targets.items():
        if target.kind == "discord":
            urls.append((target_id, target.url))
            continue
        warmup_url = str(
            os.getenv(f"WEBHOOK_{target_id.upper()}_WARMUP_URL", "")
        ).strip()
        if warmup_url:
            urls.append((target_id, warmup_url))
    while not stop_event.is_set():
        for target_id, url in urls:
            started = time.monotonic()
            try:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=3),
                ) as response:
                    response.release()
                metrics.observe(
                    "alert_connection_warm_seconds",
                    time.monotonic() - started,
                    labels={"target": target_id, "outcome": "response"},
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                metrics.increment(
                    "alert_connection_warm_total",
                    labels={"target": target_id, "outcome": "error"},
                )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def run_durable_monitor(config, webhook_targets):
    monitor_id = validate_durable_configuration(config)
    store = await connect_and_migrate(
        pool_max_size=int(os.getenv("DATABASE_POOL_SIZE", "40"))
    )
    client = None
    operations = None
    try:
        client = TVSSClient()
        configured_credential_id = str(
            os.getenv("TVSS_CREDENTIAL_ID", "")
        ).strip()
        if not configured_credential_id and not client.has_stable_device_identity:
            raise TVSSConfigError(
                "TVSS_CREDENTIAL_ID is required when credentials do not include "
                "a stable device identity"
            )
        credential_identity = configured_credential_id or client.device_udid
        if not credential_identity:
            raise TVSSConfigError(
                "TVSS_CREDENTIAL_ID or stable TVSS device identity is required"
            )
        credential_key = stable_credential_key(
            f"{client.marketplace_id}:{credential_identity}",
            salt=str(os.getenv("TVSS_CREDENTIAL_SALT", "")),
        )
        owner_id = new_owner_id()
        all_asins = list(
            dict.fromkeys(
                asin for group in config.groups for asin in group.asins
            )
        )
        if len(all_asins) > 20:
            raise TVSSConfigError(
                "durable batch monitor supports at most 20 unique ASINs"
            )
        calibration_store = PostgresCadenceCalibrationStore(store.pool)
        await calibration_store.initialize()
        calibration_key = CalibrationKey(
            credential_key=credential_key,
            marketplace_id=client.marketplace_id,
            region=str(os.getenv("RAILWAY_REPLICA_REGION", "local")),
            direct_route=bool(client.proxy_pool.primary_route.is_direct),
            batch_size=len(all_asins),
        )
        calibration_validity_seconds = float(
            os.getenv(
                "TVSS_CALIBRATION_VALIDITY_SECONDS",
                str(CALIBRATION_VALIDITY_SECONDS),
            )
        )
        standby_interval = max(
            config.poll_interval_seconds,
            PRODUCTION_MIN_INTERVAL_SECONDS,
        )
        effective_config = replace(
            config, poll_interval_seconds=standby_interval
        )
        governor = PostgresCredentialGovernor(
            store.pool,
            base_interval=standby_interval,
            cooldown_seconds=float(
                os.getenv("TVSS_429_COOLDOWN_SECONDS", "900")
            ),
            max_cooldown_seconds=float(
                os.getenv("TVSS_MAX_COOLDOWN_SECONDS", "3600")
            ),
            max_interval_seconds=float(
                os.getenv("TVSS_MAX_INTERVAL_SECONDS", "300")
            ),
            recovery_success_count=int(
                os.getenv("TVSS_RECOVERY_SUCCESS_COUNT", "120")
            ),
            recovery_decrement_seconds=float(
                os.getenv("TVSS_INTERVAL_DECREMENT", "0.25")
            ),
        )
        await governor.initialize()
        client.configure_durable_governor(governor, credential_key, owner_id)

        asin_groups, asin_targets = _build_group_maps(
            config, webhook_targets
        )
        metrics = DeliveryMetrics()
        metrics.set_gauge(
            "tvss_effective_calibrated_interval_seconds", standby_interval
        )
        metrics.set_gauge("tvss_calibration_age_seconds", -1)
        delivery_health = DeliveryHealth(repository_ready=True)
        runtime_status = RuntimeStatus()
        tracer = configure_tracing()
        outbox_wakeup = OutboxWakeup()
        coordinator = DurableStockCoordinator(
            store,
            effective_config,
            monitor_id,
            client.marketplace_id,
            client.domain,
            asin_groups,
            asin_targets,
            metrics,
            tracer=tracer,
            outbox_wakeup=outbox_wakeup,
        )
        stop_event = asyncio.Event()

        loop = asyncio.get_running_loop()

        def stop(received):
            logging.info("Received signal %s; stopping durable monitor", received)
            runtime_status.stopping = True
            stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop, sig.name)
            except NotImplementedError:
                pass

        tvss_connector = aiohttp.TCPConnector(
            limit=0,
            ttl_dns_cache=300,
            keepalive_timeout=120,
            enable_cleanup_closed=True,
        )
        alert_connector = aiohttp.TCPConnector(
            limit=0,
            ttl_dns_cache=300,
            keepalive_timeout=120,
            enable_cleanup_closed=True,
        )
        async with (
            aiohttp.ClientSession(connector=tvss_connector) as tvss_session,
            aiohttp.ClientSession(connector=alert_connector) as alert_session,
        ):
            worker_concurrency = int(
                os.getenv("ALERT_WORKER_CONCURRENCY", "32")
            )
            target_concurrency = int(
                os.getenv("ALERT_TARGET_CONCURRENCY", "2")
            )
            circuit_failure_threshold = int(
                os.getenv("ALERT_CIRCUIT_FAILURE_THRESHOLD", "5")
            )
            circuit_open_seconds = float(
                os.getenv("ALERT_CIRCUIT_OPEN_SECONDS", "60")
            )
            repository = PostgresOutboxRepository(
                store,
                webhook_targets,
                owner_id,
                global_concurrency=worker_concurrency,
                per_target_concurrency=target_concurrency,
                circuit_failure_threshold=circuit_failure_threshold,
                circuit_open_seconds=circuit_open_seconds,
                dead_letter_retention_days=int(
                    os.getenv(
                        "ALERT_DEAD_LETTER_RETENTION_DAYS",
                        "30",
                    )
                ),
                metrics=metrics,
                wakeup=outbox_wakeup,
            )
            sender = DurableDeliverySender(alert_session, webhook_targets)
            worker = AlertDeliveryWorker(
                repository,
                sender,
                metrics=metrics,
                health=delivery_health,
                concurrency=worker_concurrency,
                per_target_concurrency=target_concurrency,
                lease_seconds=float(
                    os.getenv("ALERT_LEASE_SECONDS", "30")
                ),
                max_attempts=int(os.getenv("ALERT_MAX_ATTEMPTS", "10")),
                max_age_seconds=float(
                    os.getenv("ALERT_MAX_AGE_SECONDS", "900")
                ),
                max_backoff_seconds=float(
                    os.getenv("ALERT_MAX_BACKOFF_SECONDS", "60")
                ),
                circuit_failure_threshold=circuit_failure_threshold,
                circuit_open_seconds=circuit_open_seconds,
                wakeup=outbox_wakeup,
                fallback_poll_seconds=float(
                    os.getenv("ALERT_OUTBOX_FALLBACK_POLL_SECONDS", "1")
                ),
            )
            operations = OperationsServer(
                store,
                metrics,
                delivery_health,
                runtime_status,
                int(os.getenv("METRICS_PORT", str(DEFAULT_METRICS_PORT))),
            )
            await operations.start()
            worker_task = asyncio.create_task(
                worker.run(), name="durable-alert-worker"
            )
            leader_task = asyncio.create_task(
                _leader_supervisor(
                    governor,
                    credential_key,
                    owner_id,
                    client,
                    tvss_session,
                    store,
                    coordinator,
                    all_asins,
                    metrics,
                    runtime_status,
                    stop_event,
                    calibration_store,
                    calibration_key,
                    config.poll_interval_seconds,
                    calibration_validity_seconds,
                ),
                name="credential-leader-supervisor",
            )
            maintenance_task = asyncio.create_task(
                _maintenance_loop(store, metrics, stop_event),
                name="durable-maintenance",
            )
            notification_task = asyncio.create_task(
                _outbox_notification_listener(
                    store, outbox_wakeup, stop_event
                ),
                name="outbox-notification-listener",
            )
            warmer_task = asyncio.create_task(
                _connection_warmer(
                    alert_session, webhook_targets, metrics, stop_event
                ),
                name="alert-connection-warmer",
            )
            await stop_event.wait()
            runtime_status.stopping = True
            worker.stop()
            results = await asyncio.gather(
                leader_task,
                worker_task,
                maintenance_task,
                notification_task,
                warmer_task,
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, BaseException) and not isinstance(
                    result, asyncio.CancelledError
                ):
                    raise result
    finally:
        if operations is not None:
            await operations.close()
        await store.close()


async def run_alert_admin(action, delivery_id=None, limit=100):
    store = await connect_and_migrate(pool_max_size=2)
    try:
        if action == "list":
            rows = await store.list_dead_letters(limit=limit)
            print(json.dumps(_jsonable(rows), sort_keys=True))
            return 0
        if not delivery_id:
            raise TVSSConfigError("delivery ID is required")
        parsed = UUID(str(delivery_id))
        if action == "replay":
            changed = await store.replay_delivery(parsed)
        elif action == "suppress":
            changed = await store.suppress_delivery(parsed)
        else:
            raise TVSSConfigError(f"unknown alerts action: {action}")
        print(
            json.dumps(
                {
                    "action": action,
                    "delivery_id": str(parsed),
                    "changed": changed,
                },
                sort_keys=True,
            )
        )
        return 0 if changed else 1
    finally:
        await store.close()


@asynccontextmanager
async def exclusive_canary_lease(
    client,
    base_interval=5.0,
    calibration=False,
):
    """Prevent calibration traffic from overlapping a production poll leader."""
    base_interval = float(base_interval)
    if base_interval < PRODUCTION_MIN_INTERVAL_SECONDS and not calibration:
        raise TVSSConfigError(
            "sub-5-second canary traffic requires explicit calibration mode"
        )
    store = await connect_and_migrate(pool_max_size=2)
    configured_credential_id = str(
        os.getenv("TVSS_CREDENTIAL_ID", "")
    ).strip()
    if not configured_credential_id and not client.has_stable_device_identity:
        await store.close()
        raise TVSSConfigError(
            "TVSS_CREDENTIAL_ID is required for credential-safe canaries"
        )
    identity = configured_credential_id or client.device_udid
    key = stable_credential_key(
        f"{client.marketplace_id}:{identity}",
        salt=str(os.getenv("TVSS_CREDENTIAL_SALT", "")),
    )
    owner_id = new_owner_id()
    governor = PostgresCredentialGovernor(
        store.pool,
        base_interval=base_interval,
        cooldown_seconds=float(
            os.getenv("TVSS_429_COOLDOWN_SECONDS", "900")
        ),
        max_cooldown_seconds=float(
            os.getenv("TVSS_MAX_COOLDOWN_SECONDS", "3600")
        ),
        max_interval_seconds=float(
            os.getenv("TVSS_MAX_INTERVAL_SECONDS", "300")
        ),
        recovery_success_count=int(
            os.getenv("TVSS_RECOVERY_SUCCESS_COUNT", "120")
        ),
        recovery_decrement_seconds=float(
            os.getenv("TVSS_INTERVAL_DECREMENT", "0.25")
        ),
    )
    await governor.initialize()
    acquired = await governor.acquire_leader(
        key,
        owner_id,
        float(
            os.getenv(
                "TVSS_CANARY_LEASE_SECONDS",
                str(4 * 60 * 60),
            )
        ),
    )
    if not acquired:
        await store.close()
        raise TVSSConfigError(
            "production or another canary currently owns this TVSS credential"
        )
    await governor.set_interval_floor(
        key,
        base_interval,
        allow_lower=calibration,
    )
    client.configure_durable_governor(governor, key, owner_id)
    try:
        yield {
            "store": store,
            "governor": governor,
            "credential_key": key,
            "owner_id": owner_id,
        }
    finally:
        await governor.release_leader(key, owner_id)
        await store.close()
