import asyncio
import json
import logging
import os
import random
import re
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
import discord
from discord import Embed, Webhook
from dotenv import load_dotenv

from amazon_tvss import (
    BatchObservation,
    ObservationStatus,
    TVSSClient,
    TVSSConfigError,
    TVSSRateLimitError,
)


load_dotenv("endpoint.env")


_LOG_LEVEL_NAME = os.getenv("LOG_LEVEL", "INFO").strip().upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL_NAME, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
)


ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")
DEFAULT_POLL_INTERVAL_SECONDS = 5.0
MIN_POLL_INTERVAL_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 60.0
GENERIC_WEBHOOK_TIMEOUT_SECONDS = 5.0
WEBHOOK_RETRY_ATTEMPTS = 3
WEBHOOK_RETRY_BACKOFF_SECONDS = 1.0

# The lower-level endpoint hard-caps batches at 50. The optimized monitor
# deliberately accepts one batch of at most 20 ASINs.
TVSS_BATCH_HARD_CAP = 50
TVSS_MONITOR_ASIN_CAP = 20
DEFAULT_BATCH_CHUNK_SIZE = 20
DEFAULT_BATCH_CONCURRENCY = 1
MAX_BATCH_CONCURRENCY = 4

# Legacy exports retained for configuration and test compatibility. Active
# pacing and cooldown behavior lives in CredentialRateController.
JITTER_FRACTION = 0.15
AIMD_MULT = 2.0
AIMD_DECREMENT = 0.05
AIMD_DECREMENT_AFTER = 30
AIMD_INTERVAL_CAP = 120.0

PENALTY_BOX_THRESHOLD = 3
PENALTY_BOX_SLEEP = 90.0

# Bound concurrent full-product fetches a single chunk loop can fan out
# during a coordinated restock (50 ASINs flipping in one batch).
TRANSITION_FETCH_CONCURRENCY = 4


class MonitorConfigError(ValueError):
    pass


@dataclass(frozen=True)
class MonitorGroup:
    name: str
    asins: list
    webhook_names: list


@dataclass(frozen=True)
class MonitorConfig:
    poll_interval_seconds: float
    groups: list
    require_amazon_seller: bool = True
    # When True, batch-mode alerts fire on OOS→IN from basicproducts
    # (has_offer) without waiting for a full product() confirm. Seller
    # filtering, if enabled, runs async and only logs (does not retract).
    fast_alert: bool = False


class AlertState:
    """Tracks stock state and returns true only on out-of-stock to in-stock moves.

    The first observation of any ASIN is treated as priming and never alerts,
    so a process restart while an item is in stock does not fire a false-positive
    transition alert.

    The split peek/commit API lets callers defer state mutation until alert
    delivery has succeeded, so a webhook outage does not cause a transition
    alert to be silently lost: peek() answers "would observe() fire?" without
    mutating, and commit() records the observation. observe() is preserved as
    peek-then-commit for callers that don't care about delivery confirmation.
    """

    def __init__(self):
        self._states = {}
        self._inflight = set()

    def peek(self, asin, in_stock):
        in_stock = bool(in_stock)
        if asin not in self._states:
            return False
        return in_stock and not self._states[asin]

    def commit(self, asin, in_stock):
        self._states[asin] = bool(in_stock)

    def observe(self, asin, in_stock):
        result = self.peek(asin, in_stock)
        self.commit(asin, in_stock)
        return result

    def reserve_transition(self, asin, status):
        if status is not ObservationStatus.IN_STOCK:
            return False
        if asin in self._inflight or not self.peek(asin, True):
            return False
        self._inflight.add(asin)
        return True

    def finish_transition(self, asin, delivered):
        if delivered:
            self.commit(asin, True)
        self._inflight.discard(asin)

    def has_inflight(self, asin):
        return asin in self._inflight


def parse_poll_interval(env):
    raw_value = env.get("POLL_INTERVAL_SECONDS", str(DEFAULT_POLL_INTERVAL_SECONDS))
    try:
        interval = float(raw_value)
    except (TypeError, ValueError):
        raise MonitorConfigError("POLL_INTERVAL_SECONDS must be a number")

    if interval < MIN_POLL_INTERVAL_SECONDS:
        raise MonitorConfigError(
            f"POLL_INTERVAL_SECONDS must be at least {MIN_POLL_INTERVAL_SECONDS}"
        )
    return interval


def clamp(value, lo, hi):
    return max(lo, min(hi, value))


def jittered(interval, fraction=JITTER_FRACTION):
    return interval * (1 + random.uniform(-fraction, fraction))


def parse_monitor_config_json(raw_value):
    if not raw_value or not raw_value.strip():
        raise MonitorConfigError("MONITOR_CONFIG_JSON is required")

    try:
        config_data = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise MonitorConfigError(f"MONITOR_CONFIG_JSON is invalid JSON: {exc}") from exc

    if not isinstance(config_data, dict):
        raise MonitorConfigError("MONITOR_CONFIG_JSON must be a JSON object")

    return config_data


def normalize_asins(raw_asins, group_name):
    if not isinstance(raw_asins, list) or not raw_asins:
        raise MonitorConfigError(f"group '{group_name}' must define at least one ASIN")

    asins = []
    seen = set()
    for raw_asin in raw_asins:
        asin = str(raw_asin).strip().upper()
        if not ASIN_RE.match(asin):
            raise MonitorConfigError(
                f"group '{group_name}' contains invalid ASIN '{raw_asin}'"
            )
        if asin not in seen:
            asins.append(asin)
            seen.add(asin)

    return asins


def normalize_webhook_names(raw_names, group_name, valid_target_names):
    if not isinstance(raw_names, list) or not raw_names:
        raise MonitorConfigError(f"group '{group_name}' must define webhook names")

    names = []
    seen = set()
    for raw_name in raw_names:
        name = str(raw_name).strip()
        if not name:
            raise MonitorConfigError(f"group '{group_name}' contains a blank webhook name")
        if name not in valid_target_names:
            raise MonitorConfigError(
                f"group '{group_name}' references unknown webhook '{name}'"
            )
        if name not in seen:
            names.append(name)
            seen.add(name)

    return names


def load_monitor_config(env=None, webhook_targets=None):
    env = os.environ if env is None else env
    webhook_targets = webhook_targets if webhook_targets is not None else WEBHOOK_TARGETS
    valid_target_names = set(webhook_targets.keys())

    config_data = parse_monitor_config_json(env.get("MONITOR_CONFIG_JSON", ""))
    groups_data = config_data.get("groups")
    if not isinstance(groups_data, list) or not groups_data:
        raise MonitorConfigError("MONITOR_CONFIG_JSON must include at least one group")

    default_webhooks = config_data.get("default_webhooks", [])
    groups = []
    for index, group_data in enumerate(groups_data, start=1):
        if not isinstance(group_data, dict):
            raise MonitorConfigError(f"group #{index} must be a JSON object")

        name = str(group_data.get("name", "")).strip()
        if not name:
            raise MonitorConfigError(f"group #{index} must define a name")

        asins = normalize_asins(group_data.get("asins"), name)
        raw_webhooks = group_data.get("webhooks", default_webhooks)
        webhook_names = normalize_webhook_names(raw_webhooks, name, valid_target_names)
        groups.append(MonitorGroup(name=name, asins=asins, webhook_names=webhook_names))

    raw_seller_filter = str(env.get("MONITOR_REQUIRE_AMAZON_SELLER", "true")).strip().lower()
    require_amazon_seller = raw_seller_filter not in ("0", "false", "no", "off")

    # Accuracy-first default: seller-qualified full confirmation precedes alerts.
    # Fast mode remains an explicit speculative, separately labeled signal.
    raw_fast = str(env.get("MONITOR_FAST_ALERT", "false")).strip().lower()
    fast_alert = raw_fast not in ("0", "false", "no", "off")

    return MonitorConfig(
        poll_interval_seconds=parse_poll_interval(env),
        groups=groups,
        require_amazon_seller=require_amazon_seller,
        fast_alert=fast_alert,
    )


def product_from_batch(asin, batch_info, domain="amazon.com"):
    """Minimal product payload for fast-path alerts (no full product fetch)."""
    price = None
    if isinstance(batch_info, (dict, BatchObservation)):
        price = batch_info.get("price")
    return {
        "asin": asin,
        "title": "Restock detected",
        "in_stock": True,
        "link": f"https://www.{domain}/dp/{asin}",
        "images": [],
        "price": price,
        "source": "tvss-batch",
        "signal": "offer_detected",
        "seller_verified": False,
        "seller": None,
        "soldByAmazon": None,
        "availability": {
            "primaryMessage": "Unconfirmed buyable offer detected"
        },
    }


def build_generic_payload(product_data, group_name=None, ts=None):
    price = product_data.get("price")
    offers = product_data.get("offers") or []
    if not price and offers:
        price = offers[0].get("priceInfo", {}).get("price")

    if ts is None:
        ts = datetime.now(timezone.utc)

    images = product_data.get("images") or []
    return {
        "asin": product_data.get("asin"),
        "title": product_data.get("title"),
        "in_stock": bool(product_data.get("in_stock")),
        "price": price,
        "link": product_data.get("link"),
        "image": images[0] if images else None,
        "seller": product_data.get("seller"),
        "signal": product_data.get("signal"),
        "seller_verified": bool(product_data.get("seller_verified")),
        "source": product_data.get("source"),
        "group": group_name,
        "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


class _SkippedDelivery(Exception):
    """Sentinel raised by `_noop_skip` so the gather loop can distinguish a
    backed-off-target skip from a successful or failed delivery."""


class AlertDispatcher:
    def __init__(self, session, clock=None):
        self.session = session
        self._clock = clock or time.monotonic
        self._target_backoff_until = {}
        self.last_ack_ms = 0.0

    def _is_target_backed_off(self, target):
        deadline = self._target_backoff_until.get(target.name)
        return deadline is not None and self._clock() < deadline

    async def send_notification(self, product_data, webhook_targets, group_name=None, ts=None):
        """Returns True iff at least one target accepted the alert. The caller
        uses this to decide whether to commit a state transition; a False return
        means the alert should be retried on the next observation."""
        if not webhook_targets:
            logging.error("No webhook targets selected for ASIN %s", product_data.get("asin"))
            return False

        ack_started_ns = time.perf_counter_ns()

        if ts is None:
            ts = datetime.now(timezone.utc)

        embed = self.create_embed(product_data, group_name, ts)
        payload = build_generic_payload(product_data, group_name, ts)

        tasks = []
        for target in webhook_targets:
            if target.kind == "generic":
                tasks.append(self._send_generic(target, payload))
            elif self._is_target_backed_off(target):
                tasks.append(self._noop_skip(target.name))
            else:
                tasks.append(self._send_discord(target, embed))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        delivered = 0
        for target, result in zip(webhook_targets, results):
            if isinstance(result, _SkippedDelivery):
                continue
            if isinstance(result, discord.HTTPException) and result.status == 429:
                retry_after = float(getattr(result, "retry_after", 1) or 1)
                self._target_backoff_until[target.name] = self._clock() + retry_after + 1.0
                logging.warning(
                    "Discord rate limited target %s; backing off that target for %.2fs",
                    target.name,
                    retry_after,
                )
            elif isinstance(result, Exception):
                logging.error("Webhook send failed for %s: %s", target.name, result)
            else:
                delivered += 1
        self.last_ack_ms = (time.perf_counter_ns() - ack_started_ns) / 1_000_000
        return delivered > 0

    async def _noop_skip(self, name):
        logging.warning("Skipping Discord target %s while rate limit is active", name)
        raise _SkippedDelivery(name)

    async def _send_discord(self, target, embed):
        last_exc = None
        for attempt in range(WEBHOOK_RETRY_ATTEMPTS):
            try:
                webhook = Webhook.from_url(target.url, session=self.session)
                content = f"<@&{target.role_id}>" if target.role_id else None
                await webhook.send(content=content, embed=embed)
                return
            except discord.HTTPException as exc:
                # 429 must propagate immediately so the dispatcher can install
                # per-target backoff; do not retry it internally.
                if exc.status == 429:
                    raise
                last_exc = exc
                retryable = exc.status is not None and 500 <= exc.status < 600
                if retryable and attempt + 1 < WEBHOOK_RETRY_ATTEMPTS:
                    await asyncio.sleep(WEBHOOK_RETRY_BACKOFF_SECONDS * (2 ** attempt))
                    continue
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                if attempt + 1 < WEBHOOK_RETRY_ATTEMPTS:
                    await asyncio.sleep(WEBHOOK_RETRY_BACKOFF_SECONDS * (2 ** attempt))
                    continue
                raise
        if last_exc is not None:
            raise last_exc

    async def _send_generic(self, target, payload):
        last_exc = None
        for attempt in range(WEBHOOK_RETRY_ATTEMPTS):
            try:
                async with self.session.post(
                    target.url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=GENERIC_WEBHOOK_TIMEOUT_SECONDS),
                ) as response:
                    if 200 <= response.status < 300:
                        return
                    body = await response.text()
                    err = RuntimeError(
                        f"generic webhook {target.name} HTTP {response.status}: {body[:200]}"
                    )
                    if 500 <= response.status < 600 and attempt + 1 < WEBHOOK_RETRY_ATTEMPTS:
                        last_exc = err
                        await asyncio.sleep(WEBHOOK_RETRY_BACKOFF_SECONDS * (2 ** attempt))
                        continue
                    raise err
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                if attempt + 1 < WEBHOOK_RETRY_ATTEMPTS:
                    await asyncio.sleep(WEBHOOK_RETRY_BACKOFF_SECONDS * (2 ** attempt))
                    continue
                raise
        if last_exc is not None:
            raise last_exc

    def create_embed(self, product_data, group_name=None, ts=None):
        title = (
            f"Amazon Stock Monitor — {group_name}"
            if group_name
            else "Amazon Stock Monitor"
        )
        embed = Embed(title=title, color=discord.Color.purple())
        price = product_data.get("price") or "MSRP"

        if product_data.get("offers"):
            price_info = product_data["offers"][0].get("priceInfo", {})
            price = price_info.get("price") or price

        if product_data.get("images"):
            embed.set_thumbnail(url=product_data["images"][0])

        product_link = product_data.get("link", "")
        product_name = product_data.get("title", "N/A")
        seller_name = product_data.get("seller") or "Seller unconfirmed"
        product_name_with_link = (
            f"[{product_name}]({product_link})" if product_link else product_name
        )

        embed.add_field(
            name="Product Details",
            value=(
                f"**{product_name_with_link}**\n"
                f"**SKU:** {product_data.get('asin', 'N/A')}\n"
                f"**Price:** {price}\n"
                f"**Condition:** New\n"
                f"**Sold By:** {seller_name}\n"
                f"**Source:** {product_data.get('source', 'tvss')}"
            ),
            inline=False,
        )

        availability = product_data.get("availability") or {}
        availability_message = (
            availability.get("primaryMessage")
            or availability.get("status")
            or availability.get("availabilityCondition")
        )
        if availability_message:
            embed.add_field(
                name="Availability",
                value=str(availability_message),
                inline=False,
            )

        if ts is None:
            ts = datetime.now(timezone.utc)
        embed.set_footer(text=f"Amazon Stock Monitor | {ts.strftime('%Y-%m-%d %H:%M:%S')}")
        return embed


def selected_webhook_targets(group, webhook_targets):
    return [webhook_targets[name] for name in group.webhook_names]


def asin_backoff_seconds(base_seconds, failure_count):
    if failure_count <= 0:
        return base_seconds
    return min(
        max(base_seconds, 1.0) * (2 ** min(failure_count - 1, 5)),
        MAX_BACKOFF_SECONDS,
    )


def auth_expired_alert_payload():
    return {
        "asin": "N/A",
        "title": "TVSS auth expired",
        "in_stock": False,
        "link": "",
        "images": [],
        "price": None,
        "seller": "monitor",
        "source": "monitor",
        "availability": {
            "primaryMessage": "Run 'python main.py login' to refresh credentials, then restart the monitor.",
        },
    }


class AuthFailureWatch:
    """Trips when no successful poll has happened within `grace_seconds`
    AND at least one auth failure has been observed in that window.

    Designed for per-ASIN concurrent polling: any successful poll on any
    ASIN resets the watch, while any auth failure flags the watch as
    "armed". This separates true credential expiry (every ASIN auth-fails)
    from intermittent network errors (no auth-fail flag set).
    """

    def __init__(self, grace_seconds=30.0, clock=None):
        self._grace = float(grace_seconds)
        self._clock = clock or time.monotonic
        self._last_success = self._clock()
        self._auth_failure_seen = False

    def record_success(self):
        self._last_success = self._clock()
        self._auth_failure_seen = False

    def record_auth_failure(self):
        self._auth_failure_seen = True

    def record_other_failure(self):
        return

    def is_tripped(self):
        return (
            self._auth_failure_seen
            and (self._clock() - self._last_success) > self._grace
        )


async def poll_asin_loop(
    asin,
    group,
    tvss_client,
    session,
    state,
    dispatcher,
    targets,
    auth_watch,
    semaphore,
    config,
    shutdown_event,
    auth_expired_event,
):
    failure_count = 0
    while not shutdown_event.is_set():
        product = None
        try:
            async with semaphore:
                product = await tvss_client.product(session, asin)
            auth_watch.record_success()
            failure_count = 0
        except TVSSConfigError as exc:
            auth_watch.record_auth_failure()
            failure_count += 1
            logging.error("ASIN %s auth failure: %s", asin, exc)
            if auth_watch.is_tripped():
                auth_expired_event.set()
                return
        except Exception as exc:
            auth_watch.record_other_failure()
            failure_count += 1
            logging.error("ASIN %s poll failed: %s", asin, exc)

        if product is not None:
            try:
                in_stock_raw = bool(product.get("in_stock"))
                in_stock = in_stock_raw
                if (
                    config.require_amazon_seller
                    and in_stock_raw
                    and not product.get("soldByAmazon")
                ):
                    in_stock = False
                    logging.debug(
                        "Group %s ASIN %s: IN_STOCK_FILTERED (third-party seller)",
                        group.name,
                        asin,
                    )
                else:
                    logging.debug(
                        "Group %s ASIN %s: %s",
                        group.name,
                        asin,
                        "IN_STOCK" if in_stock else "OUT_OF_STOCK",
                    )

                if state.peek(asin, in_stock):
                    detected_at = datetime.now(timezone.utc)
                    logging.info("ASIN %s restock detected; sending alerts", asin)
                    delivered = await dispatcher.send_notification(
                        product, targets, group_name=group.name, ts=detected_at
                    )
                    if delivered:
                        state.commit(asin, in_stock)
                    else:
                        logging.error(
                            "ASIN %s alert delivery failed; state not committed, "
                            "will retry on next observation",
                            asin,
                        )
                else:
                    state.commit(asin, in_stock)
            except Exception as exc:
                logging.exception("Error processing ASIN %s: %s", asin, exc)

        sleep_for = asin_backoff_seconds(config.poll_interval_seconds, failure_count)
        if failure_count:
            logging.warning(
                "ASIN %s backing off for %.2fs (failure_count=%s)",
                asin,
                sleep_for,
                failure_count,
            )
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_for)
            return
        except asyncio.TimeoutError:
            pass


async def _handle_transition(
    asin,
    asin_to_group,
    asin_to_targets,
    tvss_client,
    session,
    state,
    state_lock,
    dispatcher,
    auth_watch,
    transition_semaphore,
    config,
    auth_expired_event,
    already_alerted=False,
):
    """Full product fetch after a batch offer detection.

    When `already_alerted` is False (legacy path / MONITOR_FAST_ALERT=false):
    confirm stock + Amazon-seller, then send notification and commit.

    When `already_alerted` is True (fast-alert path): notification was already
    sent from batch data; this only enriches logs and records seller filter
    outcomes without retracting the alert.

    Spawned fire-and-forget by `poll_chunk_loop` so the batch tick rate is
    preserved. Holds `state_lock` only across commit.
    """
    delivered = False
    owns_reservation = not already_alerted
    try:
        async with transition_semaphore:
            product = await tvss_client.product(session, asin)

        in_stock = bool(product.get("in_stock"))
        third_party = (
            config.require_amazon_seller
            and in_stock
            and not product.get("soldByAmazon")
        )

        if already_alerted:
            if third_party:
                logging.info(
                    "ASIN %s: fast-alert already sent; full fetch is third-party "
                    "seller=%s (not retracting)",
                    asin,
                    product.get("seller") or "unknown",
                )
            elif not in_stock:
                logging.info(
                    "ASIN %s: fast-alert already sent; full fetch now OOS "
                    "(not retracting)",
                    asin,
                )
            else:
                logging.debug(
                    "ASIN %s: fast-alert confirmed by full fetch (seller=%s)",
                    asin,
                    product.get("seller") or "unknown",
                )
            return

        if third_party:
            logging.info(
                "ASIN %s: IN_STOCK_FILTERED (third-party seller: %s)",
                asin,
                product.get("seller") or "unknown",
            )
            async with state_lock:
                # AlertState tracks the batch buyable-offer signal. Remember
                # this offer even though seller policy suppressed the alert,
                # otherwise every batch poll schedules another full fetch.
                state.commit(asin, True)
            return

        if not in_stock:
            logging.debug("ASIN %s: OUT_OF_STOCK (full confirm)", asin)
            async with state_lock:
                state.commit(asin, False)
            return

        group = asin_to_group.get(asin)
        targets = asin_to_targets.get(asin, [])
        group_name = group.name if group else None
        detected_at = datetime.now(timezone.utc)
        product["signal"] = "offer_detected"
        logging.info("ASIN %s Amazon restock confirmed; sending alerts", asin)
        webhook_started_ns = time.perf_counter_ns()
        delivered = await dispatcher.send_notification(
            product, targets, group_name=group_name, ts=detected_at,
        )
        webhook_ack_ms = (
            time.perf_counter_ns() - webhook_started_ns
        ) / 1_000_000
        logging.info(
            "tvss_stage %s",
            json.dumps(
                {
                    "asin": asin,
                    "webhook_ack_ms": round(webhook_ack_ms, 3),
                    "seller_verified": bool(product.get("seller_verified")),
                },
                separators=(",", ":"),
            ),
        )
        if not delivered:
            logging.error(
                "ASIN %s alert delivery failed; will retry next cycle", asin
            )
    except TVSSConfigError as exc:
        auth_watch.record_auth_failure()
        logging.error("ASIN %s full fetch auth failure: %s", asin, exc)
        if auth_watch.is_tripped():
            auth_expired_event.set()
    except Exception as exc:
        logging.error("ASIN %s full fetch failed: %s", asin, exc)
    finally:
        if owns_reservation:
            async with state_lock:
                state.finish_transition(asin, delivered)


async def _fast_alert_from_batch(
    asin,
    batch_info,
    asin_to_group,
    asin_to_targets,
    tvss_client,
    session,
    state,
    state_lock,
    dispatcher,
    auth_watch,
    transition_semaphore,
    config,
    auth_expired_event,
):
    """Send alert immediately from batch data, then optionally async-confirm."""
    group = asin_to_group.get(asin)
    targets = asin_to_targets.get(asin, [])
    group_name = group.name if group else None
    product = product_from_batch(asin, batch_info, domain=tvss_client.domain)
    detected_at = datetime.now(timezone.utc)
    delivered = False
    try:
        logging.info(
            "ASIN %s unconfirmed buyable offer detected; sending fast alert",
            asin,
        )
        webhook_started_ns = time.perf_counter_ns()
        delivered = await dispatcher.send_notification(
            product, targets, group_name=group_name, ts=detected_at,
        )
        webhook_ack_ms = (
            time.perf_counter_ns() - webhook_started_ns
        ) / 1_000_000
        logging.info(
            "tvss_stage %s",
            json.dumps(
                {
                    "asin": asin,
                    "webhook_ack_ms": round(webhook_ack_ms, 3),
                    "signal": "offer_detected",
                    "seller_verified": False,
                },
                separators=(",", ":"),
            ),
        )
        if not delivered:
            logging.error(
                "ASIN %s fast-alert delivery failed; state not committed, will retry",
                asin,
            )
    finally:
        async with state_lock:
            state.finish_transition(asin, delivered)

    if delivered:
        await _handle_transition(
            asin,
            asin_to_group,
            asin_to_targets,
            tvss_client,
            session,
            state,
            state_lock,
            dispatcher,
            auth_watch,
            transition_semaphore,
            config,
            auth_expired_event,
            already_alerted=True,
        )


async def poll_chunk_loop(
    chunk_idx,
    chunk_asins,
    asin_to_group,
    asin_to_targets,
    tvss_client,
    session,
    state,
    state_lock,
    dispatcher,
    auth_watch,
    chunk_semaphore,
    transition_semaphore,
    config,
    shutdown_event,
    auth_expired_event,
):
    """Poll one batch using the client's credential-wide deadline controller."""
    transition_tasks = set()

    while not shutdown_event.is_set():
        batch_result = None
        try:
            async with chunk_semaphore:
                batch_result = await tvss_client.batch_products(session, chunk_asins)
            auth_watch.record_success()
        except TVSSConfigError as exc:
            auth_watch.record_auth_failure()
            logging.error("batch auth failure: %s", exc)
            if auth_watch.is_tripped():
                auth_expired_event.set()
                break
        except TVSSRateLimitError as exc:
            controller = tvss_client.rate_controller
            snapshot = controller.snapshot() if controller else {}
            timing = exc.timing
            logging.warning(
                "tvss_stage %s",
                json.dumps(
                    {
                        "request_wall_ms": round(exc.request_ms, 3),
                        "active_route": exc.route_id,
                        "attempts": timing.attempts,
                        "credential_queue_ms": round(
                            timing.credential_queue_ms, 3
                        ),
                        "cadence_wait_ms": round(timing.cadence_wait_ms, 3),
                        "rate_limited": True,
                        "retry_after_seconds": exc.retry_after,
                        "cooldown_seconds": round(
                            snapshot.get("blocked_seconds", 0.0), 3
                        ),
                        "interval_seconds": snapshot.get("interval_seconds"),
                    },
                    separators=(",", ":"),
                ),
            )
        except Exception as exc:
            auth_watch.record_other_failure()
            logging.error("batch poll failed: %s", exc)

        if batch_result is not None:
            evaluation_started_ns = time.perf_counter_ns()
            dispatch_scheduling_ns = 0
            unknown_count = 0
            transition_count = 0

            for asin in chunk_asins:
                try:
                    observation = batch_result[asin]
                    if observation.status is ObservationStatus.UNKNOWN:
                        unknown_count += 1
                        logging.debug("ASIN %s: UNKNOWN", asin)
                        continue

                    if observation.status is ObservationStatus.OUT_OF_STOCK:
                        async with state_lock:
                            if not state.has_inflight(asin):
                                state.commit(asin, False)
                        logging.debug("ASIN %s: OUT_OF_STOCK", asin)
                        continue

                    async with state_lock:
                        is_transition = state.reserve_transition(
                            asin, observation.status
                        )
                        if not is_transition and not state.has_inflight(asin):
                            state.commit(asin, True)

                    if not is_transition:
                        logging.debug("ASIN %s: IN_STOCK (no transition)", asin)
                        continue

                    schedule_started_ns = time.perf_counter_ns()
                    if config.fast_alert:
                        task = asyncio.create_task(
                            _fast_alert_from_batch(
                                asin,
                                observation,
                                asin_to_group,
                                asin_to_targets,
                                tvss_client,
                                session,
                                state,
                                state_lock,
                                dispatcher,
                                auth_watch,
                                transition_semaphore,
                                config,
                                auth_expired_event,
                            ),
                            name=f"fast-alert-{asin}",
                        )
                    else:
                        task = asyncio.create_task(
                            _handle_transition(
                                asin,
                                asin_to_group,
                                asin_to_targets,
                                tvss_client,
                                session,
                                state,
                                state_lock,
                                dispatcher,
                                auth_watch,
                                transition_semaphore,
                                config,
                                auth_expired_event,
                            ),
                            name=f"transition-{asin}",
                        )
                    dispatch_scheduling_ns += (
                        time.perf_counter_ns() - schedule_started_ns
                    )
                    transition_count += 1
                    transition_tasks.add(task)
                    task.add_done_callback(transition_tasks.discard)
                except Exception as exc:
                    async with state_lock:
                        state.finish_transition(asin, False)
                    logging.exception(
                        "batch error processing ASIN %s: %s", asin, exc
                    )

            evaluated_ns = time.perf_counter_ns()
            timing = batch_result.timing
            controller = tvss_client.rate_controller
            snapshot = controller.snapshot() if controller else {}
            internal_ms = (
                (evaluated_ns - timing.response_read_ns) / 1_000_000
                if timing.response_read_ns
                else 0.0
            )
            logging.info(
                "tvss_stage %s",
                json.dumps(
                    {
                        "request_wall_ms": round(timing.request_wall_ms, 3),
                        "response_read_ms": round(timing.response_read_ms, 3),
                        "json_decode_ms": round(timing.json_decode_ms, 3),
                        "state_evaluation_ms": round(
                            (evaluated_ns - evaluation_started_ns) / 1_000_000,
                            3,
                        ),
                        "dispatch_scheduling_ms": round(
                            dispatch_scheduling_ns / 1_000_000, 3
                        ),
                        "read_to_dispatch_ms": round(internal_ms, 3),
                        "active_route": timing.route_id,
                        "attempts": timing.attempts,
                        "credential_queue_ms": round(
                            timing.credential_queue_ms, 3
                        ),
                        "cadence_wait_ms": round(timing.cadence_wait_ms, 3),
                        "interval_seconds": snapshot.get("interval_seconds"),
                        "rate_limited": False,
                        "unknown_observations": unknown_count,
                        "scheduled_transitions": transition_count,
                    },
                    separators=(",", ":"),
                ),
            )

    if transition_tasks:
        await asyncio.gather(*transition_tasks, return_exceptions=True)


async def run_monitor(config, webhook_targets):
    try:
        tvss_client = TVSSClient()
    except TVSSConfigError as exc:
        logging.error("TVSS configuration error: %s", exc)
        return

    all_pairs = [(group, asin) for group in config.groups for asin in group.asins]
    total_asins = len(all_pairs)
    per_asin_concurrency = max(
        1, min(total_asins, int(os.getenv("TVSS_CONCURRENCY", "20")))
    )
    grace_seconds = float(os.getenv("AUTH_FAILURE_GRACE_SECONDS", "30"))
    use_batch = str(os.getenv("MONITOR_USE_BATCH", "true")).strip().lower() not in (
        "0", "false", "no", "off",
    )

    raw_batch_conc = os.getenv("TVSS_BATCH_CONCURRENCY")
    if raw_batch_conc is None and os.getenv("TVSS_CONCURRENCY") is not None and use_batch:
        logging.warning(
            "TVSS_CONCURRENCY is ignored in batch mode; "
            "set TVSS_BATCH_CONCURRENCY to control parallel chunks."
        )
    batch_concurrency = DEFAULT_BATCH_CONCURRENCY
    if raw_batch_conc not in (None, "1"):
        logging.warning(
            "TVSS_BATCH_CONCURRENCY=%s ignored; the credential-wide scheduler "
            "uses one request stream.",
            raw_batch_conc,
        )

    # Deduplicate ASINs across groups (batch polls each ASIN once)
    all_asins = list(dict.fromkeys(asin for _, asin in all_pairs))
    if use_batch and len(all_asins) > TVSS_MONITOR_ASIN_CAP:
        raise MonitorConfigError(
            f"batch monitor supports at most {TVSS_MONITOR_ASIN_CAP} ASINs"
        )
    tvss_client.configure_rate_controller(config.poll_interval_seconds)
    # Map ASIN → first group it belongs to (for group_name in alerts)
    asin_to_group = {}
    asin_to_targets = {}
    for group, asin in all_pairs:
        if asin not in asin_to_group:
            asin_to_group[asin] = group
            asin_to_targets[asin] = selected_webhook_targets(group, webhook_targets)

    chunks = [all_asins]

    mode = "batch" if use_batch else "per-asin"
    if use_batch:
        logging.info(
            "Starting monitor mode=batch groups=%s asins=%s chunks=%s "
            "chunk_size=%s batch_concurrency=%s poll_interval=%.2fs "
            "require_amazon_seller=%s fast_alert=%s auth_grace=%.0fs "
            "(target per-ASIN cadence %.1fs)",
            len(config.groups),
            len(all_asins),
            len(chunks),
            len(all_asins),
            batch_concurrency,
            config.poll_interval_seconds,
            config.require_amazon_seller,
            config.fast_alert,
            grace_seconds,
            config.poll_interval_seconds,
        )
    else:
        logging.info(
            "Starting monitor mode=per-asin groups=%s asins=%s "
            "concurrency=%s poll_interval=%.2fs require_amazon_seller=%s "
            "fast_alert=%s auth_grace=%.0fs",
            len(config.groups),
            len(all_asins),
            per_asin_concurrency,
            config.poll_interval_seconds,
            config.require_amazon_seller,
            config.fast_alert,
            grace_seconds,
        )

    state = AlertState()
    state_lock = asyncio.Lock()
    auth_watch = AuthFailureWatch(grace_seconds=grace_seconds)
    chunk_semaphore = asyncio.Semaphore(batch_concurrency)
    transition_semaphore = asyncio.Semaphore(TRANSITION_FETCH_CONCURRENCY)
    per_asin_semaphore = asyncio.Semaphore(per_asin_concurrency)
    shutdown_event = asyncio.Event()
    auth_expired_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def _handle_signal(received):
        logging.info(
            "Received signal %s; initiating graceful shutdown", received.name
        )
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except NotImplementedError:
            logging.debug(
                "Signal handler not supported on this platform; skipping %s",
                sig.name,
            )

    connector = aiohttp.TCPConnector(
        limit=0,
        ttl_dns_cache=300,
        keepalive_timeout=120,
        enable_cleanup_closed=True,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        dispatcher = AlertDispatcher(session)

        if use_batch:
            poll_tasks = [
                asyncio.create_task(
                    poll_chunk_loop(
                        idx,
                        chunk,
                        asin_to_group,
                        asin_to_targets,
                        tvss_client,
                        session,
                        state,
                        state_lock,
                        dispatcher,
                        auth_watch,
                        chunk_semaphore,
                        transition_semaphore,
                        config,
                        shutdown_event,
                        auth_expired_event,
                    ),
                    name=f"poll-chunk-{idx}",
                )
                for idx, chunk in enumerate(chunks)
            ]
        else:
            poll_tasks = [
                asyncio.create_task(
                    poll_asin_loop(
                        asin,
                        group,
                        tvss_client,
                        session,
                        state,
                        dispatcher,
                        selected_webhook_targets(group, webhook_targets),
                        auth_watch,
                        per_asin_semaphore,
                        config,
                        shutdown_event,
                        auth_expired_event,
                    ),
                    name=f"poll-{asin}",
                )
                for group, asin in all_pairs
            ]

        auth_waiter = asyncio.create_task(
            auth_expired_event.wait(), name="auth-waiter"
        )
        shutdown_waiter = asyncio.create_task(
            shutdown_event.wait(), name="shutdown-waiter"
        )

        try:
            await asyncio.wait(
                [auth_waiter, shutdown_waiter, *poll_tasks],
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            shutdown_event.set()

        if auth_expired_event.is_set():
            logging.error(
                "TVSS auth grace exceeded (%.0fs); sending alert and exiting",
                grace_seconds,
            )
            all_targets = list(webhook_targets.values())
            if all_targets:
                try:
                    await dispatcher.send_notification(
                        auth_expired_alert_payload(),
                        all_targets,
                        group_name="monitor",
                    )
                except Exception as exc:
                    logging.error("Failed to send auth-expiry alert: %s", exc)

        for task in [auth_waiter, shutdown_waiter, *poll_tasks]:
            if not task.done():
                task.cancel()
        await asyncio.gather(
            auth_waiter, shutdown_waiter, *poll_tasks, return_exceptions=True
        )

        if auth_expired_event.is_set():
            sys.exit(1)


async def main():
    from webhooks import WEBHOOK_TARGETS
    from durable_runtime import run_durable_monitor

    try:
        config = load_monitor_config(webhook_targets=WEBHOOK_TARGETS)
    except MonitorConfigError as exc:
        logging.error("Configuration error: %s", exc)
        return 2

    try:
        await run_durable_monitor(config, WEBHOOK_TARGETS)
    except (MonitorConfigError, TVSSConfigError) as exc:
        logging.error("Configuration error: %s", exc)
        return 2
    return 0


async def login(domain="amazon.com"):
    from amazon_auth import login_flow
    await login_flow(domain)


if __name__ == "__main__":
    args = sys.argv[1:]

    if args and args[0] == "login":
        domain = "amazon.com"
        for i, arg in enumerate(args[1:], 1):
            if arg == "--domain" and i + 1 < len(args):
                domain = args[i + 1]
            elif arg.startswith("--domain="):
                domain = arg.split("=", 1)[1]
        try:
            asyncio.run(login(domain))
        except KeyboardInterrupt:
            print()
    elif args and args[0] == "alerts":
        from durable_runtime import run_alert_admin

        action = args[1] if len(args) > 1 else "list"
        delivery_id = args[2] if len(args) > 2 else None
        try:
            raise SystemExit(
                asyncio.run(
                    run_alert_admin(action, delivery_id=delivery_id)
                )
            )
        except KeyboardInterrupt:
            logging.info("Alert administration interrupted")
    else:
        try:
            raise SystemExit(asyncio.run(main()))
        except KeyboardInterrupt:
            logging.info("Program terminated by user")
