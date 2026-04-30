import asyncio
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
import discord
from discord import Embed, Webhook
from dotenv import load_dotenv

from amazon_tvss import TVSSClient, TVSSConfigError
from webhooks import WEBHOOK_TARGETS


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

load_dotenv("endpoint.env")


ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")
DEFAULT_POLL_INTERVAL_SECONDS = 2.0
MIN_POLL_INTERVAL_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 60.0
GENERIC_WEBHOOK_TIMEOUT_SECONDS = 10.0


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
    require_amazon_seller: bool = False


class AlertState:
    """Tracks stock state and returns true only on out-of-stock to in-stock moves.

    The first observation of any ASIN is treated as priming and never alerts,
    so a process restart while an item is in stock does not fire a false-positive
    transition alert.
    """

    def __init__(self):
        self._states = {}

    def observe(self, asin, in_stock):
        in_stock = bool(in_stock)
        previously_seen = asin in self._states
        previous = self._states.get(asin, False)
        self._states[asin] = in_stock
        if not previously_seen:
            return False
        return in_stock and not previous


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

    require_amazon_seller = (
        str(env.get("MONITOR_REQUIRE_AMAZON_SELLER", "")).strip().lower()
        in ("1", "true", "yes", "on")
    )

    return MonitorConfig(
        poll_interval_seconds=parse_poll_interval(env),
        groups=groups,
        require_amazon_seller=require_amazon_seller,
    )


def build_generic_payload(product_data, group_name=None):
    price = product_data.get("price")
    offers = product_data.get("offers") or []
    if not price and offers:
        price = offers[0].get("priceInfo", {}).get("price")

    images = product_data.get("images") or []
    return {
        "asin": product_data.get("asin"),
        "title": product_data.get("title"),
        "in_stock": bool(product_data.get("in_stock")),
        "price": price,
        "link": product_data.get("link"),
        "image": images[0] if images else None,
        "seller": product_data.get("seller"),
        "source": product_data.get("source"),
        "group": group_name,
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


class AlertDispatcher:
    def __init__(self, session, clock=None):
        self.session = session
        self._clock = clock or time.monotonic
        self._target_backoff_until = {}

    def _is_target_backed_off(self, target):
        deadline = self._target_backoff_until.get(target.name)
        return deadline is not None and self._clock() < deadline

    async def send_notification(self, product_data, webhook_targets, group_name=None):
        if not webhook_targets:
            logging.error("No webhook targets selected for ASIN %s", product_data.get("asin"))
            return

        embed = self.create_embed(product_data)
        payload = build_generic_payload(product_data, group_name)

        tasks = []
        for target in webhook_targets:
            if target.kind == "generic":
                tasks.append(self._send_generic(target, payload))
            elif self._is_target_backed_off(target):
                tasks.append(self._noop_skip(target.name))
            else:
                tasks.append(self._send_discord(target, embed))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for target, result in zip(webhook_targets, results):
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

    async def _noop_skip(self, name):
        logging.warning("Skipping Discord target %s while rate limit is active", name)

    async def _send_discord(self, target, embed):
        webhook = Webhook.from_url(target.url, session=self.session)
        content = f"<@&{target.role_id}>" if target.role_id else None
        await webhook.send(content=content, embed=embed)

    async def _send_generic(self, target, payload):
        async with self.session.post(
            target.url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=GENERIC_WEBHOOK_TIMEOUT_SECONDS),
        ) as response:
            if response.status < 200 or response.status >= 300:
                body = await response.text()
                raise RuntimeError(
                    f"generic webhook {target.name} HTTP {response.status}: {body[:200]}"
                )

    def create_embed(self, product_data):
        embed = Embed(title="Amazon Stock Monitor", color=discord.Color.purple())
        price = product_data.get("price") or "MSRP"

        if product_data.get("offers"):
            price_info = product_data["offers"][0].get("priceInfo", {})
            price = price_info.get("price") or price

        if product_data.get("images"):
            embed.set_thumbnail(url=product_data["images"][0])

        product_link = product_data.get("link", "")
        product_name = product_data.get("title", "N/A")
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
                f"**Sold By:** {product_data.get('seller') or 'Amazon.com'}\n"
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

        embed.set_footer(text=f"Amazon Stock Monitor | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
            "primaryMessage": "Refresh TVSS_COOKIE_HEADER (and TVSS_ACCESS_TOKEN if set) and redeploy.",
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
                    logging.info(
                        "Group %s ASIN %s: IN_STOCK_FILTERED (third-party seller)",
                        group.name,
                        asin,
                    )
                else:
                    logging.info(
                        "Group %s ASIN %s: %s",
                        group.name,
                        asin,
                        "IN_STOCK" if in_stock else "OUT_OF_STOCK",
                    )

                if state.observe(asin, in_stock):
                    logging.info("ASIN %s restock detected; sending alerts", asin)
                    await dispatcher.send_notification(
                        product, targets, group_name=group.name
                    )
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


async def run_monitor(config, webhook_targets):
    try:
        tvss_client = TVSSClient()
    except TVSSConfigError as exc:
        logging.error("TVSS configuration error: %s", exc)
        return

    all_pairs = [(group, asin) for group in config.groups for asin in group.asins]
    total_asins = len(all_pairs)
    concurrency = max(
        1, min(total_asins, int(os.getenv("TVSS_CONCURRENCY", "5")))
    )
    grace_seconds = float(os.getenv("AUTH_FAILURE_GRACE_SECONDS", "30"))

    logging.info(
        "Starting monitor groups=%s asins=%s poll_interval=%.2fs concurrency=%s "
        "require_amazon_seller=%s auth_grace=%.0fs",
        len(config.groups),
        total_asins,
        config.poll_interval_seconds,
        concurrency,
        config.require_amazon_seller,
        grace_seconds,
    )

    state = AlertState()
    auth_watch = AuthFailureWatch(grace_seconds=grace_seconds)
    semaphore = asyncio.Semaphore(concurrency)
    shutdown_event = asyncio.Event()
    auth_expired_event = asyncio.Event()

    async with aiohttp.ClientSession() as session:
        dispatcher = AlertDispatcher(session)

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
                    semaphore,
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

        try:
            await asyncio.wait(
                [auth_waiter, *poll_tasks],
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

        for task in [auth_waiter, *poll_tasks]:
            if not task.done():
                task.cancel()
        await asyncio.gather(auth_waiter, *poll_tasks, return_exceptions=True)

        if auth_expired_event.is_set():
            sys.exit(1)


async def main():
    try:
        config = load_monitor_config(webhook_targets=WEBHOOK_TARGETS)
    except MonitorConfigError as exc:
        logging.error("Configuration error: %s", exc)
        return

    await run_monitor(config, WEBHOOK_TARGETS)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Program terminated by user")
