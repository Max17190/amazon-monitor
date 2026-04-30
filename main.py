import asyncio
import json
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime

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


class MonitorConfigError(ValueError):
    pass


@dataclass(frozen=True)
class MonitorGroup:
    name: str
    asins: list
    webhook_names: list


@dataclass(frozen=True)
class MonitorConfig:
    backend: str
    poll_interval_seconds: float
    groups: list


class AlertState:
    """Tracks stock state and returns true only on out-of-stock to in-stock moves."""

    def __init__(self):
        self._states = {}

    def observe(self, asin, in_stock):
        previous = self._states.get(asin, False)
        self._states[asin] = bool(in_stock)
        return bool(in_stock) and not previous


def get_random_user_agent():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 5.1; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 5.1; rv:78.0) Gecko/20100101 Firefox/78.0 Mypal/68.14.5",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 5.1; rv:102.0) Gecko/20100101 Goanna/4.0 Firefox/102.0 Basilisk/20231124",
        "Mozilla/5.0 (Windows NT 5.1; rv:88.0) Gecko/20100101 Firefox/88.0",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
        "Mozilla/5.0 (Windows NT 5.1; rv:6.7) Goanna/6.7 PaleMoon/33.2",
        "Mozilla/5.0 (Windows NT 5.1; rv:68.9.0) Gecko/20100101 Goanna/4.8 Firefox/68.9.0 Basilisk/52.9.0",
    ]
    return random.choice(user_agents)


def build_proxy_url(env=None):
    env = os.environ if env is None else env
    host = env.get("PROXY_HOST")
    port = env.get("PROXY_PORT")
    user = env.get("PROXY_USER")
    password = env.get("PROXY_PASS")

    values = [host, port, user, password]
    if not any(values):
        return None
    if not all(values):
        logging.warning("Incomplete proxy configuration; proxy will be disabled")
        return None

    return f"http://{user}:{password}@{host}:{port}"


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

    backend = env.get("AMAZON_MONITOR_BACKEND", "tvss").strip().lower()
    if backend not in ("tvss", "ajax"):
        raise MonitorConfigError("AMAZON_MONITOR_BACKEND must be 'tvss' or 'ajax'")

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

    return MonitorConfig(
        backend=backend,
        poll_interval_seconds=parse_poll_interval(env),
        groups=groups,
    )


class BlinkMonitor:
    def __init__(self, session):
        self.session = session
        self.rate_limited = False

    async def send_notification(self, product_data, webhook_targets):
        if self.rate_limited:
            logging.warning("Skipping webhook sends while Discord rate limit is active")
            return

        if not webhook_targets:
            logging.error("No webhook targets selected for ASIN %s", product_data.get("asin"))
            return

        embed = self.create_embed(product_data)
        tasks = [self._send_to_target(target, embed) for target in webhook_targets]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for target, result in zip(webhook_targets, results):
            if isinstance(result, discord.HTTPException) and result.status == 429:
                retry_after = getattr(result, "retry_after", 1)
                logging.warning(
                    "Discord rate limited target %s; backing off for %.2fs",
                    target.name,
                    retry_after,
                )
                self.rate_limited = True
                await asyncio.sleep(retry_after + 1)
                self.rate_limited = False
            elif isinstance(result, Exception):
                logging.error("Webhook send failed for %s: %s", target.name, result)

    async def _send_to_target(self, target, embed):
        webhook = Webhook.from_url(target.url, session=self.session)
        content = f"<@&{target.role_id}>" if target.role_id else None
        await webhook.send(content=content, embed=embed)

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

        embed.set_footer(text=f"Blink FNF | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return embed


def parse_json(response_data):
    """Parse the legacy Amazon Ajax response into the monitor product shape."""
    parsed_list = []
    try:
        products = response_data if isinstance(response_data, list) else response_data.get("products", [])

        for product in products:
            title = product.get("title", "N/A")
            if isinstance(title, dict):
                title = title.get("displayString", "N/A")

            price = None
            offers = product.get("offers") or []
            if offers:
                price = offers[0].get("priceInfo", {}).get("price")

            item = {
                "asin": product.get("asin", "N/A"),
                "title": title,
                "in_stock": bool(product.get("canAddToCart", False)),
                "link": f"https://www.amazon.com/dp/{product.get('asin', '')}",
                "images": [
                    img.get("hiRes", {}).get("url")
                    for img in product.get("productImages", {}).get("images", [])
                    if img and img.get("hiRes", {}).get("url")
                ],
                "price": price,
                "seller": product.get("seller") or "Amazon.com",
                "availability": product.get("availability") or {},
                "source": "ajax",
            }
            parsed_list.append(item)

        return parsed_list

    except Exception as exc:
        logging.error("Parsing error: %s", exc)
        return []


async def get_slate_token(session, proxy_url):
    try:
        async with session.get(
            os.getenv("AMAZON_URL"),
            headers={"User-Agent": get_random_user_agent()},
            proxy=proxy_url,
            timeout=5,
        ) as response:
            response.raise_for_status()
            text = await response.text()
            match = re.search(r'"slateToken"\s*:\s*"([^"]+)"', text)
            return match.group(1) if match else None
    except Exception as exc:
        logging.error("Failed to get slate token: %s", exc)
        return None


async def check_stock_ajax(session, asins, proxy_url):
    """Check stock with the legacy Ajax endpoint in batches of at most 25 ASINs."""
    results = []
    for start in range(0, len(asins), 25):
        batch = asins[start : start + 25]
        session_id = (
            f"{random.randint(100, 999)}-"
            f"{random.randint(10 ** 6, 10 ** 7 - 1)}-"
            f"{random.randint(10 ** 6, 10 ** 7 - 1)}"
        )
        data = {
            "requestContext": {
                "obfuscatedMarketplaceId": os.getenv("MARKETPLACE_ID"),
                "obfuscatedMerchantId": os.getenv("MERCHANT_ID"),
                "language": "en-US",
                "sessionId": session_id,
                "currency": "USD",
                "amazonApiAjaxEndpoint": "data.amazon.com",
                "slateToken": await get_slate_token(session, proxy_url),
            },
            "content": {"includeOutOfStock": False},
            "includeOutOfStock": True,
            "endpoint": "ajax-data",
            "ASINList": batch,
        }
        headers = {
            "User-Agent": get_random_user_agent(),
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": "https://www.amazon.com",
            "Referer": "https://www.amazon.com/",
        }

        try:
            async with session.post(
                os.getenv("AMAZON_ENDPOINT"),
                headers=headers,
                json=data,
                proxy=proxy_url,
                timeout=5,
            ) as response:
                response.raise_for_status()
                response_data = await response.json()
                results.extend(parse_json(response_data))
        except aiohttp.ClientError as exc:
            logging.error("Ajax request failed for ASIN batch %s: %s", batch, exc)
        except json.JSONDecodeError:
            logging.error("Failed to parse Ajax JSON response for ASIN batch %s", batch)
        except Exception as exc:
            logging.error("Unexpected Ajax stock check error for batch %s: %s", batch, exc)

    return results


async def poll_group(session, config, group, tvss_client, proxy_url):
    if config.backend == "tvss":
        return await tvss_client.products(session, group.asins)
    return await check_stock_ajax(session, group.asins, proxy_url)


def selected_webhook_targets(group, webhook_targets):
    return [webhook_targets[name] for name in group.webhook_names]


def cycle_sleep_seconds(config, failure_count):
    if failure_count <= 0:
        return config.poll_interval_seconds
    return min(
        max(config.poll_interval_seconds, 1.0) * (2 ** min(failure_count - 1, 5)),
        MAX_BACKOFF_SECONDS,
    )


async def run_monitor(config, webhook_targets):
    tvss_client = None
    proxy_url = build_proxy_url()

    if config.backend == "tvss":
        try:
            tvss_client = TVSSClient()
        except TVSSConfigError as exc:
            logging.error("TVSS configuration error: %s", exc)
            return

    logging.info(
        "Starting monitor backend=%s groups=%s asins=%s poll_interval=%.2fs",
        config.backend,
        len(config.groups),
        sum(len(group.asins) for group in config.groups),
        config.poll_interval_seconds,
    )

    state = AlertState()
    cycle_number = 0
    failure_count = 0

    async with aiohttp.ClientSession() as session:
        monitor = BlinkMonitor(session)
        while True:
            cycle_number += 1
            cycle_failed = False
            logging.info("Poll cycle %s started", cycle_number)

            for group in config.groups:
                try:
                    products = await poll_group(session, config, group, tvss_client, proxy_url)
                except TVSSConfigError as exc:
                    cycle_failed = True
                    logging.error("TVSS authentication/configuration error: %s", exc)
                    continue
                except Exception as exc:
                    cycle_failed = True
                    logging.error("Poll failed for group %s: %s", group.name, exc)
                    continue

                if not products:
                    cycle_failed = True
                    logging.warning("Group %s returned no products", group.name)
                    continue

                products_by_asin = {}
                error_count = 0
                for product in products:
                    asin = product.get("asin", "UNKNOWN_ASIN")
                    products_by_asin[asin] = product
                    if product.get("error"):
                        error_count += 1
                        logging.error(
                            "ASIN %s check failed via %s: %s",
                            asin,
                            product.get("source", config.backend),
                            product.get("error"),
                        )
                        continue

                    in_stock = bool(product.get("in_stock"))
                    logging.info(
                        "Group %s ASIN %s: %s",
                        group.name,
                        asin,
                        "IN_STOCK" if in_stock else "OUT_OF_STOCK",
                    )

                    if state.observe(asin, in_stock):
                        logging.info("ASIN %s restock detected; sending alerts", asin)
                        await monitor.send_notification(
                            product,
                            selected_webhook_targets(group, webhook_targets),
                        )

                missing_asins = set(group.asins) - set(products_by_asin.keys())
                for asin in sorted(missing_asins):
                    logging.error("Group %s ASIN %s: MISSING_FROM_RESPONSE", group.name, asin)

                if error_count == len(products):
                    cycle_failed = True

                logging.info(
                    "Group %s complete: %s/%s ASINs returned",
                    group.name,
                    len(products_by_asin),
                    len(group.asins),
                )

            failure_count = failure_count + 1 if cycle_failed else 0
            sleep_seconds = cycle_sleep_seconds(config, failure_count)
            if failure_count:
                logging.warning(
                    "Poll cycle %s completed with errors; backing off for %.2fs",
                    cycle_number,
                    sleep_seconds,
                )
            await asyncio.sleep(sleep_seconds)


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
