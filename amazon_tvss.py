import asyncio
import contextvars
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from email.utils import parsedate_to_datetime
from urllib.parse import urlencode

import aiohttp

from credential_governor import RequestClass
from tvss_runtime import CredentialRateController, DIRECT_ROUTE_ID, ProxyPool


TVSS_BASE_URL = "https://tvss.amazon.com"
TVSS_USER_AGENT = (
    "AMZN(SetTopBox/Amazon Fire TV Mantis/AKPGW064GI9HE,"
    "Android/7.1.2,ShopTV3P/release/2.0)"
)
DEFAULT_MARKETPLACE_ID = "ATVPDKIKX0DER"
DEFAULT_DOMAIN = "amazon.com"


class TVSSConfigError(RuntimeError):
    pass


class ObservationStatus(str, Enum):
    IN_STOCK = "IN_STOCK"
    OUT_OF_STOCK = "OUT_OF_STOCK"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class BatchObservation:
    asin: str
    status: ObservationStatus
    price: object = None
    availability_condition: object = None
    offer_id: object = None
    response_complete: bool = False
    offer_explicitly_null: object = None

    @property
    def has_offer(self):
        return self.status is ObservationStatus.IN_STOCK

    def get(self, key, default=None):
        if key == "asin":
            return self.asin
        if key == "status":
            return self.status
        if key == "has_offer":
            return self.has_offer
        if key == "price":
            return self.price
        if key == "availability_condition":
            return self.availability_condition
        if key == "offer_id":
            return self.offer_id
        if key == "response_complete":
            return self.response_complete
        if key == "offer_explicitly_null":
            return self.offer_explicitly_null
        return default

    def __getitem__(self, key):
        value = self.get(key, self)
        if value is self:
            raise KeyError(key)
        return value


@dataclass
class TVSSRequestTiming:
    request_started_ns: int = 0
    response_headers_ns: int = 0
    response_read_ns: int = 0
    json_decoded_ns: int = 0
    route_id: str = DIRECT_ROUTE_ID
    attempts: int = 0
    credential_queue_ms: float = 0.0
    cadence_wait_ms: float = 0.0
    half_open_probe: bool = False
    confirmation_slot_borrowed: bool = False
    status: int = 0

    @property
    def request_wall_ms(self):
        end = self.response_read_ns or self.response_headers_ns
        if not self.request_started_ns or not end:
            return 0.0
        return (end - self.request_started_ns) / 1_000_000

    @property
    def response_read_ms(self):
        if not self.response_headers_ns or not self.response_read_ns:
            return 0.0
        return (self.response_read_ns - self.response_headers_ns) / 1_000_000

    @property
    def json_decode_ms(self):
        if not self.response_read_ns or not self.json_decoded_ns:
            return 0.0
        return (self.json_decoded_ns - self.response_read_ns) / 1_000_000


class BatchPollResult(dict):
    def __init__(self, observations, timing=None, top_level_errors=False):
        super().__init__(observations)
        self.observations = observations
        self.timing = timing or TVSSRequestTiming()
        self.top_level_errors = bool(top_level_errors)


class TVSSRateLimitError(RuntimeError):
    def __init__(
        self,
        retry_after=None,
        route_id=DIRECT_ROUTE_ID,
        request_ms=0.0,
        timing=None,
    ):
        super().__init__("TVSS rate limited with HTTP 429")
        self.retry_after = retry_after
        self.route_id = route_id
        self.request_ms = float(request_ms)
        self.timing = timing or TVSSRequestTiming()


class TVSSClient:
    """Minimal Amazon TVSS client for ASIN inventory monitoring.

    All per-request overhead is eliminated at construction time:
    headers, URL prefix, and timeout are pre-built once.  The only
    per-request work is a dict copy + one key assignment (request ID)
    and a single f-string concatenation for the URL.
    """

    def __init__(self):
        self.marketplace_id = os.getenv("TVSS_MARKETPLACE_ID", DEFAULT_MARKETPLACE_ID)
        self.domain = os.getenv("TVSS_DOMAIN", DEFAULT_DOMAIN)
        self.base_url = os.getenv("TVSS_BASE_URL", TVSS_BASE_URL).rstrip("/")
        self.currency = os.getenv("TVSS_CURRENCY", "USD")
        configured_udid = os.getenv("TVSS_DEVICE_UDID")
        auth_udid = self._load_device_udid_from_auth_state()
        self.device_udid = configured_udid or auth_udid or os.urandom(16).hex()
        self.has_stable_device_identity = bool(configured_udid or auth_udid)
        self.cookie_header = self._load_cookie_header()

        # Fallback: if no env-var cookies, try loading from auth.json
        # (written by `python main.py login`).
        if not self.cookie_header:
            self.cookie_header = self._load_cookie_header_from_auth_state()

        self.access_token = os.getenv("TVSS_ACCESS_TOKEN") or self._cookie_value("at-main")

        if not self.cookie_header:
            raise TVSSConfigError(
                "No TVSS credentials found. Run 'python main.py login' to authenticate, "
                "or set TVSS_COOKIE_HEADER / TVSS_COOKIES_JSON in endpoint.env"
            )

        # Pre-build the frozen portion of request headers.  Per-request,
        # only x-amzn-RequestId is swapped via a shallow copy + one write.
        self._base_headers = {
            "Cookie": self.cookie_header,
            "User-Agent": TVSS_USER_AGENT,
            "x-amz-msh-appid": (
                "name=ShopTV3P;ver=2000610;device=AFTMM;"
                f"os=Android_7.1.2;UDID={self.device_udid};tag=mshop-amazon-us-20"
            ),
        }
        if self.access_token:
            self._base_headers["x-amz-access-token"] = self.access_token

        # Pre-compute URL prefixes so the hot path is a single f-string.
        self._product_url_prefix = (
            f"{self.base_url}/marketplaces/{self.marketplace_id}/products/"
        )
        self._basicproducts_url_prefix = (
            f"{self.base_url}/marketplaces/{self.marketplace_id}/basicproducts/"
        )

        # Parse timeout once.  Split into connect vs read for faster
        # failure detection: a hung TCP connect is caught in 2 s instead
        # of waiting for the full timeout, freeing the semaphore slot
        # for other ASINs.
        total = float(os.getenv("TVSS_TIMEOUT", "5"))
        self._timeout = aiohttp.ClientTimeout(
            total=total,
            sock_connect=min(total, 2.0),
            sock_read=total,
        )

        try:
            self.proxy_pool = ProxyPool.from_env()
        except (TypeError, ValueError) as exc:
            raise TVSSConfigError(f"invalid proxy configuration: {exc}") from exc
        self.rate_controller = None
        self.durable_governor = None
        self.credential_key = None
        self.last_request_timing = TVSSRequestTiming()
        self._traffic_lock = None
        self._request_timing = contextvars.ContextVar(
            f"tvss_request_timing_{id(self)}",
            default=None,
        )
        self._request_class = contextvars.ContextVar(
            f"tvss_request_class_{id(self)}",
            default=RequestClass.CONFIRM,
        )

    @property
    def proxy(self):
        """Compatibility surface used by the one-shot latency probe."""
        return self.proxy_pool.primary_route.url

    def configure_rate_controller(self, interval_seconds):
        self.rate_controller = CredentialRateController(
            interval_seconds,
            max_interval=float(os.getenv("TVSS_MAX_INTERVAL_SECONDS", "120")),
            cooldown_seconds=float(os.getenv("TVSS_429_COOLDOWN_SECONDS", "90")),
            success_window=int(os.getenv("TVSS_RECOVERY_SUCCESS_COUNT", "30")),
            additive_decrease=float(os.getenv("TVSS_INTERVAL_DECREMENT", "0.05")),
        )
        return self.rate_controller

    def configure_durable_governor(self, governor, credential_key, owner_id=None):
        if not credential_key:
            raise TVSSConfigError("credential_key is required for durable governor")
        self.durable_governor = governor
        self.credential_key = credential_key
        # Durable callers must provide the leader identity.  It is checked
        # immediately before each physical request so a stale replica cannot
        # spend a permit after a lease takeover.
        self.credential_owner_id = owner_id
        self.rate_controller = None
        return governor

    def enable_proxy_fallback(self):
        """Select the best healthy proxy for the next half-open recovery probe."""
        route_id = self.proxy_pool.activate_recovery()
        if route_id:
            logging.warning("TVSS proxy recovery armed route=%s", route_id)
        return route_id

    def disable_proxy_fallback(self):
        """Return fallback mode to direct egress."""
        self.proxy_pool.deactivate_recovery()
        logging.info("TVSS proxy recovery cleared route=direct")

    def _load_cookie_header(self):
        cookie_header = os.getenv("TVSS_COOKIE_HEADER") or os.getenv("AMAZON_COOKIE_HEADER")
        if cookie_header:
            return cookie_header.strip()

        cookies_json = os.getenv("TVSS_COOKIES_JSON")
        if not cookies_json:
            return ""

        try:
            cookies = json.loads(cookies_json)
        except json.JSONDecodeError as exc:
            raise TVSSConfigError(f"invalid TVSS_COOKIES_JSON: {exc}") from exc

        parts = []
        for cookie in cookies:
            name = cookie.get("name")
            value = str(cookie.get("value", "")).replace('"', "")
            if name and value:
                parts.append(f"{name}={value}")
        return "; ".join(parts)

    def _load_cookie_header_from_auth_state(self):
        from amazon_auth import load_auth_state, cookies_to_header
        state = load_auth_state()
        if state and state.get("state") == "authenticated" and state.get("cookies"):
            return cookies_to_header(state["cookies"])
        return ""

    @staticmethod
    def _load_device_udid_from_auth_state():
        from amazon_auth import load_auth_state

        state = load_auth_state()
        device = state.get("device") if isinstance(state, dict) else None
        if not isinstance(device, dict):
            return ""
        return str(device.get("device_serial") or "").strip()

    def _cookie_value(self, name):
        if not self.cookie_header:
            return ""

        for part in self.cookie_header.split(";"):
            key, _, value = part.strip().partition("=")
            if key == name:
                return value
        return ""

    def _tvss_url(self, *segments, **params):
        """General-purpose URL builder.  Not used on the product hot path."""
        path = "/".join(
            [self.base_url, "marketplaces", self.marketplace_id]
            + [str(segment).strip("/") for segment in segments]
        )
        query = {"sif_profile": "tvss"}
        query.update({k: v for k, v in params.items() if v is not None})
        return f"{path}?{urlencode(query)}"

    def _headers(self):
        """Return request headers.  Shallow-copies the pre-built base and
        stamps a fresh request ID — the only per-request work."""
        headers = self._base_headers.copy()
        headers["x-amzn-RequestId"] = os.urandom(10).hex().upper()
        return headers

    @staticmethod
    def _retry_after_seconds(raw_value):
        if not raw_value:
            return None
        try:
            return max(0.0, float(raw_value))
        except (TypeError, ValueError):
            try:
                retry_at = parsedate_to_datetime(str(raw_value))
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                return max(
                    0.0,
                    (retry_at - datetime.now(timezone.utc)).total_seconds(),
                )
            except (TypeError, ValueError, OverflowError):
                return None

    async def _request(
        self,
        session,
        method,
        url,
        json_body=None,
        request_class=None,
        borrow_confirmation_slot=False,
    ):
        request_class = request_class or self._request_class.get()
        if self._traffic_lock is None:
            self._traffic_lock = asyncio.Lock()
        queued_ns = time.perf_counter_ns()
        async with self._traffic_lock:
            queue_ms = (time.perf_counter_ns() - queued_ns) / 1_000_000
            return await self._request_serialized(
                session,
                method,
                url,
                json_body=json_body,
                credential_queue_ms=queue_ms,
                request_class=request_class,
                borrow_confirmation_slot=borrow_confirmation_slot,
            )

    async def _request_serialized(
        self,
        session,
        method,
        url,
        json_body=None,
        credential_queue_ms=0.0,
        request_class=RequestClass.CONFIRM,
        borrow_confirmation_slot=False,
    ):
        timing = TVSSRequestTiming()
        timing.credential_queue_ms = credential_queue_ms
        if self.rate_controller is not None and self.durable_governor is None:
            _, wait_seconds, half_open = await self.rate_controller.acquire()
            timing.cadence_wait_ms = wait_seconds * 1000.0
            timing.half_open_probe = half_open

        timing.request_started_ns = time.perf_counter_ns()
        routes = self.proxy_pool.request_routes()
        last_network_error = None

        for attempt, route in enumerate(routes, start=1):
            permit = None
            if self.durable_governor is not None:
                if (
                    borrow_confirmation_slot
                    and request_class is RequestClass.CONFIRM
                ):
                    permit = (
                        await self.durable_governor
                        .acquire_borrowed_confirmation_permit(
                            self.credential_key,
                            owner_id=self.credential_owner_id,
                        )
                    )
                else:
                    permit = await self.durable_governor.acquire_permit(
                        self.credential_key,
                        request_class,
                        owner_id=self.credential_owner_id,
                    )
                if permit.wait_seconds:
                    await asyncio.sleep(permit.wait_seconds)
                # A permit can wait for a full cadence slot. Revalidate after
                # that wait at the network boundary, not only when reserving.
                if self.credential_owner_id is not None:
                    await self.durable_governor.ensure_leader(
                        self.credential_key,
                        self.credential_owner_id,
                    )
                timing.cadence_wait_ms += permit.wait_seconds * 1000.0
                timing.half_open_probe = permit.half_open_probe
                timing.confirmation_slot_borrowed = permit.borrowed
            attempt_started_ns = time.perf_counter_ns()
            timing.attempts = attempt
            timing.route_id = route.route_id
            try:
                async with session.request(
                    method,
                    url,
                    headers=self._headers(),
                    json=json_body,
                    timeout=self._timeout,
                    proxy=route.url,
                ) as response:
                    timing.response_headers_ns = time.perf_counter_ns()
                    timing.status = response.status
                    body = await response.read()
                    timing.response_read_ns = time.perf_counter_ns()
                    attempt_ms = (
                        timing.response_read_ns - attempt_started_ns
                    ) / 1_000_000

                    if response.status in (401, 403):
                        if permit is not None:
                            await self.durable_governor.record_result(
                                permit, response.status
                            )
                        self.last_request_timing = timing
                        raise TVSSConfigError(
                            f"TVSS auth rejected with HTTP {response.status}"
                        )

                    if response.status == 429:
                        retry_after = self._retry_after_seconds(
                            response.headers.get("Retry-After")
                        )
                        if (
                            not route.is_direct
                            and self.durable_governor is None
                        ):
                            self.proxy_pool.record_failure(
                                route.route_id,
                                quarantine_seconds=max(retry_after or 0.0, 90.0),
                            )
                        if permit is not None:
                            await self.durable_governor.record_result(
                                permit,
                                response.status,
                                retry_after,
                            )
                        elif self.rate_controller is not None:
                            self.rate_controller.record_rate_limit(retry_after)
                        if self.durable_governor is None:
                            self.enable_proxy_fallback()
                        self.last_request_timing = timing
                        self._request_timing.set(timing)
                        raise TVSSRateLimitError(
                            retry_after=retry_after,
                            route_id=route.route_id,
                            request_ms=timing.request_wall_ms,
                            timing=timing,
                        )

                    if response.status < 200 or response.status >= 300:
                        if permit is not None:
                            await self.durable_governor.record_result(
                                permit, response.status
                            )
                        self.last_request_timing = timing
                        raise RuntimeError(
                            f"TVSS HTTP {response.status} (body_len={len(body)})"
                        )

                    self.proxy_pool.record_success(route.route_id, attempt_ms)
                    if permit is not None:
                        await self.durable_governor.record_result(
                            permit, response.status
                        )
                    elif self.rate_controller is not None:
                        self.rate_controller.record_success()

                    if not body:
                        timing.json_decoded_ns = timing.response_read_ns
                        self.last_request_timing = timing
                        self._request_timing.set(timing)
                        return None

                    self.last_request_timing = timing
                    self._request_timing.set(timing)
                    data = json.loads(body)
                    timing.json_decoded_ns = time.perf_counter_ns()
                    self.last_request_timing = timing
                    self._request_timing.set(timing)
                    return data.get("entity", data) if isinstance(data, dict) else data
            except TVSSRateLimitError:
                raise
            except TVSSConfigError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_network_error = exc
                if permit is not None:
                    await self.durable_governor.record_result(permit, None)
                self.proxy_pool.record_failure(route.route_id)
                logging.warning(
                    "TVSS network failure route=%s attempt=%s",
                    route.route_id,
                    attempt,
                )
                if attempt >= len(routes):
                    self.last_request_timing = timing
                    raise RuntimeError(
                        f"TVSS network failure after {attempt} route attempt(s)"
                    ) from exc

        if last_network_error is not None:
            raise last_network_error
        raise RuntimeError("TVSS request had no available route")

    async def product(self, session, asin, *, borrow_confirmation_slot=False):
        url = f"{self._product_url_prefix}{asin}?sif_profile=tvss"
        token = self._request_class.set(RequestClass.CONFIRM)
        try:
            data = await self._request(
                session,
                "GET",
                url,
                borrow_confirmation_slot=borrow_confirmation_slot,
            )
        finally:
            self._request_class.reset(token)
        return self._parse_product(data, asin)

    async def batch_products(self, session, asins):
        """Fetch basic product data for up to 50 ASINs in one request.

        Returns typed observations with IN_STOCK, OUT_OF_STOCK, or UNKNOWN.
        The batch endpoint does NOT include merchantInfo (no soldByAmazon);
        callers should issue a full product() call when a transition is detected.

        Amazon hard-rejects 51+ ASINs with HTTP 400 (empty body) — the cap
        was confirmed empirically by bisecting from 50 upward; 51 fails in
        ~100 ms at the edge layer with no body.
        """
        if len(asins) > 50:
            raise ValueError(
                f"batch_products called with {len(asins)} ASINs; "
                "Amazon TVSS basicproducts hard-caps at 50 (51+ returns HTTP 400)"
            )
        joined = ",".join(asins)
        url = f"{self._basicproducts_url_prefix}{joined}?get-deals=false&sif_profile=tvss"
        token = self._request_class.set(RequestClass.POLL)
        try:
            data = await self._request(session, "GET", url)
        finally:
            self._request_class.reset(token)
        result = self.parse_batch_response(data, asins)
        result.timing = self._request_timing.get() or self.last_request_timing
        return result

    @staticmethod
    def parse_batch_response(data, asins):
        requested = tuple(asins)
        observations = {
            asin: BatchObservation(asin=asin, status=ObservationStatus.UNKNOWN)
            for asin in requested
        }

        top_level_errors = bool(
            isinstance(data, dict) and (data.get("errors") or data.get("error"))
        )
        if top_level_errors:
            return BatchPollResult(
                observations,
                top_level_errors=True,
            )

        products = []
        if isinstance(data, dict):
            products = data.get("products", [])
        elif isinstance(data, list):
            products = data

        if not isinstance(products, list):
            return BatchPollResult(observations)

        for item in products:
            if not isinstance(item, dict):
                continue
            bp = item.get("basicProduct")
            if not isinstance(bp, dict):
                continue
            asin = bp.get("asin")
            if asin not in observations:
                continue
            if "basicOffer" not in item:
                continue
            basic_offer = item["basicOffer"]
            availability = bp.get("availabilityCondition")
            if basic_offer is None:
                observations[asin] = BatchObservation(
                    asin=asin,
                    status=ObservationStatus.OUT_OF_STOCK,
                    availability_condition=availability,
                    offer_id=None,
                    response_complete=bool(availability),
                    offer_explicitly_null=True,
                )
                continue
            if not isinstance(basic_offer, dict):
                continue
            if "offerId" not in basic_offer:
                continue

            status = (
                ObservationStatus.IN_STOCK
                if bool(basic_offer.get("offerId"))
                else ObservationStatus.OUT_OF_STOCK
            )
            observations[asin] = BatchObservation(
                asin=asin,
                status=status,
                price=basic_offer.get("price"),
                availability_condition=availability,
                offer_id=basic_offer.get("offerId"),
                response_complete=True,
                offer_explicitly_null=False,
            )

        return BatchPollResult(observations)

    @classmethod
    def decode_batch_response(cls, body, asins):
        """Decode response bytes and classify observations for hot-path benchmarks."""
        data = json.loads(body)
        if isinstance(data, dict):
            data = data.get("entity", data)
        return cls.parse_batch_response(data, asins)

    def _parse_product(self, data, fallback_asin):
        response_complete = isinstance(data, dict) and bool(data)
        if not isinstance(data, dict):
            data = {}

        returned_asin = data.get("asin")
        if returned_asin != fallback_asin:
            response_complete = False
        asin = fallback_asin
        raw_availability = data.get("productAvailabilityDetails")
        availability = raw_availability or {}
        if raw_availability is not None and not isinstance(
            raw_availability,
            dict,
        ):
            availability = {}
            response_complete = False

        raw_offer_id = (
            data.get("offerId")
            or data.get("offerListingId")
            or data.get("buyingOptionId")
            or ""
        )
        if raw_offer_id and not isinstance(raw_offer_id, str):
            response_complete = False
            offer_id = ""
        else:
            offer_id = raw_offer_id
        price = self._format_price(data.get("price"))
        raw_merchant = data.get("merchantInfo")
        merchant = raw_merchant or {}
        if raw_merchant is not None and not isinstance(raw_merchant, dict):
            merchant = {}
            response_complete = False
        images = self._image_urls(data.get("productImageUrls"))
        buyable = self._has_buyable_signal(data, availability, offer_id)

        return {
            "asin": asin,
            "title": data.get("title") or "N/A",
            "in_stock": self._is_in_stock(availability, buyable),
            "link": f"https://www.{self.domain}/dp/{asin}",
            "images": images,
            "price": price,
            "source": "tvss",
            "offerId": offer_id,
            "availability": availability,
            "seller": merchant.get("merchantName"),
            "seller_id": merchant.get("merchantId"),
            "soldByAmazon": merchant.get("soldByAmazon"),
            "seller_verified": bool(merchant.get("merchantName")),
            "response_complete": response_complete,
            "buyable_signals": tuple(
                key
                for key in ("canAddToCart", "isBuyable", "buyable", "available")
                if data.get(key) is True or availability.get(key) is True
            ),
        }

    @staticmethod
    def _image_urls(raw_images):
        image_urls = []
        if not isinstance(raw_images, list):
            return image_urls

        for image in raw_images:
            if isinstance(image, str) and image:
                image_urls.append(image)
            elif isinstance(image, dict):
                url = image.get("url") or image.get("hiRes") or image.get("large")
                if isinstance(url, dict):
                    url = url.get("url")
                if url:
                    image_urls.append(url)

        return image_urls

    @staticmethod
    def _has_buyable_signal(data, availability, offer_id):
        if offer_id:
            return True

        for key in ("canAddToCart", "isBuyable", "buyable", "available"):
            if data.get(key) is True or availability.get(key) is True:
                return True

        return False

    @staticmethod
    def _is_in_stock(availability, buyable):
        status_text = " ".join(
            str(availability.get(key, ""))
            for key in (
                "availabilityCondition",
                "status",
                "primaryMessage",
                "secondaryMessage",
            )
        ).lower()
        out_patterns = (
            "out_of_stock",
            "out of stock",
            "unavailable",
            "currently unavailable",
            "not available",
        )
        return bool(buyable) and not any(pattern in status_text for pattern in out_patterns)

    def _format_price(self, raw_price):
        if raw_price in (None, ""):
            return None
        if isinstance(raw_price, dict):
            display_price = raw_price.get("displayString") or raw_price.get("display")
            if display_price:
                return str(display_price)
            raw_price = raw_price.get("amount") or raw_price.get("value")
        text = str(raw_price).strip()
        if not text:
            return None
        if re.search(r"[A-Z$€£¥]", text):
            return text
        try:
            amount = Decimal(text)
            if amount == amount.to_integral_value() and abs(amount) >= 100:
                amount = amount / Decimal("100")
            return f"${amount:.2f}" if self.currency == "USD" else text
        except InvalidOperation:
            return text
