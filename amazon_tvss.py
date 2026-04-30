import asyncio
import json
import logging
import os
import random
import re
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode

import aiohttp


TVSS_BASE_URL = "https://tvss.amazon.com"
TVSS_USER_AGENT = (
    "AMZN(SetTopBox/Amazon Fire TV Mantis/AKPGW064GI9HE,"
    "Android/7.1.2,ShopTV3P/release/2.0)"
)
DEFAULT_MARKETPLACE_ID = "ATVPDKIKX0DER"
DEFAULT_DOMAIN = "amazon.com"


class TVSSConfigError(RuntimeError):
    pass


class TVSSClient:
    """Minimal Amazon TVSS client for ASIN inventory monitoring."""

    def __init__(self):
        self.marketplace_id = os.getenv("TVSS_MARKETPLACE_ID", DEFAULT_MARKETPLACE_ID)
        self.domain = os.getenv("TVSS_DOMAIN", DEFAULT_DOMAIN)
        self.base_url = os.getenv("TVSS_BASE_URL", TVSS_BASE_URL).rstrip("/")
        self.currency = os.getenv("TVSS_CURRENCY", "USD")
        self.device_udid = os.getenv("TVSS_DEVICE_UDID") or self._random_udid()
        self.cookie_header = self._load_cookie_header()
        self.access_token = os.getenv("TVSS_ACCESS_TOKEN") or self._cookie_value("at-main")

        if not self.cookie_header:
            raise TVSSConfigError(
                "TVSS requires TVSS_COOKIE_HEADER or TVSS_COOKIES_JSON in endpoint.env"
            )

    @staticmethod
    def _random_udid():
        return "".join(random.choice("0123456789abcdef") for _ in range(32))

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

    def _cookie_value(self, name):
        if not self.cookie_header:
            return ""

        for part in self.cookie_header.split(";"):
            key, _, value = part.strip().partition("=")
            if key == name:
                return value
        return ""

    def _tvss_url(self, *segments, **params):
        path = "/".join(
            [self.base_url, "marketplaces", self.marketplace_id]
            + [str(segment).strip("/") for segment in segments]
        )
        query = {"sif_profile": "tvss"}
        query.update({k: v for k, v in params.items() if v is not None})
        return f"{path}?{urlencode(query)}"

    def _headers(self, media_type=None):
        request_id = "".join(random.choice("0123456789ABCDEF") for _ in range(20))
        headers = {
            "Accept": "application/json",
            "Cookie": self.cookie_header,
            "User-Agent": TVSS_USER_AGENT,
            "x-amzn-RequestId": request_id,
            "x-amz-msh-appid": (
                "name=ShopTV3P;ver=2000610;device=AFTMM;"
                f"os=Android_7.1.2;UDID={self.device_udid};tag=mshop-amazon-us-20"
            ),
        }
        if self.access_token:
            headers["x-amz-access-token"] = self.access_token
        if media_type:
            headers["Content-Type"] = (
                'application/vnd.com.amazon.tvss.api+json; '
                f'type="{media_type}"'
            )
        return headers

    async def _request(self, session, method, url, media_type=None, json_body=None):
        async with session.request(
            method,
            url,
            headers=self._headers(media_type),
            json=json_body,
            timeout=aiohttp.ClientTimeout(total=float(os.getenv("TVSS_TIMEOUT", "8"))),
        ) as response:
            text = await response.text()
            if response.status in (401, 403):
                raise TVSSConfigError(f"TVSS auth rejected with HTTP {response.status}")
            if response.status == 429:
                raise RuntimeError("TVSS rate limited")
            if response.status < 200 or response.status >= 300:
                raise RuntimeError(f"TVSS HTTP {response.status}: {text[:300]}")
            if not text:
                return None
            data = json.loads(text)
            return data.get("entity", data) if isinstance(data, dict) else data

    async def product(self, session, asin):
        data = await self._request(
            session,
            "GET",
            self._tvss_url("products", asin),
        )
        return self._parse_product(data, asin)

    async def products(self, session, asins):
        concurrency = int(os.getenv("TVSS_CONCURRENCY", "5"))
        semaphore = asyncio.Semaphore(max(1, concurrency))

        async def fetch(asin):
            async with semaphore:
                try:
                    return await self.product(session, asin)
                except TVSSConfigError as exc:
                    logging.error(f"TVSS auth/config check failed for {asin}: {exc}")
                    return {
                        "asin": asin,
                        "title": "N/A",
                        "in_stock": False,
                        "link": f"https://www.{self.domain}/dp/{asin}",
                        "images": [],
                        "price": None,
                        "seller": None,
                        "availability": {},
                        "source": "tvss",
                        "error": str(exc),
                        "error_type": "auth",
                    }
                except Exception as exc:
                    logging.error(f"TVSS product check failed for {asin}: {exc}")
                    return {
                        "asin": asin,
                        "title": "N/A",
                        "in_stock": False,
                        "link": f"https://www.{self.domain}/dp/{asin}",
                        "images": [],
                        "price": None,
                        "seller": None,
                        "availability": {},
                        "source": "tvss",
                        "error": str(exc),
                        "error_type": "request",
                    }

        return await asyncio.gather(*(fetch(asin) for asin in asins))

    def _parse_product(self, data, fallback_asin):
        if not isinstance(data, dict):
            data = {}

        asin = data.get("asin") or fallback_asin
        availability = data.get("productAvailabilityDetails") or {}
        if not isinstance(availability, dict):
            availability = {}

        offer_id = (
            data.get("offerId")
            or data.get("offerListingId")
            or data.get("buyingOptionId")
            or ""
        )
        price = self._format_price(data.get("price"))
        merchant = data.get("merchantInfo") or {}
        if not isinstance(merchant, dict):
            merchant = {}
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
            "seller": merchant.get("merchantName") or "Amazon.com",
            "soldByAmazon": merchant.get("soldByAmazon"),
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
