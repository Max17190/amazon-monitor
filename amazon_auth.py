"""Amazon device-code authentication for the TVSS API.

Implements the same Fire TV device-code pairing flow as saucesteals/shop:
  1. Generate a device with a random serial
  2. POST to api.amazon.com/auth/create/codepair → get a public code
  3. User authorizes at amazon.com/a/code
  4. POST to api.amazon.com/auth/register → get session cookies

The resulting cookies are saved to auth.json and automatically picked up
by TVSSClient when no TVSS_COOKIE_HEADER or TVSS_COOKIES_JSON is set.
"""

import asyncio
import json
import logging
import os
import stat
import sys
import time
from datetime import datetime, timezone

import aiohttp


CODEPAIR_URL = "https://api.amazon.com/auth/create/codepair"
REGISTER_URL = "https://api.amazon.com/auth/register"

DEVICE_TYPE = "A3NWHXTQ4EBCZS"
DEVICE_DOMAIN = "Device"
APP_NAME = "Amazon Shopping"
APP_VERSION = "24.20.2"
DEVICE_MODEL = "iPhone"
OS_VERSION = "17.6.1"
SOFTWARE_VERSION = "1"

MOBILE_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6_1 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/21G93"
)
AUTH_DOMAIN = "api.amazon.com"
COOKIE_DOMAIN = ".amazon.com"

POLL_INTERVAL = 5
DEFAULT_AUTH_PATH = "auth.json"


class AuthError(RuntimeError):
    pass


def generate_device():
    return {
        "domain": DEVICE_DOMAIN,
        "device_type": DEVICE_TYPE,
        "device_serial": os.urandom(12).hex(),
        "app_name": APP_NAME,
        "app_version": APP_VERSION,
        "device_model": DEVICE_MODEL,
        "os_version": OS_VERSION,
        "software_version": SOFTWARE_VERSION,
    }


def _auth_headers():
    return {
        "Content-Type": "application/json",
        "User-Agent": MOBILE_UA,
        "x-amzn-identity-auth-domain": AUTH_DOMAIN,
    }


async def create_code_pair(session, device):
    async with session.post(
        CODEPAIR_URL,
        json={"code_data": device},
        headers=_auth_headers(),
    ) as resp:
        body = await resp.read()
        if resp.status != 200:
            raise AuthError(f"code pair request failed ({resp.status}): {body[:512]}")
        data = json.loads(body)

    if not data.get("public_code") or not data.get("private_code"):
        raise AuthError("code pair response missing required fields")

    return data


async def register_device(session, device, public_code, private_code):
    """Attempt device registration. Returns auth result dict on success,
    None if the user hasn't authorized yet."""
    payload = {
        "auth_data": {
            "use_global_authentication": "true",
            "code_pair": {
                "public_code": public_code,
                "private_code": private_code,
            },
        },
        "registration_data": device,
        "requested_token_type": [
            "bearer",
            "mac_dms",
            "store_authentication_cookie",
            "website_cookies",
        ],
        "cookies": {
            "domain": COOKIE_DOMAIN,
            "website_cookies": [],
        },
        "requested_extensions": [
            "device_info",
            "customer_info",
        ],
    }

    async with session.post(
        REGISTER_URL,
        json=payload,
        headers=_auth_headers(),
    ) as resp:
        body = await resp.read()
        data = json.loads(body)

        if resp.status in (400, 401):
            error = data.get("response", {}).get("error", {})
            code = error.get("code", "")
            top_error = data.get("error", "")
            if code in ("InvalidValue", "AuthorizationPending", "Unauthorized") or \
               top_error in ("authorization_pending", "AuthorizationPending"):
                return None
            raise AuthError(
                f"registration rejected ({resp.status}): {error.get('message') or body[:512]}"
            )

        if resp.status == 429:
            raise AuthError("rate limited by Amazon — wait a minute and try again")

        if resp.status >= 500:
            raise AuthError(f"Amazon server error ({resp.status})")

        if resp.status != 200:
            raise AuthError(f"unexpected status ({resp.status}): {body[:512]}")

    success = data.get("response", {}).get("success", {})
    tokens = success.get("tokens", {})
    bearer = tokens.get("bearer", {})

    if not bearer.get("access_token"):
        return None

    raw_cookies = tokens.get("website_cookies", [])
    cookies = [
        {"name": c["Name"], "value": c["Value"].replace('"', "")}
        for c in raw_cookies
        if c.get("Name") and c.get("Value")
    ]

    customer_id = (
        success.get("extensions", {})
        .get("customer_info", {})
        .get("customer_id", "")
    )

    return {
        "cookies": cookies,
        "customer_id": customer_id,
        "bearer_token": bearer["access_token"],
        "refresh_token": data.get("refresh_token", ""),
    }


def auth_state_path():
    return os.getenv("AUTH_STATE_PATH", DEFAULT_AUTH_PATH)


def save_auth_state(state, path=None):
    path = path or auth_state_path()
    data = json.dumps(state, indent=2)
    with open(path, "w") as f:
        f.write(data)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)


def load_auth_state(path=None):
    path = path or auth_state_path()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def cookies_to_header(cookies):
    return "; ".join(
        f"{c['name']}={c['value']}"
        for c in cookies
        if c.get("name") and c.get("value")
    )


async def login_flow(domain="amazon.com"):
    device = generate_device()

    async with aiohttp.ClientSession() as session:
        # Step 1: Create code pair
        code_pair = await create_code_pair(session, device)
        public_code = code_pair["public_code"]
        private_code = code_pair["private_code"]
        expires_in = code_pair.get("expires_in", 600)

        url = f"https://www.{domain}/a/code?cbl-code={public_code}"

        print()
        print(f"  Open this URL and authorize the device:")
        print()
        print(f"    {url}")
        print()
        print(f"  Or go to https://www.{domain}/a/code and enter: {public_code}")
        print()

        # Step 2: Poll register until user completes
        deadline = time.monotonic() + expires_in
        attempt = 0
        while time.monotonic() < deadline:
            attempt += 1
            await asyncio.sleep(POLL_INTERVAL)

            try:
                result = await register_device(
                    session, device, public_code, private_code
                )
            except AuthError as exc:
                print(f"  Error: {exc}")
                return None

            if result is not None:
                state = {
                    "state": "authenticated",
                    "device": device,
                    "customerId": result["customer_id"],
                    "cookies": result["cookies"],
                    "refreshToken": result["refresh_token"],
                    "bearerToken": result["bearer_token"],
                    "authenticatedAt": datetime.now(timezone.utc).isoformat(),
                }

                path = auth_state_path()
                save_auth_state(state, path)

                print(f"  Authenticated successfully.")
                print(f"  Credentials saved to {path}")
                print()
                return state

            elapsed = int(time.monotonic() - (deadline - expires_in))
            remaining = expires_in - elapsed
            sys.stdout.write(f"\r  Waiting for authorization... ({remaining}s remaining)  ")
            sys.stdout.flush()

        print()
        print("  Code expired. Run login again to get a new code.")
        return None
