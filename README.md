# Amazon Stock Monitor

A 24/7 Amazon availability monitor that polls configured ASINs and sends webhook alerts when products come in stock.

Built on the [TVSS](https://tvss.amazon.com) (TV Shopping Service) API — the same internal API behind Amazon's Fire TV Shopping app.

This tool only monitors inventory and sends alerts. It does not add products to cart, create checkout sessions, or place orders.

## Features

- Amazon TVSS product endpoint — structured JSON, no HTML scraping, no browser
- One coroutine per ASIN with a global concurrency semaphore — fleet size doesn't bound per-ASIN poll rate
- Per-group webhook routing — Discord and/or generic HTTP
- In-stock transition alerts; first observation per ASIN is treated as priming so restarts never fire false-positive alerts
- Webhook delivery is retried on transient errors and a transition is not committed unless at least one target accepts it
- Optional sold-by-Amazon filter to ignore third-party seller restocks
- Per-target Discord rate-limit backoff (one rate-limited target does not stall the rest)
- Configurable polling, timeout, and concurrency; `LOG_LEVEL` for controlling per-poll log volume
- Auth-expiry tripwire that alerts and exits non-zero so the platform restarts
- Graceful shutdown on `SIGINT` / `SIGTERM` so deploys do not abort in-flight requests
- Low-latency TCP connector: DNS caching, connection keepalive, no artificial pool cap

## Quick Start

```sh
pip install -r requirements.txt
python main.py login
# → authenticates with Amazon, saves credentials to auth.json
# Configure endpoint.env with your ASINs and webhooks (see Configuration below)
python main.py
# → starts monitoring
```

## Authentication

The monitor uses Amazon's device-code pairing flow — the same mechanism Fire TV hardware uses to register itself. No browser cookie extraction required.

### `python main.py login`

```sh
python main.py login
```

This will:

1. Generate a device registration
2. Print a URL and code
3. Wait for you to authorize at `amazon.com/a/code`
4. Save the session cookies to `auth.json`

```
  Open this URL and authorize the device:

    https://www.amazon.com/a/code?cbl-code=A4K7X2

  Or go to https://www.amazon.com/a/code and enter: A4K7X2

  Waiting for authorization... (580s remaining)
  Authenticated successfully.
  Credentials saved to auth.json
```

Once `auth.json` exists, `python main.py` picks it up automatically — no env var configuration needed.

For non-US Amazon regions, pass `--domain`:

```sh
python main.py login --domain amazon.co.uk
python main.py login --domain amazon.de
python main.py login --domain amazon.co.jp
```

### Alternative: manual cookie env vars

You can also set cookies directly via environment variables (in `endpoint.env` or your deployment platform). These take precedence over `auth.json`:

```env
TVSS_COOKIE_HEADER=session-id=...; ubid-main=...; at-main=...
```

Or as JSON:

```env
TVSS_COOKIES_JSON='[{"name":"session-id","value":"..."},{"name":"at-main","value":"..."}]'
```

### Cookie expiry

Cookies expire periodically. When they do, the monitor detects the auth failure (HTTP 401/403 from TVSS), sends a one-shot alert to all webhook targets, and exits with code `1`. Your deployment platform (Railway, Docker restart policy, systemd, etc.) should restart the process. Run `python main.py login` again to refresh credentials.

## Configuration

Create `endpoint.env` locally, or set the same variables in your deployment platform (Railway, AWS, GCP, etc.).

### Monitor config

Configure product groups with `MONITOR_CONFIG_JSON`. Each group has a display name, ASIN list, and named webhook targets.

```env
MONITOR_CONFIG_JSON='{
  "groups": [
    {
      "name": "NVIDIA",
      "asins": ["B0DT7L98J1", "B0DTJFSSZG"],
      "webhooks": ["PRIMARY"]
    }
  ]
}'
```

ASINs must be 10-character Amazon IDs (find them in Amazon product URLs: `amazon.com/dp/B0DT7L98J1`). Webhook names must match configured webhook target names.

### Webhooks

Each named target is either a Discord webhook (default) or a generic HTTP webhook.

```env
WEBHOOK_PRIMARY_URL=https://discord.com/api/webhooks/...
WEBHOOK_PRIMARY_ROLE_ID=123456789012345678

WEBHOOK_NOTIFY_URL=https://example.com/hooks/restock
WEBHOOK_NOTIFY_KIND=generic
```

`WEBHOOK_<NAME>_KIND` is `discord` (default) or `generic`. `WEBHOOK_<NAME>_ROLE_ID` is optional and only applies to Discord targets — if set, the alert pings that role.

Generic targets receive a POST with this JSON body:

```json
{
  "asin": "B0DT7L98J1",
  "title": "Product title",
  "in_stock": true,
  "price": "$1999.00",
  "link": "https://www.amazon.com/dp/B0DT7L98J1",
  "image": "https://...",
  "seller": "Amazon.com",
  "source": "tvss",
  "group": "NVIDIA",
  "ts": "2026-04-30T18:22:11Z"
}
```

### Amazon TVSS

```env
TVSS_COOKIE_HEADER=session-id=...; ubid-main=...; at-main=...
TVSS_MARKETPLACE_ID=ATVPDKIKX0DER
TVSS_DOMAIN=amazon.com
TVSS_CURRENCY=USD
```

Instead of `TVSS_COOKIE_HEADER`, you can pass cookies as the JSON array format from `shop`'s auth state:

```env
TVSS_COOKIES_JSON='[{"name":"session-id","value":"..."},{"name":"at-main","value":"..."}]'
```

If `at-main` is present in the cookies, it is reused as the `x-amz-access-token` header. Otherwise set explicitly:

```env
TVSS_ACCESS_TOKEN=...
```

### Marketplace IDs

| Region | `TVSS_MARKETPLACE_ID` | `TVSS_DOMAIN` |
|--------|----------------------|---------------|
| US | `ATVPDKIKX0DER` | `amazon.com` |
| UK | `A1F83G8C2ARO7P` | `amazon.co.uk` |
| DE | `A1PA6795UKMFR9` | `amazon.de` |
| JP | `A1VC38T7YXB528` | `amazon.co.jp` |
| CA | `A2EUQ1WTGCTBG2` | `amazon.ca` |
| AU | `A39IBJ37TRP1C6` | `amazon.com.au` |

### Optional runtime settings

```env
POLL_INTERVAL_SECONDS=2
TVSS_TIMEOUT=5
AUTH_FAILURE_GRACE_SECONDS=30
MONITOR_REQUIRE_AMAZON_SELLER=true
MONITOR_USE_BATCH=true
LOG_LEVEL=INFO
```

- `POLL_INTERVAL_SECONDS` is the poll cadence. Must be at least `0.5`. In batch mode (default), one request per cycle covers all ASINs. In per-ASIN mode, each ASIN runs in its own coroutine.
- `TVSS_TIMEOUT` is the total request timeout in seconds. Default `5`. TCP connect is capped at 2 s within this budget so a hung connection is detected quickly.
- `AUTH_FAILURE_GRACE_SECONDS` is how long the monitor will tolerate auth failures without a successful poll before tripping the auth-expiry alert and exiting.
- `MONITOR_REQUIRE_AMAZON_SELLER` filters alerts to listings **sold by Amazon**. Default `true`. Third-party seller restocks are logged as `IN_STOCK_FILTERED` and do not fire alerts. Set to `false` to alert on any seller.
- `MONITOR_USE_BATCH` enables hybrid batch polling (default `true`). One request polls all ASINs; a full product fetch is only made when a stock transition is detected, to confirm the seller is Amazon. Set to `false` for legacy per-ASIN polling.
- `LOG_LEVEL` sets logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`). Default `INFO`. Per-poll status is logged at `DEBUG` only — at `INFO` you see startup, restock transitions, errors, and the auth-expiry alert.

### Webhook delivery semantics

- Each Discord or generic send is retried up to 3 times with exponential backoff (1 s, 2 s, 4 s) on transient failures (network errors, HTTP 5xx).
- HTTP 429 from Discord is **not** retried internally — it triggers per-target backoff inside the dispatcher so a single rate-limited webhook does not stall delivery to other targets.
- A restock transition is only committed to internal state if at least one target accepts the alert. If every target fails or is in backoff, the transition stays uncommitted and the next poll will re-attempt delivery — alerts are not silently dropped on transient outages.

### Graceful shutdown

The monitor installs handlers for `SIGINT` and `SIGTERM`. On signal it sets the shutdown event and the polling tasks exit at their next sleep, so deploys/restarts do not abort in-flight TVSS requests.

## Docker / Railway

A `Dockerfile` and `railway.toml` are included.

```sh
docker build -t amazon-monitor .
docker run --env-file endpoint.env amazon-monitor
```

On Railway, point the service at this repo — the included `railway.toml` selects the Dockerfile builder, runs `python main.py`, and restarts up to 10 times on non-zero exit.

## Latency

The single biggest factor in detection speed is **network proximity to Amazon's TVSS servers**. Deploy the container in **AWS us-east-1** (or the closest available region on your platform) to cut TVSS round-trip from ~100-200 ms down to ~5-20 ms.

With optimal deployment and `POLL_INTERVAL_SECONDS=0.5`:

- Average detection delay: ~250 ms (half the poll interval)
- TVSS request: ~10-20 ms (us-east-1 to TVSS)
- Webhook delivery: ~100-500 ms (depends on target)
- **Total restock-to-notification: ~350-750 ms**

Set `TVSS_CONCURRENCY` to at least the number of ASINs monitored so no coroutine waits for a semaphore slot.

## Full Example

After running `python main.py login`, create `endpoint.env`:

```env
MONITOR_CONFIG_JSON={"groups":[{"name":"NVIDIA","asins":["B0DT7L98J1","B0DTJFSSZG"],"webhooks":["PRIMARY"]}]}
WEBHOOK_PRIMARY_URL=https://discord.com/api/webhooks/...
WEBHOOK_PRIMARY_ROLE_ID=123456789012345678
TVSS_MARKETPLACE_ID=ATVPDKIKX0DER
TVSS_DOMAIN=amazon.com
TVSS_CURRENCY=USD
POLL_INTERVAL_SECONDS=2
TVSS_CONCURRENCY=20
TVSS_TIMEOUT=5
```

TVSS credentials are read from `auth.json` automatically. No cookie env vars needed unless you prefer to set them explicitly.

## Verification

Local checks that do not hit external services:

```sh
python -m py_compile main.py webhooks.py amazon_tvss.py amazon_auth.py
python -m unittest discover -s tests
```

Full runtime verification requires valid Amazon TVSS credentials and webhook configuration.
