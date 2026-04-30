# Amazon Stock Monitor

A 24/7 Amazon availability monitor that polls configured ASINs and sends webhook alerts when products come in stock.

This tool only monitors inventory and sends alerts. It does not add products to cart, create checkout sessions, or place orders.

## Features

- Environment-driven ASIN groups for hosted deployments
- Amazon TVSS product endpoint (Fire TV Shopping API)
- One coroutine per ASIN with a global concurrency semaphore — fleet size doesn't bound per-ASIN poll rate
- Per-group webhook routing — Discord and/or generic HTTP
- In-stock transition alerts; first observation per ASIN is treated as priming so restarts never fire false-positive alerts
- Optional sold-by-Amazon filter to ignore third-party seller restocks
- Per-target Discord rate-limit backoff (one rate-limited target does not stall the rest)
- Configurable polling, timeout, and concurrency
- Auth-expiry tripwire that alerts and exits non-zero so the platform restarts
- Structured operational logging

## Setup

1. Install dependencies:

   ```sh
   pip install -r requirements.txt
   ```

2. Create `endpoint.env` locally, or set the same variables in Railway, AWS, or your deployment platform.

3. Run the monitor:

   ```sh
   python main.py
   ```

## Required Environment

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

ASINs must be 10-character Amazon IDs. Webhook names must match configured webhook target names.

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

Instead of `TVSS_COOKIE_HEADER`, you can pass cookies as JSON:

```env
TVSS_COOKIES_JSON='[{"name":"session-id","value":"..."},{"name":"at-main","value":"..."}]'
```

If `at-main` is present, it is reused as `x-amz-access-token`. Otherwise set:

```env
TVSS_ACCESS_TOKEN=...
```

### Optional runtime settings

```env
POLL_INTERVAL_SECONDS=2
TVSS_CONCURRENCY=5
TVSS_TIMEOUT=8
AUTH_FAILURE_GRACE_SECONDS=30
MONITOR_REQUIRE_AMAZON_SELLER=true
```

- `POLL_INTERVAL_SECONDS` is the per-ASIN poll cadence. Must be at least `0.5`. Each ASIN runs in its own coroutine, so total fleet load is approximately `len(asins) / POLL_INTERVAL_SECONDS` requests/sec, capped by `TVSS_CONCURRENCY`.
- `TVSS_CONCURRENCY` caps in-flight TVSS requests across all ASINs.
- `AUTH_FAILURE_GRACE_SECONDS` is how long the monitor will tolerate auth failures without a successful poll before tripping the auth-expiry alert and exiting.
- `MONITOR_REQUIRE_AMAZON_SELLER=true` filters alerts to listings sold by Amazon — restocks where `merchantInfo.soldByAmazon` is false are logged as `IN_STOCK_FILTERED` and do not fire alerts.

## Docker / Railway

A `Dockerfile` and `railway.toml` are included.

```sh
docker build -t amazon-monitor .
docker run --env-file endpoint.env amazon-monitor
```

On Railway, point the service at this repo — the included `railway.toml` selects the Dockerfile builder, runs `python main.py`, and restarts up to 10 times on non-zero exit.

When TVSS auth has been failing continuously for `AUTH_FAILURE_GRACE_SECONDS` (default 30 s) without a single successful poll, the monitor sends a one-shot alert to all configured webhook targets and exits with code `1`. Refresh `TVSS_COOKIE_HEADER` (and `TVSS_ACCESS_TOKEN` if set) and redeploy.

## Railway/AWS Example

```env
MONITOR_CONFIG_JSON={"groups":[{"name":"NVIDIA","asins":["B0DT7L98J1","B0DTJFSSZG"],"webhooks":["PRIMARY"]}]}
WEBHOOK_PRIMARY_URL=https://discord.com/api/webhooks/...
WEBHOOK_PRIMARY_ROLE_ID=123456789012345678
TVSS_COOKIE_HEADER=session-id=...; ubid-main=...; at-main=...
TVSS_MARKETPLACE_ID=ATVPDKIKX0DER
TVSS_DOMAIN=amazon.com
TVSS_CURRENCY=USD
POLL_INTERVAL_SECONDS=2
TVSS_CONCURRENCY=5
TVSS_TIMEOUT=8
```

## Verification

Local checks that do not hit external services:

```sh
python -m py_compile main.py webhooks.py amazon_tvss.py
python -m unittest discover -s tests
```

Full runtime verification requires valid Amazon TVSS credentials and webhook configuration.
