# Amazon Stock Monitor

A 24/7 Amazon availability monitor that polls configured ASINs and sends Discord webhook alerts when products come in stock.

This tool only monitors inventory and sends alerts. It does not add products to cart, create checkout sessions, or place orders.

## Features

- Environment-driven ASIN groups for hosted deployments
- Amazon TVSS product endpoint as the default backend
- Legacy Ajax backend available only when explicitly selected
- Per-group Discord webhook routing
- In-stock transition alerts with duplicate suppression
- Configurable polling, timeout, and concurrency
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
      "webhooks": ["BLINK_FNF"]
    }
  ]
}'
```

ASINs must be 10-character Amazon IDs. Webhook names must match configured webhook target names.

### Discord webhooks

Use named webhook targets:

```env
WEBHOOK_BLINK_FNF_URL=https://discord.com/api/webhooks/...
WEBHOOK_BLINK_FNF_ROLE_ID=123456789012345678
```

`WEBHOOK_<NAME>_ROLE_ID` is optional. If present, the alert message pings that Discord role.

Legacy variables are still accepted for compatibility:

```env
BLINK_FNF_WEBHOOK_URL=...
BLINK_FNF_CHANNEL_ID=...
```

### Amazon TVSS

TVSS is the default backend:

```env
AMAZON_MONITOR_BACKEND=tvss
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
```

`POLL_INTERVAL_SECONDS` must be at least `0.5` to avoid unsafe tight loops.

## Railway/AWS Example

```env
AMAZON_MONITOR_BACKEND=tvss
MONITOR_CONFIG_JSON={"groups":[{"name":"NVIDIA","asins":["B0DT7L98J1","B0DTJFSSZG"],"webhooks":["BLINK_FNF"]}]}
WEBHOOK_BLINK_FNF_URL=https://discord.com/api/webhooks/...
WEBHOOK_BLINK_FNF_ROLE_ID=123456789012345678
TVSS_COOKIE_HEADER=session-id=...; ubid-main=...; at-main=...
TVSS_MARKETPLACE_ID=ATVPDKIKX0DER
TVSS_DOMAIN=amazon.com
TVSS_CURRENCY=USD
POLL_INTERVAL_SECONDS=2
TVSS_CONCURRENCY=5
TVSS_TIMEOUT=8
```

## Legacy Ajax Backend

Ajax is available for debugging or compatibility only:

```env
AMAZON_MONITOR_BACKEND=ajax
AMAZON_ENDPOINT=...
AMAZON_URL=...
MARKETPLACE_ID=...
MERCHANT_ID=...
PROXY_HOST=...
PROXY_PORT=...
PROXY_USER=...
PROXY_PASS=...
```

Ajax requests are batched at 25 ASINs per request.

## Verification

Local checks that do not hit external services:

```sh
python -m py_compile main.py webhooks.py amazon_tvss.py
python -m unittest discover -s tests
```

Full runtime verification requires valid Amazon TVSS credentials and Discord webhook configuration.
