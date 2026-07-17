# Amazon Stock Monitor

Fast Amazon restock monitor that polls Amazon's TVSS API and sends alerts the moment a tracked ASIN moves from out of stock to in stock.

## Table of Contents

- [What It Does](#what-it-does)
- [Requirements](#requirements)
- [Quick Start (Docker)](#quick-start-docker)
- [Quick Start (pip)](#quick-start-pip)
- [Configuration](#configuration)
- [Authentication](#authentication)
- [Tuning](#tuning)
- [Latency](#latency)
- [Calibration](#calibration)
- [Running the Monitor](#running-the-monitor)
- [Docker / Railway](#docker--railway)
- [How Alerts Work](#how-alerts-work)
- [Troubleshooting](#troubleshooting)
- [Local Verification](#local-verification)

## What It Does

- Polls configured Amazon ASINs on a loop
- Sends alerts only on out-of-stock to in-stock transitions
- Sends alerts directly to Discord, or to Slack and other tools through a generic webhook endpoint
- Can monitor multiple groups of ASINs with different webhook targets
- Uses Amazon's TVSS endpoint instead of browser automation or HTML scraping
- Supports Amazon-auth-only filtering so third-party seller restocks can be ignored
- Fast-alert path: notify as soon as a batch poll sees an OOS to in-stock offer (optional async seller check)

## Requirements

- Python 3.7+
- `pip`
- An Amazon account for TVSS authentication
- At least one webhook destination

## Quick Start (Docker)

The easiest path. You need Docker (with `docker compose`) and an Amazon account.

```sh
git clone https://github.com/<your-user-or-org>/amazon-monitor.git
cd amazon-monitor

cp endpoint.env.example endpoint.env
# Edit endpoint.env: set MONITOR_CONFIG_JSON, WEBHOOK_DISCORD_URL, etc.

touch auth.json && chmod 600 auth.json   # placeholder for the bind-mount

docker compose run --rm monitor python main.py login   # one-time interactive auth
docker compose up -d                                   # start polling
docker compose logs -f monitor                         # tail logs
```

`auth.json` lives on the host (bind-mounted into the container), so restarts and
rebuilds keep your credentials. To stop:

```sh
docker compose down
```

Optional: calibrate to your cookies/IP once you have auth (see [Calibration](#calibration)):

```sh
docker compose run --rm monitor python benchmark.py --confirm
```

## Quick Start (pip)

### 1. Clone the repository

```sh
git clone https://github.com/<your-user-or-org>/amazon-monitor.git
cd amazon-monitor
```

### 2. Install dependencies

```sh
pip install -r requirements.txt
```

Using a virtual environment is recommended if you do not want to install dependencies globally.

### 3. Authenticate with Amazon

Run:

```sh
python main.py login
```

The script will print a URL and code. Open the URL, sign in to Amazon, and approve the device.

Example:

```text
Open this URL and authorize the device:

  https://www.amazon.com/a/code?cbl-code=XXXXXX

Or go to https://www.amazon.com/a/code and enter: XXXXXX
```

After success, credentials are saved to `auth.json`.

### 4. Create `endpoint.env`

Create a file named `endpoint.env` in the project root:

```env
MONITOR_CONFIG_JSON={"groups":[{"name":"NVIDIA GPUs","asins":["B0DT7L98J1"],"webhooks":["DISCORD"]}]}

WEBHOOK_DISCORD_URL=https://discord.com/api/webhooks/REPLACE_ME
WEBHOOK_DISCORD_ROLE_ID=

TVSS_MARKETPLACE_ID=ATVPDKIKX0DER
TVSS_DOMAIN=amazon.com
TVSS_CURRENCY=USD

POLL_INTERVAL_SECONDS=2.0
TVSS_BATCH_CHUNK_SIZE=50
TVSS_BATCH_CONCURRENCY=1
MONITOR_REQUIRE_AMAZON_SELLER=true
MONITOR_USE_BATCH=true
LOG_LEVEL=INFO
```

That is the simplest Discord setup. If you want to forward alerts into Slack, a private app, or an automation platform, point a second target at your own webhook consumer:

```env
MONITOR_CONFIG_JSON={"groups":[{"name":"NVIDIA GPUs","asins":["B0DT7L98J1"],"webhooks":["DISCORD","AUTOMATION"]}]}

WEBHOOK_DISCORD_URL=https://discord.com/api/webhooks/REPLACE_ME

WEBHOOK_AUTOMATION_URL=https://example.com/my-webhook
WEBHOOK_AUTOMATION_KIND=generic
```

### 5. Start the monitor

```sh
python main.py
```

If configuration is valid, the monitor will begin polling immediately.

## Configuration

The monitor loads environment variables from `endpoint.env`.

Do not commit `endpoint.env`, `auth.json`, webhook URLs, cookies, or role IDs.

### `MONITOR_CONFIG_JSON`

This variable defines what to monitor and where alerts should go.

Example:

```env
MONITOR_CONFIG_JSON={
  "groups": [
    {
      "name": "GPUs",
      "asins": ["B0DT7L98J1", "B0DTJFSSZG"],
      "webhooks": ["DISCORD"]
    },
    {
      "name": "CPUs",
      "asins": ["B0ABC12345"],
      "webhooks": ["DISCORD", "AUTOMATION"]
    }
  ],
  "default_webhooks": ["DISCORD"]
}
```

Notes:

- `groups` is required
- Each group needs a `name`
- Each group needs at least one 10-character ASIN
- Each webhook name must match a configured `WEBHOOK_<NAME>_URL`
- `default_webhooks` is optional and can be used when a group omits `webhooks`

### Webhook variables

Each target is defined by name:

```env
WEBHOOK_DISCORD_URL=https://discord.com/api/webhooks/...
WEBHOOK_DISCORD_ROLE_ID=123456789012345678

WEBHOOK_AUTOMATION_URL=https://example.com/restock
WEBHOOK_AUTOMATION_KIND=generic
```

Discord is the default, so `WEBHOOK_<NAME>_KIND=discord` is optional and usually better omitted.

Supported kinds:

- `discord` (default)
- `generic`

`WEBHOOK_<NAME>_ROLE_ID` is optional and only applies to Discord targets.

### Discord and Slack

Discord works out of the box through native Discord webhooks.

Slack usually fits best through the generic webhook path:

- a small relay you control
- Zapier, Make, Pipedream, or n8n
- a Slack app or workflow that accepts JSON and reformats it

That keeps the monitor simple while still making Slack delivery easy for teams that already use automation tooling.

### Amazon / TVSS variables

These control the marketplace and credential source:

```env
TVSS_MARKETPLACE_ID=ATVPDKIKX0DER
TVSS_DOMAIN=amazon.com
TVSS_CURRENCY=USD
```

Default US values:

- `TVSS_MARKETPLACE_ID=ATVPDKIKX0DER`
- `TVSS_DOMAIN=amazon.com`
- `TVSS_CURRENCY=USD`

Common marketplace values:

| Region | `TVSS_MARKETPLACE_ID` | `TVSS_DOMAIN` |
| --- | --- | --- |
| US | `ATVPDKIKX0DER` | `amazon.com` |
| UK | `A1F83G8C2ARO7P` | `amazon.co.uk` |
| DE | `A1PA6795UKMFR9` | `amazon.de` |
| JP | `A1VC38T7YXB528` | `amazon.co.jp` |
| CA | `A2EUQ1WTGCTBG2` | `amazon.ca` |
| AU | `A39IBJ37TRP1C6` | `amazon.com.au` |

### Runtime variables

```env
POLL_INTERVAL_SECONDS=2.0
TVSS_BATCH_CHUNK_SIZE=50
TVSS_BATCH_CONCURRENCY=1
TVSS_CONCURRENCY=20
TVSS_TIMEOUT=5
AUTH_FAILURE_GRACE_SECONDS=30
MONITOR_REQUIRE_AMAZON_SELLER=true
MONITOR_FAST_ALERT=true
MONITOR_USE_BATCH=true
LOG_LEVEL=INFO
```

| Variable | Default | Notes |
| --- | --- | --- |
| `POLL_INTERVAL_SECONDS` | `2.0` | Per-chunk base interval. Minimum `0.5`. AIMD doubles on 429 (cap `120 s`) and decrements after 30 successes. |
| `TVSS_BATCH_CHUNK_SIZE` | `50` | ASINs per batch request. **Amazon hard-rejects 51+ with HTTP 400.** |
| `TVSS_BATCH_CONCURRENCY` | `1` | Parallel chunks. Increasing this typically causes 429 storms — empirically 4 chunks at `1 s` triggered 84 % 429s in 75 s. Hard cap `4`. |
| `TVSS_CONCURRENCY` | `20` | Per-ASIN-mode semaphore (only used when `MONITOR_USE_BATCH=false`). Ignored in batch mode. |
| `TVSS_TIMEOUT` | `5` | TVSS request timeout in seconds. |
| `AUTH_FAILURE_GRACE_SECONDS` | `30` | How long auth failures can persist without a successful poll before the monitor sends an auth-expiry alert and exits with code `1`. |
| `MONITOR_REQUIRE_AMAZON_SELLER` | `true` | Prefer Amazon-sold offers. With `MONITOR_FAST_ALERT=true`, seller is checked asynchronously after the alert. |
| `MONITOR_FAST_ALERT` | `true` | If `true`, alert on batch OOS to in-stock as soon as an offer appears (one TVSS round trip on the critical path). Set `false` to wait for a full `product()` confirm before notifying. |
| `MONITOR_USE_BATCH` | `true` | If `true`, use chunked batch polling (`basicproducts`). |
| `PROXY_URL` | unset | Optional HTTP proxy URL. Credentials are never logged. |
| `PROXY_MODE` | `fallback` | `fallback`: stay direct until TVSS 429s, then use `PROXY_URL`. `always`: send all TVSS traffic through the proxy (higher latency). |
| `LOG_LEVEL` | `INFO` | `INFO` or `DEBUG`. Per-poll status lines are at `DEBUG`. |

## Latency

Detect path means: the poll that sees stock, then alert dispatch. It does **not** include waiting for the next poll (`0 … POLL_INTERVAL_SECONDS`).

Measured on Railway **us-east**, direct egress (no proxy), live in-stock ASIN, local webhook:

| Mode | p50 | p95 | n |
| --- | --- | --- | --- |
| **Fast-alert** (batch → webhook) | **~110 ms** | **~230 ms** | 15 |
| Confirm path (batch → full product → webhook) | ~210 ms | ~340 ms | 11 |

Notes:

- Fast-alert is the default critical path (`MONITOR_FAST_ALERT=true`).
- Average end-to-end restock timing also depends on poll interval (often about half the interval, plus the detect path).
- External webhooks (for example Discord) add their own delivery time.
- Routing TVSS through a residential proxy increases detect-path latency substantially; keep proxies as `PROXY_MODE=fallback` for reliability, not for best latency.
- Sub-10 ms restock-to-notification is not achievable with public TVSS HTTP polling.

## Tuning

**Per-ASIN poll cadence at default settings** is approximately:

```
ceil(num_asins / TVSS_BATCH_CHUNK_SIZE) * POLL_INTERVAL_SECONDS / TVSS_BATCH_CONCURRENCY
```

For 200 ASINs at chunk size 50, interval 2 s, concurrency 1: `ceil(200/50) * 2.0 / 1 = 8 s` between polls of any given ASIN. That is the rate Amazon's TVSS sustainably tolerates per cookie; raising `TVSS_BATCH_CONCURRENCY` past 1 typically backfires (cookies get parked in a multi-minute penalty box and the monitor backs off to ride it out).

When to lower `POLL_INTERVAL_SECONDS`:

- Small fleet (≤ 50 ASINs in 1 chunk) and a clean run with no 429s in the logs for several minutes → drop to `1.5` or `1.0`.
- After running `python benchmark.py` and seeing a bucket below `2.0` finish without aborting.

When to raise it:

- Sustained 429s in logs, or `chunkN entering penalty box` warnings.

When to lower `TVSS_BATCH_CHUNK_SIZE`:

- You see `HTTP 400` responses (only happens if you set the value above 50).
- Otherwise leave it at 50 — it is the sweet spot for both throughput and tail latency.

## Calibration

`benchmark.py` finds the best `TVSS_BATCH_CHUNK_SIZE`, `TVSS_BATCH_CONCURRENCY`, and `POLL_INTERVAL_SECONDS` for **your** cookies + IP + region.

```sh
python benchmark.py --confirm --asins B0DT7L98J1,B0DTJFSSZG
```

Or via Docker:

```sh
docker compose run --rm monitor python benchmark.py --confirm
```

What it does:

1. **Phase A** sweeps batch sizes `[1, 10, 25, 50]` × 3 reps with 5 s spacing, measuring P50 / P95 latency.
2. **Phase B** sustains polling at intervals `[5.0, 3.0, 2.0, 1.5, 1.0]` for 60 s each (slow → fast), with 30 s cooldown between buckets. Aborts a bucket on the first 3 consecutive 429s so a too-aggressive setting does not burn your cookies.
3. Prints paste-ready `endpoint.env` lines.

Run without `--confirm` to see the request budget without actually hitting Amazon. To restrict cookie burn, use `--max-asins 5`.

Re-run when:

- You refresh cookies (`python main.py login`).
- Your fleet doubles (or halves) in size.
- You start seeing `429` in steady-state logs.

## Authentication

Credential priority is:

1. `TVSS_COOKIE_HEADER`
2. `TVSS_COOKIES_JSON`
3. `auth.json`

For most users, `python main.py login` is the easiest setup path.

### Standard login flow

```sh
python main.py login
```

For non-US domains:

```sh
python main.py login --domain amazon.co.uk
python main.py login --domain amazon.de
python main.py login --domain amazon.co.jp
```

### Manual cookie setup

If you already have valid TVSS cookies, you can provide them directly:

```env
TVSS_COOKIE_HEADER=session-id=...; ubid-main=...; at-main=...
```

Or:

```env
TVSS_COOKIES_JSON=[{"name":"session-id","value":"..."},{"name":"at-main","value":"..."}]
```

If `at-main` is present in the cookies, it is reused automatically as `x-amz-access-token`. You can also set:

```env
TVSS_ACCESS_TOKEN=...
```

## Running the Monitor

Start the monitor:

```sh
python main.py
```

Useful behavior to know:

- The first observation of an ASIN is treated as priming and does not alert
- Alerts are sent only on out-of-stock to in-stock transitions
- With fast-alert (default), a newly seen batch offer triggers the webhook immediately; Amazon-seller confirm can still run in the background and does not block the alert
- With `MONITOR_FAST_ALERT=false`, the monitor waits for a full product confirm before notifying
- If webhook delivery fails for every target, the transition is not committed and will be retried on a later poll
- If TVSS auth expires and the grace window is exceeded, the monitor sends an auth-expiry alert and exits non-zero so your platform can restart it

## Docker / Railway

This repo includes a `Dockerfile` and `railway.toml`.

### Docker

Build:

```sh
docker build -t amazon-monitor .
```

Run:

```sh
docker run --env-file endpoint.env amazon-monitor
```

If you are using `auth.json`, mount it into the container or supply cookie env vars instead.

### Railway

The included `railway.toml` is set up to run the monitor with Docker.

Typical flow:

1. Create a Railway project from this repository
2. Add the same variables from `endpoint.env` to Railway
3. Deploy
4. If using `auth.json`, either bake an alternative credential strategy into env vars or provide the file through your deployment workflow

## How Alerts Work

### Discord

Discord targets receive an embed with:

- Product title and link
- ASIN
- Price
- Seller
- Source
- Availability message when present

If `WEBHOOK_<NAME>_ROLE_ID` is set, that role is mentioned in the alert.

### Slack and other automations

Generic targets receive a JSON POST payload like:

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
  "ts": "2026-05-05T12:34:56Z"
}
```

That payload is intended for automation systems, custom relays, and Slack-forwarding workflows.

### Retry behavior

- Transient webhook failures are retried
- Discord HTTP 429s trigger per-target backoff
- One rate-limited Discord webhook does not block delivery to other targets

## Troubleshooting

### `MONITOR_CONFIG_JSON is required`

Your `endpoint.env` is missing `MONITOR_CONFIG_JSON`, or the file is not being loaded from the project root.

### `references unknown webhook`

A group in `MONITOR_CONFIG_JSON` names a webhook target that does not have a matching `WEBHOOK_<NAME>_URL`.

### `No TVSS credentials found`

Run:

```sh
python main.py login
```

Or provide `TVSS_COOKIE_HEADER` / `TVSS_COOKIES_JSON`.

### Auth worked before, then stopped

Amazon credentials can expire. Re-run:

```sh
python main.py login
```

Then restart the monitor.

### I only want alerts for Amazon as the seller

Set:

```env
MONITOR_REQUIRE_AMAZON_SELLER=true
```

### I want to see per-poll status

Set:

```env
LOG_LEVEL=DEBUG
```

### I am hitting 429s every cycle

Run `python benchmark.py --confirm` to find a sustainable interval, then paste its output into `endpoint.env`. If the monitor keeps logging `entering penalty box`, your cookies are in Amazon's multi-minute throttle window — wait 5–15 minutes before resuming, or run `python main.py login` to refresh credentials.

### `HTTP 400` from TVSS

`TVSS_BATCH_CHUNK_SIZE` must be ≤ 50. Amazon's basicproducts endpoint hard-rejects 51+ ASINs with an empty-body 400.

### Per-ASIN polling looks slow

That is expected for large fleets. Per-ASIN cadence ≈ `ceil(num_asins / 50) * POLL_INTERVAL_SECONDS / TVSS_BATCH_CONCURRENCY`. For 200 ASINs at the defaults that is 8 s between polls of any one ASIN — increasing concurrency typically *worsens* this because parallel chunks burn the cookie. Calibrate first.

## Local Verification

Run:

```sh
python -m py_compile main.py webhooks.py amazon_tvss.py amazon_auth.py
python -m unittest discover -s tests
```

Full live verification requires valid Amazon TVSS credentials and at least one working webhook target.
