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
- Sends alerts directly to Discord or to a generic webhook endpoint
- Can monitor multiple groups of ASINs with different webhook targets
- Uses Amazon's TVSS endpoint instead of browser automation or HTML scraping
- Supports Amazon-seller filtering on the confirmed path
- Fast-alert path: notify as soon as `basicOffer.offerId` appears, clearly label seller as unconfirmed, then enrich asynchronously

## Requirements

- Python 3.11
- `pip`
- An Amazon account for TVSS authentication
- PostgreSQL 14 or newer
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
docker compose run --rm monitor python cadence_canary.py --confirm
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

DATABASE_URL=postgresql://monitor:monitor@localhost:5432/amazon_monitor
MONITOR_ID=primary-us
TVSS_CREDENTIAL_ID=primary-amazon-account

POLL_INTERVAL_SECONDS=5.0
MONITOR_REQUIRE_AMAZON_SELLER=true
MONITOR_USE_BATCH=true
LOG_LEVEL=INFO
```

That is the simplest Discord setup. To forward alerts into a private app or an automation platform, point a second target at your own webhook consumer:

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
POLL_INTERVAL_SECONDS=5.0
TVSS_CONCURRENCY=20
TVSS_TIMEOUT=5
TVSS_429_COOLDOWN_SECONDS=900
TVSS_MAX_INTERVAL_SECONDS=300
TVSS_RECOVERY_SUCCESS_COUNT=120
AUTH_FAILURE_GRACE_SECONDS=30
MONITOR_REQUIRE_AMAZON_SELLER=true
MONITOR_FAST_ALERT=false
MONITOR_USE_BATCH=true
DATABASE_URL=postgresql://monitor:monitor@localhost:5432/amazon_monitor
MONITOR_ID=primary-us
TVSS_CREDENTIAL_ID=primary-amazon-account
ALERT_WORKER_CONCURRENCY=32
ALERT_TARGET_CONCURRENCY=2
ALERT_MAX_ATTEMPTS=10
ALERT_MAX_AGE_SECONDS=900
METRICS_PORT=9090
DATABASE_POOL_SIZE=40
PERFORMANCE_EXPERIMENT_ID=production
PERFORMANCE_VARIANT=control
PERFORMANCE_LOG_INTERVAL_SECONDS=300
TVSS_CONFIRMATION_SLOT_BORROWING=false
LOG_LEVEL=INFO
```

| Variable | Default | Notes |
| --- | --- | --- |
| `DATABASE_URL` | required | Shared Postgres authority for leases, stock state, transitions, and alert delivery. |
| `MONITOR_ID` | required | Stable deployment identity used in stock-state scope keys. |
| `TVSS_CREDENTIAL_ID` | device identity | Stable non-secret identity for one Amazon credential budget. |
| `POLL_INTERVAL_SECONDS` | `5.0` | Credential-wide request-start cadence. Requested values below `5.0` require a matching valid direct calibration; otherwise production uses `5.0`. |
| `TVSS_CALIBRATION_VALIDITY_SECONDS` | `86400` | Maximum age of a direct, error-free 120-observation calibration. |
| `FAST_ALERT_CONFIRM_EVERY_POLLS` | `12` | Latency-profile confirmation slot limit. This prevents confirmations from consuming every batch slot. |
| `ALERT_OUTBOX_FALLBACK_POLL_SECONDS` | `1` | Crash-recovery scan interval when an outbox wakeup notification is missed. |
| `ALERT_CONNECTION_WARM_SECONDS` | `90` | Idle Discord connection refresh cadence in the latency profile. |
| `TVSS_CONCURRENCY` | `20` | Per-ASIN-mode semaphore (only used when `MONITOR_USE_BATCH=false`). Ignored in batch mode. |
| `TVSS_TIMEOUT` | `5` | TVSS request timeout in seconds. |
| `TVSS_429_COOLDOWN_SECONDS` | `900` | Minimum credential-wide cooldown after HTTP 429. `Retry-After` is honored when longer. |
| `TVSS_MAX_COOLDOWN_SECONDS` | `3600` | Maximum exponentially increased credential cooldown. |
| `TVSS_MAX_INTERVAL_SECONDS` | `300` | Maximum adaptive request interval after repeated rate limits. |
| `TVSS_RECOVERY_SUCCESS_COUNT` | `120` | Clean batch polls required before reducing an adaptive interval. |
| `TVSS_INTERVAL_DECREMENT` | `0.25` | Interval reduction in seconds after a complete clean recovery window. |
| `TVSS_LEADER_LEASE_SECONDS` | `30` | Credential poll-leader lease lifetime. |
| `TVSS_LEADER_RENEW_SECONDS` | `10` | Credential lease renewal cadence. |
| `AUTH_FAILURE_GRACE_SECONDS` | `30` | How long auth failures can persist without a successful poll before the monitor sends an auth-expiry alert and exits with code `1`. |
| `MONITOR_REQUIRE_AMAZON_SELLER` | `true` | Require a full seller-qualified product confirmation before a confirmed alert. |
| `MONITOR_FAST_ALERT` | `false` | Opt-in speculative `offer_detected` delivery. Speculative alerts are labeled unconfirmed and use a separate deduplication identity. |
| `MONITOR_USE_BATCH` | `true` | If `true`, poll one `basicproducts` batch containing at most 20 ASINs. |
| `ALERT_WORKER_CONCURRENCY` | `32` | Global durable delivery worker concurrency. |
| `ALERT_TARGET_CONCURRENCY` | `2` | Per-target delivery concurrency. |
| `ALERT_MAX_ATTEMPTS` | `10` | Maximum attempts before a target delivery is dead-lettered. |
| `ALERT_MAX_AGE_SECONDS` | `900` | Maximum delivery retry age. |
| `ALERT_MAX_BACKOFF_SECONDS` | `60` | Maximum decorrelated retry delay. |
| `ALERT_CONNECT_TIMEOUT_SECONDS` | `1` | Per-attempt webhook connect timeout. |
| `ALERT_READ_TIMEOUT_SECONDS` | `2` | Per-attempt webhook read timeout. |
| `ALERT_ATTEMPT_TIMEOUT_SECONDS` | `3` | Total webhook attempt deadline. |
| `ALERT_CIRCUIT_FAILURE_THRESHOLD` | `5` | Consecutive transient target failures before opening its circuit. |
| `ALERT_CIRCUIT_OPEN_SECONDS` | `60` | Target circuit open duration. |
| `ALERT_DEAD_LETTER_RETENTION_DAYS` | `30` | Dead-letter audit retention before cleanup. |
| `STOCK_CONFIRM_TTL_SECONDS` | `90` | Maximum age for a full-product confirmation job. |
| `STOCK_OOS_REARM_COUNT` | `2` | Strong, cadence-separated out-of-stock observations required to rearm. Values below two are rejected. |
| `METRICS_PORT` | `9090` | Port for liveness, readiness, and Prometheus metrics when `PORT` is not assigned by the platform. |
| `DATABASE_POOL_SIZE` | `40` | Maximum asyncpg client connections. Benchmark smaller values before changing production. |
| `PERFORMANCE_EXPERIMENT_ID` | `production` | Stable identifier included in structured performance windows. |
| `PERFORMANCE_VARIANT` | `control` | Low-cardinality control or candidate label for performance comparisons. |
| `PERFORMANCE_LOG_INTERVAL_SECONDS` | `300` | Railway structured performance-window interval. Set to zero to disable. |
| `TVSS_CONFIRMATION_SLOT_BORROWING` | `false` | Canary-only confirmed-alert policy. Uses the next request slot immediately and defers the following poll without adding a second polling stream. |
| `PROXY_URL` | unset | Optional HTTP proxy URL. Credentials are never logged. |
| `PROXY_URLS_JSON` | unset | JSON array of proxy URLs or `host:port:user:password` entries, suitable for a Railway secret. |
| `PROXY_POOL_FILE` | unset | Ignored local proxy file with one URL or Webshare entry per line. |
| `PROXY_MODE` | `fallback` | `fallback`: direct first, then a ranked alternate only on network failure. HTTP 429 never rotates proxies. `always`: proxy first. |
| `LOG_LEVEL` | `INFO` | `INFO` or `DEBUG`. Per-poll status lines are at `DEBUG`. |

`endpoint.latency.env.example` is the low-latency overlay. It enables the
clearly labeled speculative alert, requests calibrated 0.5-second polling,
throttles confirmations to one slot per 12 successful batch polls, and keeps
the push-driven outbox and Discord connection warmer active.

Each successful batch emits a structured `tvss_stage` record with request wall
time, response read, JSON decode, state evaluation, alert-task scheduling,
credential queue wait, cadence wait, active route, attempt count, and unknown
observation count. Rate-limit records add `Retry-After` and cooldown fields.
Webhook completion emits its acknowledgment time separately. Proxy routes are
identified only by opaque hashes.

The durable monitor also emits a structured `performance_window` record every
five minutes. It contains rolling p50, p95, p99, maximum, counts, counters,
and gauges labeled with the Railway deployment, region, experiment, and
variant. Use `performance_compare.py` to evaluate complete ABBA canary blocks.

## Latency

Detect path means: the poll that sees stock, then alert dispatch. It does **not** include waiting for the next poll (`0 … POLL_INTERVAL_SECONDS`).

The current Railway direct-egress baseline, measured from successful attempts
only, is 109 ms p50 and 234 ms p95. Four of 19 attempts returned 429, so those
numbers are a diagnostic baseline rather than an accepted production result.
`latency_e2e.py` now includes retry, cadence, and cooldown time in the headline.

The bounded `us-east4-eqdc4a` canary selected a 5-second cadence. Its final
60-observation validation completed with zero 429s at 109.8 ms p50 and
223.7 ms p95. The internal response-read-through-dispatch benchmark measured
0.054 ms p95 on the same Railway run.

The real worst-case detection bound is the effective calibrated poll interval
plus TVSS p95, internal durable-path p95, and webhook p95. Subsecond alert
claims are valid only when a matching live calibration authorizes that cadence.

A subsequent [20-ASIN transport bakeoff](docs/tvss-transport-benchmark.md)
compared aiohttp with curl_cffi HTTP/1.1, HTTP/2, and pinned Chrome-profile
HTTP/2. No curl candidate passed the latency and confidence gates, so aiohttp
remains the production transport and no curl dependency or transport setting
was added.

| Mode | p50 | p95 | n |
| --- | --- | --- | --- |
| **Fast-alert** (batch to webhook) | **109 ms** | **234 ms** | 15 successful attempts |
| Confirm path (batch to full product to webhook) | ~210 ms | ~340 ms | 11 successful attempts |

Notes:

- Confirmed seller-qualified alerts are the default.
- `MONITOR_FAST_ALERT=true` is an explicit speculative path.
- Average end-to-end restock timing also depends on poll interval (often about half the interval, plus the detect path).
- External webhooks (for example Discord) add their own delivery time.
- Routing TVSS through a residential proxy increases detect-path latency substantially; keep proxies as `PROXY_MODE=fallback` for reliability, not for best latency.
- Sub-10 ms restock-to-notification is not achievable with public TVSS HTTP polling.

## Tuning

The optimized production topology is one credential and one batch of at most
20 ASINs. `POLL_INTERVAL_SECONDS` is the target cadence for request starts.
Response time does not get added to the interval.

When to lower `POLL_INTERVAL_SECONDS`:

- A direct 20-ASIN `cadence_canary.py` run completed its discovery and 120-observation validation with zero 429s or network errors.

When to raise it:

- Any 429 appears during validation.
- The structured `tvss_stage` log shows the credential-wide circuit breaker is active.

## Calibration

`cadence_canary.py` finds the fastest sustainable cadence for the current
credentials and region.

```sh
python cadence_canary.py --confirm --asins B0DT7L98J1,B0DTJFSSZG
```

Or via Docker:

```sh
docker compose run --rm monitor python cadence_canary.py --confirm
```

What it does:

1. Runs 60-second discovery buckets at `5`, `3`, `2`, `1.5`, `1`, `0.75`, and `0.5` seconds.
2. Stops the ladder immediately on the first 429 and does not test faster buckets.
3. Waits 15 minutes, then validates the fastest clean interval for 120 consecutive observations.
4. Any 429 immediately invalidates the credential calibration and ends the run.
5. Emits a machine-readable calibration record keyed by credential hash, marketplace, region, direct route, and batch size. Only a clean 120-observation validation with direct p50 at or below 119.9 ms may unlock a sub-five-second cadence.

Production fails closed to `5.0` seconds for a missing, expired (24 hours),
mismatched, invalidated, proxy-routed, or incomplete calibration. A 429
invalidates every calibration for that credential and marketplace.

The command requires `--confirm` because it uses live TVSS credentials.

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

- The first durable observation of an ASIN is treated as priming and does not alert
- A new epoch is armed only after two strong out-of-stock observations separated by one poll interval
- Missing, malformed, contradictory, and partial responses do not change stock state
- Confirmed alerts require a buyable full-product response and the configured seller policy
- A transition and all target deliveries are committed atomically before delivery begins
- Every target retries independently for up to 15 minutes, then dead-letters without reopening the stock transition
- Restarts resume pending deliveries and preserve rate-limit cooldowns
- If TVSS auth expires and the grace window is exceeded, the monitor sends an auth-expiry alert and exits non-zero so your platform can restart it

### Delivery operations

The operations server exposes:

- `GET /health/live`
- `GET /health/ready`
- `GET /metrics`

Run the isolated confirmation-slot ABBA canary before enabling slot borrowing:

```bash
python confirmation_slot_canary.py
```

The canary compares confirmation-start delay while verifying that both variants
preserve the same following poll slot.

Dead-lettered target deliveries can be inspected and controlled without
reopening the stock transition:

```sh
python main.py alerts list
python main.py alerts replay DELIVERY_UUID
python main.py alerts suppress DELIVERY_UUID
```

Generic webhooks receive stable `Idempotency-Key`, `X-Alert-Id`, and
`X-Alert-Delivery-Id` headers. The JSON body preserves the original product
fields and adds transition identity, stock epoch, confirmation state, matching
groups, and detection and persistence timestamps.

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

The checked-in Railway configuration runs the persistent monitor worker.
Configure `AUTH_JSON`, `DATABASE_URL`, `MONITOR_CONFIG_JSON`, and the selected
webhook variables as Railway secrets. The startup wrapper materializes
`AUTH_JSON` inside the ephemeral container with owner-only permissions, then
starts the durable monitor and applies pending database migrations.

For an explicitly triggered one-shot canary, override the start command with
`sh run_railway_canary.sh`, inspect the terminal deployment result, and let the
service exit.

Run the internal acceptance benchmark before using credentials:

```sh
python hot_path_benchmark.py --iterations 10000
```

With a disposable Postgres database, exercise the complete durable path and
local webhook acceptance:

```sh
TEST_DATABASE_URL=postgresql://monitor:monitor@localhost:5432/amazon_monitor \
python durable_latency_benchmark.py --confirm-database-writes
```

This requires 20-ASIN response-to-commit p95 below 20 ms,
commit-to-first-attempt p95 below 1 ms, and response-to-local-acceptance p95
below 20 ms. The benchmark removes its uniquely scoped rows on completion.

Run public proxy health checks without authenticated TVSS traffic:

```sh
PROXY_POOL_FILE=.firecrawl/webshare-proxies.txt python proxy_canary.py
```

Add `--tvss-confirm` only when you intentionally want one bounded batch check
through each of the three fastest healthy proxies.

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
  "signal": "offer_detected",
  "seller_verified": false,
  "seller": null,
  "source": "tvss-batch",
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

Stop authenticated tests and let the credential cool down. Run
`python cadence_canary.py --confirm` after the quiet period. The monitor honors
`Retry-After`, blocks every TVSS request during the credential-wide cooldown,
and permits one half-open batch poll after the persisted cooldown. HTTP 429
never changes the active proxy route.

### `HTTP 400` from TVSS

The monitor accepts at most 20 ASINs. The lower-level TVSS client still rejects
requests above the endpoint's 50-ASIN hard cap.

### Per-ASIN polling looks slow

This optimized topology intentionally targets at most 20 ASINs per credential.
Split a larger fleet across a separately designed credential strategy instead
of increasing concurrency.

## Local Verification

Run:

```sh
python -m py_compile main.py webhooks.py amazon_tvss.py amazon_auth.py tvss_runtime.py
python -m unittest discover -s tests
python hot_path_benchmark.py --iterations 10000
TEST_DATABASE_URL=postgresql://monitor:monitor@localhost:5432/amazon_monitor \
python durable_latency_benchmark.py --confirm-database-writes
```

Full live verification requires valid Amazon TVSS credentials and at least one working webhook target.
