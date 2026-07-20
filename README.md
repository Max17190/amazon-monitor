# Amazon Stock Monitor

A reliable Amazon restock monitor for Discord and HTTP webhooks.

Amazon Stock Monitor watches up to 20 products and alerts you when an item
returns to stock. It uses Amazon's TVSS product API for fast checks and
PostgreSQL for durable state, deduplication, and alert delivery.

## Why use it

- **Fast detection:** checks all configured products in a single batch.
- **Accurate alerts:** confirms that a product is buyable and can require
  Amazon to be the seller.
- **No startup noise:** alerts only after a product moves from out of stock to
  in stock.
- **Reliable delivery:** retries failed webhooks without blocking healthy
  destinations.
- **Durable state:** preserves stock history, pending alerts, and rate-limit
  cooldowns across restarts.
- **Simple integrations:** supports Discord webhooks and generic JSON webhooks.

> [!NOTE]
> This is an unofficial project and is not affiliated with or endorsed by
> Amazon.

## How it works

```text
Amazon TVSS API → Stock confirmation → PostgreSQL → Discord or HTTP webhook
```

The monitor first learns the current state of each product. After two
out-of-stock observations, the product is armed for a restock. When a buyable
offer appears, the monitor confirms the product, records a new stock
transition, and queues an alert for each configured destination.

This model prevents false alerts at startup and ensures that a restart cannot
silently lose an accepted alert.

## Requirements

- Docker with Docker Compose, or Python 3.11 or newer
- An Amazon account for TVSS authentication
- PostgreSQL 14 or newer
- A Discord webhook or an HTTP endpoint that accepts JSON

Docker Compose is the recommended setup because it runs PostgreSQL and the
monitor together.

## Quick start

### 1. Clone the repository

```sh
git clone https://github.com/Max17190/amazon-monitor.git
cd amazon-monitor
```

### 2. Configure the monitor

```sh
cp endpoint.env.example endpoint.env
```

Open `endpoint.env` and set the products and Discord webhook:

```env
MONITOR_CONFIG_JSON={"groups":[{"name":"GPUs","asins":["B0DT7L98J1"],"webhooks":["DISCORD"]}]}
WEBHOOK_DISCORD_URL=https://discord.com/api/webhooks/REPLACE_ME
```

An ASIN is the 10-character product ID in an Amazon URL. For example, the ASIN
in `amazon.com/dp/B0DT7L98J1` is `B0DT7L98J1`.

Keep this database URL when using Docker Compose:

```env
DATABASE_URL=postgresql://monitor:monitor@postgres:5432/amazon_monitor
```

### 3. Connect your Amazon account

Create the local credential file and run the one-time login:

```sh
touch auth.json
docker compose run --rm --user "$(id -u):$(id -g)" monitor python main.py login
chmod 600 auth.json
```

Open the Amazon URL printed by the command, enter the displayed code, and
approve the device. Credentials are saved locally in `auth.json`.

### 4. Start monitoring

```sh
docker compose up -d
docker compose logs -f monitor
```

Stop the services with:

```sh
docker compose down
```

PostgreSQL data and Amazon credentials remain available for the next start.

## Configuration

The monitor loads configuration from `endpoint.env`.

### Products and destinations

`MONITOR_CONFIG_JSON` organizes products into named groups and connects each
group to one or more webhook targets:

```env
MONITOR_CONFIG_JSON={"groups":[{"name":"GPUs","asins":["B0DT7L98J1","B0DTJFSSZG"],"webhooks":["DISCORD"]},{"name":"CPUs","asins":["B0ABC12345"],"webhooks":["DISCORD","AUTOMATION"]}]}
```

Each group requires:

- A descriptive `name`
- One or more 10-character `asins`
- One or more configured webhook names

The monitor supports up to 20 unique ASINs for one Amazon credential. The same
ASIN may appear in multiple groups without being polled more than once.

To use the same destination for every group:

```env
MONITOR_CONFIG_JSON={"default_webhooks":["DISCORD"],"groups":[{"name":"GPUs","asins":["B0DT7L98J1"]}]}
```

### Discord

Set a Discord webhook URL:

```env
WEBHOOK_DISCORD_URL=https://discord.com/api/webhooks/REPLACE_ME
```

Optionally mention a role in every alert:

```env
WEBHOOK_DISCORD_ROLE_ID=123456789012345678
```

The target name, `DISCORD` in this example, must match the name used in
`MONITOR_CONFIG_JSON`.

### Generic webhooks

Generic webhooks work with custom services and automation platforms:

```env
WEBHOOK_AUTOMATION_URL=https://example.com/restock
WEBHOOK_AUTOMATION_KIND=generic
```

Requests include stable `Idempotency-Key`, `X-Alert-Id`, and
`X-Alert-Delivery-Id` headers. Consumers should use the idempotency key to
handle retries safely.

Example payload:

```json
{
  "asin": "B0DT7L98J1",
  "title": "Product title",
  "in_stock": true,
  "price": "$1999.00",
  "link": "https://www.amazon.com/dp/B0DT7L98J1",
  "seller": "Amazon.com",
  "signal": "restock_confirmed",
  "seller_verified": true,
  "group": "GPUs",
  "ts": "2026-05-05T12:34:56Z"
}
```

### Core settings

The defaults in `endpoint.env.example` are appropriate for most deployments.

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | Required | PostgreSQL connection for stock state and alert delivery. |
| `MONITOR_ID` | Required | Stable name for this deployment, such as `primary-us`. |
| `TVSS_CREDENTIAL_ID` | Example provided | Stable, non-secret name for the Amazon credential. |
| `POLL_INTERVAL_SECONDS` | `5.0` | Time between request starts. Production requires at least five seconds. |
| `MONITOR_REQUIRE_AMAZON_SELLER` | `true` | Require Amazon to be the confirmed seller. |
| `MONITOR_FAST_ALERT` | `false` | Send an unconfirmed alert before full product confirmation. |
| `ALERT_MAX_ATTEMPTS` | `10` | Delivery attempts before an alert is dead-lettered. |
| `ALERT_MAX_AGE_SECONDS` | `900` | Maximum delivery retry period in seconds. |
| `METRICS_PORT` | `9090` | Port for health checks and Prometheus metrics. |
| `LOG_LEVEL` | `INFO` | Set to `DEBUG` to log each product check. |

Advanced rate-limit, delivery, tracing, and proxy settings are documented in
`endpoint.env.example`.

## Alert behavior

By default, an alert is sent only when all of these conditions are true:

1. The product has been observed out of stock twice.
2. A new buyable offer appears.
3. The full product response confirms availability.
4. Amazon is confirmed as the seller.

Missing, partial, malformed, and contradictory responses do not change stock
state. Each stock transition is persisted before delivery begins, and every
destination retries independently.

Set `MONITOR_REQUIRE_AMAZON_SELLER=false` to allow confirmed third-party
sellers.

### Fast alert mode

Set `MONITOR_FAST_ALERT=true` to send an alert as soon as the batch response
shows an offer. This can reduce detection latency, but the seller is not yet
verified. Fast alerts are clearly labeled `offer_detected` and are not
retracted if later confirmation fails.

The default confirmed mode is recommended when accuracy matters more than the
earliest possible notification.

## Other Amazon marketplaces

The example configuration targets the United States:

```env
TVSS_MARKETPLACE_ID=ATVPDKIKX0DER
TVSS_DOMAIN=amazon.com
TVSS_CURRENCY=USD
```

Common alternatives:

| Marketplace | Marketplace ID | Domain |
| --- | --- | --- |
| United Kingdom | `A1F83G8C2ARO7P` | `amazon.co.uk` |
| Germany | `A1PA6795UKMFR9` | `amazon.de` |
| Japan | `A1VC38T7YXB528` | `amazon.co.jp` |
| Canada | `A2EUQ1WTGCTBG2` | `amazon.ca` |
| Australia | `A39IBJ37TRP1C6` | `amazon.com.au` |

Use the matching domain during login:

```sh
python main.py login --domain amazon.co.uk
```

## Run without Docker

Use this option when PostgreSQL is already running locally:

```sh
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp endpoint.env.example endpoint.env
```

Change the database host from `postgres` to `localhost`:

```env
DATABASE_URL=postgresql://monitor:monitor@localhost:5432/amazon_monitor
```

Then authenticate and start the monitor:

```sh
python main.py login
python main.py
```

Database migrations run automatically at startup.

## Operations

The monitor exposes three HTTP endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /health/live` | Confirms that the process is running. |
| `GET /health/ready` | Checks PostgreSQL, delivery health, and monitor role. |
| `GET /metrics` | Returns Prometheus-format metrics. |

A direct Python run exposes these endpoints on `localhost:9090` by default.
The Compose file does not publish the port to the host. Add a `9090:9090` port
mapping to the monitor service if external access is required.

Inspect failed deliveries:

```sh
python main.py alerts list
```

Replay or suppress a delivery using its returned ID:

```sh
python main.py alerts replay DELIVERY_UUID
python main.py alerts suppress DELIVERY_UUID
```

## Authentication and security

Credential sources are checked in this order:

1. `TVSS_COOKIE_HEADER`
2. `TVSS_COOKIES_JSON`
3. `auth.json`

Environment-based cookies are useful on hosted platforms where interactive
login is unavailable. If credentials expire, run `python main.py login` again
and restart the monitor.

Never commit `endpoint.env`, `auth.json`, cookie values, webhook URLs, or role
IDs.

## Deployment

### Docker

Docker Compose is the recommended single-host deployment. To run only the
image against an existing PostgreSQL database:

```sh
docker build -t amazon-monitor .
docker run --env-file endpoint.env -p 9090:9090 amazon-monitor
```

Mount `auth.json` at `/app/auth.json`, or provide cookies through environment
variables.

### Railway

The included `railway.toml` runs the monitor as a persistent worker. Configure:

- `DATABASE_URL`
- `MONITOR_ID`
- `TVSS_CREDENTIAL_ID`
- `MONITOR_CONFIG_JSON`
- At least one `WEBHOOK_<NAME>_URL`
- `AUTH_JSON`, containing a valid local `auth.json`

The startup script writes `AUTH_JSON` to a private file before launching the
monitor. Use one replica per TVSS credential. PostgreSQL prevents duplicate
poll leaders, but additional replicas do not increase safe polling throughput.

## Rate limits

The minimum production interval is five seconds. A faster interval can trigger
Amazon rate limits and is not automatically safe because it worked for another
account or region.

After an HTTP 429 response, the monitor pauses the entire credential, honors
`Retry-After`, and gradually recovers. The cooldown is stored in PostgreSQL and
survives restarts.

To test the fastest sustainable interval for your credentials and region:

```sh
python cadence_canary.py --confirm --asins B0DT7L98J1,B0DTJFSSZG
```

The canary sends live authenticated requests and requires exclusive use of the
credential. Stop the production monitor before running it.

## Troubleshooting

### The monitor starts but sends no alert

This is expected until a product is observed out of stock twice and then
returns to stock. A product that is already in stock at startup does not
trigger an alert.

### `MONITOR_CONFIG_JSON is required`

Confirm that `endpoint.env` exists in the project root and contains a valid,
single-line `MONITOR_CONFIG_JSON` value.

### `references unknown webhook`

A webhook name has no matching `WEBHOOK_<NAME>_URL`. Names are case-sensitive.

### `No TVSS credentials found`

Run `python main.py login`, or provide `TVSS_COOKIE_HEADER` or
`TVSS_COOKIES_JSON`.

### PostgreSQL connection fails

Use `postgres` as the database hostname inside Docker Compose. Use `localhost`
when the monitor and PostgreSQL run directly on the same machine.

### Amazon returns HTTP 429

Stop other processes using the same credential and allow the persisted
cooldown to finish. Do not rotate proxies to bypass the cooldown.

## Development

Run the local verification suite:

```sh
python -m py_compile main.py webhooks.py amazon_tvss.py amazon_auth.py tvss_runtime.py durable_runtime.py durable_store.py alert_delivery.py
python -m unittest discover -s tests
python hot_path_benchmark.py --iterations 10000
```

Live verification requires valid Amazon credentials and a working webhook.

## License

See [LICENSE](LICENSE).
