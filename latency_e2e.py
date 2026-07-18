#!/usr/bin/env python3
"""Live E2E restock-path latency probe against a real in-stock ASIN.

Measures production path timing:
  batch_products (detect offer) -> product() confirm -> generic webhook receive

Does not print cookies, tokens, or request headers.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import statistics
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import aiohttp
from dotenv import load_dotenv

load_dotenv("endpoint.env")

from amazon_tvss import TVSSClient  # noqa: E402
from main import AlertDispatcher, AlertState, product_from_batch  # noqa: E402
from webhooks import WebhookTarget  # noqa: E402


DEFAULT_ASINS = (
    "B00FLYWNYQ",  # Instant Pot Duo (often Amazon in-stock)
    "B07FZ8S74R",
    "B0DT7L98J1",
    "B0DTJFSSZG",
    "B08N5WRWNW",
    "B0D1XD1ZV3",
    "B0C33XXS56",
)
REPS = int(os.getenv("LATENCY_REPS", "11"))
PORT = int(os.getenv("LATENCY_WEBHOOK_PORT", "18765"))
SPACING = float(os.getenv("LATENCY_SPACING", "4.0"))
RETRY_429 = int(os.getenv("LATENCY_RETRY_429", "5"))
# fast = batch + webhook (MONITOR_FAST_ALERT path); confirm = batch + product + webhook
LATENCY_MODE = os.getenv("LATENCY_MODE", "fast").strip().lower()
LATENCY_BATCH_SIZE = min(20, max(1, int(os.getenv("LATENCY_BATCH_SIZE", "20"))))

receipts = []
receipt_event = None


def _retryable(exc):
    msg = str(exc).lower()
    name = type(exc).__name__.lower()
    return (
        "rate limited" in msg
        or "429" in msg
        or "timeout" in msg
        or "timeout" in name
        or "temporarily" in msg
        or "connection reset" in msg
        or "server disconnected" in msg
    )


async def tvss_call(label, coro_factory):
    """Run one logical TVSS call with retry and cooldown in headline timing."""
    last_exc = None
    total_started = time.perf_counter()
    rate_limits = 0
    for attempt in range(1, RETRY_429 + 1):
        attempt_started = time.perf_counter()
        try:
            result = await coro_factory()
            success_ms = (time.perf_counter() - attempt_started) * 1000.0
            total_ms = (time.perf_counter() - total_started) * 1000.0
            return result, total_ms, success_ms, attempt, rate_limits
        except Exception as exc:
            last_exc = exc
            if "429" in str(exc) or "rate limit" in str(exc).lower():
                rate_limits += 1
            if _retryable(exc) and attempt < RETRY_429:
                print(
                    f"  {label}: {type(exc).__name__} attempt {attempt}/{RETRY_429}; "
                    "credential controller will enforce cadence and cooldown; "
                    "all time remains in headline latency"
                )
                continue
            raise
    raise last_exc


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(n)
        t = time.perf_counter()
        receipts.append({"t_recv": t, "body": body, "path": self.path})
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")
        if receipt_event is not None:
            loop, ev = receipt_event
            try:
                loop.call_soon_threadsafe(ev.set)
            except Exception:
                pass

    def log_message(self, *_args):
        return


def start_server():
    httpd = HTTPServer(("127.0.0.1", PORT), Handler)
    thread = Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = max(0, min(len(xs) - 1, int(round((p / 100) * (len(xs) - 1)))))
    return xs[k]


def stats_line(vals):
    if not vals:
        return "n/a"
    vals = sorted(vals)
    return (
        f"min={min(vals):.1f} p50={pct(vals, 50):.1f} p95={pct(vals, 95):.1f} "
        f"mean={statistics.mean(vals):.1f} max={max(vals):.1f} ms (n={len(vals)})"
    )


def batch_asins(selected_asin):
    configured = [
        value.strip().upper()
        for value in os.getenv("LATENCY_BATCH_ASINS", "").split(",")
        if len(value.strip()) == 10 and value.strip().isalnum()
    ]
    values = [selected_asin, *configured]
    return list(dict.fromkeys(values))[:LATENCY_BATCH_SIZE]


async def wait_receipt(ev, timeout=5.0):
    try:
        await asyncio.wait_for(ev.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        return False


async def pick_in_stock_asin(client, session, candidates):
    for asin in candidates:
        try:
            batch, _total_ms, _success_ms, _attempts, _rate_limits = await tvss_call(
                f"candidate.{asin}.batch",
                lambda a=asin: client.batch_products(session, [a]),
            )
        except Exception as exc:
            print(f"candidate {asin}: error {type(exc).__name__}: {exc}")
            await asyncio.sleep(2.0)
            continue
        observation = batch[asin]
        if not observation.has_offer:
            print(f"candidate {asin}: offer_detected=False")
            continue
        try:
            product, _total_ms, _success_ms, _attempts, _rate_limits = await tvss_call(
                f"candidate.{asin}.product",
                lambda a=asin: client.product(session, a),
            )
        except Exception as exc:
            print(f"candidate {asin}: product error {type(exc).__name__}: {exc}")
            continue
        in_stock = bool(product.get("in_stock"))
        sold = product.get("soldByAmazon")
        title = (product.get("title") or "")[:70]
        print(
            f"candidate {asin}: in_stock={in_stock} soldByAmazon={sold} "
            f"price={product.get('price')} title={title!r}"
        )
        if observation.has_offer:
            return asin, product
        await asyncio.sleep(1.0)
    return None, None


async def run_probe():
    global receipt_event

    raw = os.getenv("LATENCY_ASINS", "").strip()
    candidates = [a.strip().upper() for a in raw.split(",") if a.strip()] or list(
        DEFAULT_ASINS
    )

    httpd = start_server()
    loop = asyncio.get_running_loop()
    client = TVSSClient()
    client.configure_rate_controller(SPACING)
    connector = aiohttp.TCPConnector(
        limit=0,
        ttl_dns_cache=300,
        keepalive_timeout=120,
    )
    target = WebhookTarget(
        name="LOCAL",
        url=f"http://127.0.0.1:{PORT}/restock",
        kind="generic",
    )

    route_id = client.proxy_pool.primary_route.route_id
    print("=== Live E2E restock-path latency probe ===")
    print(f"host={socket.gethostname()}")
    print(f"runner_name={os.getenv('RUNNER_NAME', '')}")
    print(f"github_repository={os.getenv('GITHUB_REPOSITORY', '')}")
    print(f"github_run_id={os.getenv('GITHUB_RUN_ID', '')}")
    print(f"tvss_base={client.base_url}")
    print(f"marketplace={client.marketplace_id}")
    print(
        f"proxy_count={client.proxy_pool.proxy_count} "
        f"initial_route={route_id}"
    )
    print(f"latency_mode={LATENCY_MODE}")
    print(f"reps={REPS}")
    print()

    # Network floor (no secrets). Direct TCP only — not via proxy.
    try:
        tcp_times = []
        for _ in range(5):
            t0 = time.perf_counter()
            s = socket.create_connection(("tvss.amazon.com", 443), timeout=5)
            tcp_times.append((time.perf_counter() - t0) * 1000)
            s.close()
        print(
            f"TCP connect tvss.amazon.com (direct): p50={pct(tcp_times, 50):.1f}ms "
            f"min={min(tcp_times):.1f} max={max(tcp_times):.1f}"
        )
    except Exception as exc:
        print(f"TCP connect probe failed: {type(exc).__name__}")
    print()

    async with aiohttp.ClientSession(connector=connector) as session:
        # Choose ASIN (product only; avoids extra batch burn before timed reps)
        print("Cooling 15s before TVSS calls (rate-limit buffer)...")
        await asyncio.sleep(15.0)

        asin, live = await pick_in_stock_asin(client, session, candidates)
        if not asin:
            print("ABORT: no in-stock Amazon-sold candidate among", candidates)
            httpd.shutdown()
            return 2

        print(f"using ASIN={asin}")
        monitored_asins = batch_asins(asin)
        print(f"batch_size={len(monitored_asins)}")
        print(
            f"live in_stock={live.get('in_stock')} soldByAmazon={live.get('soldByAmazon')} "
            f"price={live.get('price')}"
        )
        print("Spacing timed reps; sleeping 10s before first timed rep...")
        await asyncio.sleep(10.0)
        print()

        dispatcher = AlertDispatcher(session)
        results = []

        for i in range(1, REPS + 1):
            receipts.clear()
            ev = asyncio.Event()
            receipt_event = (loop, ev)

            state = AlertState()
            state.observe(asin, False)  # prime as OOS
            if not state.peek(asin, True):
                print(f"rep {i}: peek did not see transition; skip")
                continue

            path_started = time.perf_counter()
            (
                batch_result,
                batch_total_ms,
                batch_success_ms,
                batch_attempts,
                batch_rate_limits,
            ) = await tvss_call(
                f"rep{i}.batch",
                lambda: client.batch_products(session, monitored_asins),
            )
            info = batch_result.get(asin, {})
            has_offer = info.get("has_offer", False)
            if not has_offer:
                print(f"rep {i}: batch has_offer=False; skip")
                await asyncio.sleep(SPACING)
                continue

            if LATENCY_MODE in ("confirm", "full", "two-hop"):
                (
                    product,
                    product_total_ms,
                    product_success_ms,
                    product_attempts,
                    product_rate_limits,
                ) = await tvss_call(
                    f"rep{i}.product",
                    lambda: client.product(session, asin),
                )
                if not product.get("in_stock"):
                    print(f"rep {i}: product in_stock=False; skip")
                    await asyncio.sleep(SPACING)
                    continue
                if product.get("soldByAmazon") is False:
                    print(f"rep {i}: third-party only; would filter")
                    await asyncio.sleep(SPACING)
                    continue
            else:
                # Fast-alert path: notify from batch payload only.
                product = product_from_batch(asin, info, domain=client.domain)
                product_total_ms = 0.0
                product_success_ms = 0.0
                product_attempts = 0
                product_rate_limits = 0

            detected_at = datetime.now(timezone.utc)
            t_wh0 = time.perf_counter()
            delivered = await dispatcher.send_notification(
                product, [target], group_name="latency-probe", ts=detected_at
            )
            t_wh1 = time.perf_counter()
            webhook_ms = (t_wh1 - t_wh0) * 1000.0

            ok = await wait_receipt(ev, timeout=3.0)
            path_ms = (time.perf_counter() - path_started) * 1000.0

            row = {
                "rep": i,
                "delivered": delivered,
                "webhook_received": ok,
                "batch_total_ms": batch_total_ms,
                "batch_success_ms": batch_success_ms,
                "batch_attempts": batch_attempts,
                "batch_429s": batch_rate_limits,
                "product_total_ms": product_total_ms,
                "product_success_ms": product_success_ms,
                "product_attempts": product_attempts,
                "product_429s": product_rate_limits,
                "webhook_send_ms": webhook_ms,
                "poll_to_webhook_received_ms": path_ms if ok else None,
            }
            results.append(row)
            print(
                f"rep {i}: batch_total={row['batch_total_ms']:8.1f}ms "
                f"batch_success={row['batch_success_ms']:6.1f}ms "
                f"attempts={row['batch_attempts']} 429s={row['batch_429s']} "
                f"product_total={row['product_total_ms']:8.1f}ms "
                f"webhook={row['webhook_send_ms']:6.1f}ms "
                f"path={row['poll_to_webhook_received_ms']:.1f}ms "
                f"delivered={delivered} recv={ok}"
            )
            if receipts:
                try:
                    payload = json.loads(receipts[-1]["body"])
                    # Safe fields only
                    print(
                        f"       payload asin={payload.get('asin')} "
                        f"in_stock={payload.get('in_stock')} "
                        f"price={payload.get('price')} ts={payload.get('ts')}"
                    )
                except Exception:
                    pass

            await asyncio.sleep(SPACING)

        print()
        print("=== SUMMARY ===")
        if not results:
            print("No successful reps.")
            httpd.shutdown()
            return 1

        path_vals = [
            r["poll_to_webhook_received_ms"]
            for r in results
            if r["poll_to_webhook_received_ms"] is not None
        ]
        print(f"ASIN={asin}")
        print(f"latency_mode={LATENCY_MODE}")
        print(
            f"region_hint={os.getenv('RAILWAY_REPLICA_REGION', os.getenv('RAILWAY_ENVIRONMENT_NAME', ''))}"
        )
        print(
            "batch logical total:     "
            f"{stats_line([r['batch_total_ms'] for r in results])}"
        )
        print(
            "batch successful attempt:"
            f" {stats_line([r['batch_success_ms'] for r in results])}"
        )
        if LATENCY_MODE in ("confirm", "full", "two-hop"):
            print(
                "product logical total:   "
                f"{stats_line([r['product_total_ms'] for r in results])}"
            )
        print(
            f"webhook send:            {stats_line([r['webhook_send_ms'] for r in results])}"
        )
        path_label = (
            "detect path (batch+webhook)"
            if LATENCY_MODE not in ("confirm", "full", "two-hop")
            else "detect path (batch+product+webhook)"
        )
        print(f"{path_label}: {stats_line(path_vals)}")
        if path_vals:
            total_429 = sum(r["batch_429s"] + r["product_429s"] for r in results)
            print(
                f"README_NUMBERS mode={LATENCY_MODE} p50_ms={pct(path_vals, 50):.0f} "
                f"p95_ms={pct(path_vals, 95):.0f} n={len(path_vals)} "
                f"rate_limits={total_429}"
            )
        print()
        print(
            "Note: excludes poll-interval wait (0..POLL_INTERVAL). "
            "Retry and credential cooldown time are included in headline latency. "
            "Successful-attempt latency is diagnostic only. "
            "Local webhook RTT is near zero; Discord would add more."
        )

    httpd.shutdown()
    return 0


def main():
    # Refuse to run if someone wires this to an unexpected org repo by mistake.
    repo = os.getenv("GITHUB_REPOSITORY", "")
    if repo and repo != "Max17190/amazon-monitor":
        print(f"Refusing to run on unexpected repository: {repo}")
        raise SystemExit(3)
    raise SystemExit(asyncio.run(run_probe()))


if __name__ == "__main__":
    main()
