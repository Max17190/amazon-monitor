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
from main import AlertDispatcher, AlertState  # noqa: E402
from webhooks import WebhookTarget  # noqa: E402


DEFAULT_ASINS = (
    "B00FLYWNYQ",  # Instant Pot Duo (often Amazon in-stock)
    "B07FZ8S74R",
    "B0DT7L98J1",
)
REPS = int(os.getenv("LATENCY_REPS", "5"))
PORT = int(os.getenv("LATENCY_WEBHOOK_PORT", "18765"))
SPACING = float(os.getenv("LATENCY_SPACING", "1.2"))

receipts = []
receipt_event = None


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
        f"min={min(vals):.1f} p50={pct(vals, 50):.1f} "
        f"mean={statistics.mean(vals):.1f} max={max(vals):.1f} ms (n={len(vals)})"
    )


async def wait_receipt(ev, timeout=5.0):
    try:
        await asyncio.wait_for(ev.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        return False


async def pick_in_stock_asin(client, session, candidates):
    for asin in candidates:
        try:
            product = await client.product(session, asin)
        except Exception as exc:
            print(f"candidate {asin}: error {type(exc).__name__}")
            continue
        in_stock = bool(product.get("in_stock"))
        sold = product.get("soldByAmazon")
        title = (product.get("title") or "")[:70]
        print(
            f"candidate {asin}: in_stock={in_stock} soldByAmazon={sold} "
            f"price={product.get('price')} title={title!r}"
        )
        if in_stock and sold is not False:
            return asin, product
        await asyncio.sleep(0.4)
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
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300, keepalive_timeout=60)
    target = WebhookTarget(
        name="LOCAL",
        url=f"http://127.0.0.1:{PORT}/restock",
        kind="generic",
    )

    print("=== Live E2E restock-path latency probe ===")
    print(f"host={socket.gethostname()}")
    print(f"runner_name={os.getenv('RUNNER_NAME', '')}")
    print(f"github_repository={os.getenv('GITHUB_REPOSITORY', '')}")
    print(f"github_run_id={os.getenv('GITHUB_RUN_ID', '')}")
    print(f"tvss_base={client.base_url}")
    print(f"marketplace={client.marketplace_id}")
    print(f"reps={REPS}")
    print()

    # Network floor (no secrets)
    try:
        tcp_times = []
        for _ in range(5):
            t0 = time.perf_counter()
            s = socket.create_connection(("tvss.amazon.com", 443), timeout=5)
            tcp_times.append((time.perf_counter() - t0) * 1000)
            s.close()
        print(
            f"TCP connect tvss.amazon.com: p50={pct(tcp_times, 50):.1f}ms "
            f"min={min(tcp_times):.1f} max={max(tcp_times):.1f}"
        )
    except Exception as exc:
        print(f"TCP connect probe failed: {type(exc).__name__}")
    print()

    async with aiohttp.ClientSession(connector=connector) as session:
        # Warmup + choose ASIN
        try:
            await client.batch_products(session, [candidates[0]])
        except Exception as exc:
            print(f"warmup error: {type(exc).__name__}: {exc}")
        await asyncio.sleep(0.3)

        asin, live = await pick_in_stock_asin(client, session, candidates)
        if not asin:
            print("ABORT: no in-stock Amazon-sold candidate among", candidates)
            httpd.shutdown()
            return 2

        batch_check = await client.batch_products(session, [asin])
        print(f"using ASIN={asin}")
        print(f"batch has_offer={batch_check.get(asin)}")
        print(
            f"live in_stock={live.get('in_stock')} soldByAmazon={live.get('soldByAmazon')} "
            f"price={live.get('price')}"
        )
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

            t0 = time.perf_counter()

            t_batch0 = time.perf_counter()
            batch_result = await client.batch_products(session, [asin])
            t_batch1 = time.perf_counter()
            has_offer = batch_result.get(asin, {}).get("has_offer", False)
            if not has_offer:
                print(f"rep {i}: batch has_offer=False; skip")
                await asyncio.sleep(SPACING)
                continue

            t_prod0 = time.perf_counter()
            product = await client.product(session, asin)
            t_prod1 = time.perf_counter()

            if not product.get("in_stock"):
                print(f"rep {i}: product in_stock=False; skip")
                await asyncio.sleep(SPACING)
                continue
            if product.get("soldByAmazon") is False:
                print(f"rep {i}: third-party only; would filter")
                await asyncio.sleep(SPACING)
                continue

            detected_at = datetime.now(timezone.utc)
            t_wh0 = time.perf_counter()
            delivered = await dispatcher.send_notification(
                product, [target], group_name="latency-probe", ts=detected_at
            )
            t_wh1 = time.perf_counter()

            ok = await wait_receipt(ev, timeout=3.0)
            t_recv = receipts[-1]["t_recv"] if receipts else None

            row = {
                "rep": i,
                "delivered": delivered,
                "webhook_received": ok,
                "batch_ms": (t_batch1 - t_batch0) * 1000,
                "product_ms": (t_prod1 - t_prod0) * 1000,
                "webhook_send_ms": (t_wh1 - t_wh0) * 1000,
                "poll_to_webhook_received_ms": (
                    (t_recv - t0) * 1000 if t_recv is not None else None
                ),
            }
            results.append(row)
            print(
                f"rep {i}: batch={row['batch_ms']:6.1f}ms "
                f"product={row['product_ms']:6.1f}ms "
                f"webhook={row['webhook_send_ms']:6.1f}ms "
                f"POLL→WEBHOOK_RECV={row['poll_to_webhook_received_ms']} "
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

        print(f"ASIN={asin}")
        print(f"batch_products:          {stats_line([r['batch_ms'] for r in results])}")
        print(f"product() confirm:       {stats_line([r['product_ms'] for r in results])}")
        print(
            f"webhook send:            {stats_line([r['webhook_send_ms'] for r in results])}"
        )
        print(
            "POLL start → webhook RX: "
            + stats_line(
                [
                    r["poll_to_webhook_received_ms"]
                    for r in results
                    if r["poll_to_webhook_received_ms"] is not None
                ]
            )
        )
        print()
        print(
            "Note: excludes poll-interval wait (0..POLL_INTERVAL). "
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
