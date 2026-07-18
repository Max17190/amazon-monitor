#!/usr/bin/env python3
"""Run the bounded direct TVSS cadence ladder and validation canary."""

import argparse
import asyncio
import json
import math
import os
import time

import aiohttp
from dotenv import load_dotenv

from amazon_tvss import TVSSClient, TVSSConfigError, TVSSRateLimitError
from durable_runtime import exclusive_canary_lease


load_dotenv("endpoint.env")

INTERVALS = (5.0, 3.0, 2.0, 1.5, 1.0, 0.75, 0.5)
DISCOVERY_SECONDS = 60.0
QUIET_SECONDS = 900.0
VALIDATION_OBSERVATIONS = 60
DIRECT_P50_BASELINE_MS = 109.0
MAX_P50_MS = DIRECT_P50_BASELINE_MS * 1.10


def parse_asins(raw_value):
    result = []
    for value in raw_value.replace(",", " ").split():
        asin = value.strip().upper()
        if len(asin) == 10 and asin.isalnum() and asin not in result:
            result.append(asin)
    return result[:20]


def percentile(values, percentile_value):
    ordered = sorted(values)
    index = round((percentile_value / 100.0) * (len(ordered) - 1))
    return ordered[max(0, min(len(ordered) - 1, index))]


async def sleep_until(deadline):
    delay = deadline - time.perf_counter()
    if delay > 0:
        await asyncio.sleep(delay)


def advance_deadline(previous_deadline, interval, now=None):
    now = time.perf_counter() if now is None else float(now)
    interval = float(interval)
    if interval <= 0:
        return now

    next_deadline = previous_deadline + interval
    if next_deadline < now:
        elapsed_slots = math.ceil((now - next_deadline) / interval)
        next_deadline += elapsed_slots * interval
    return next_deadline


async def probe_once(client, session, asins):
    started = time.perf_counter()
    try:
        await client.batch_products(session, asins)
        return "ok", (time.perf_counter() - started) * 1000.0
    except TVSSRateLimitError:
        return "429", (time.perf_counter() - started) * 1000.0
    except Exception:
        return "error", (time.perf_counter() - started) * 1000.0


async def run_bucket(client, session, asins, interval, duration):
    started = time.perf_counter()
    deadline = started + duration
    next_start = started
    latencies = []
    outcome = "clean"

    while next_start < deadline:
        await sleep_until(next_start)
        status, latency_ms = await probe_once(client, session, asins)
        if status == "429":
            outcome = "rate_limited"
            break
        if status != "ok":
            outcome = "error"
            break
        latencies.append(latency_ms)
        next_start = advance_deadline(next_start, interval)

    return {
        "interval_seconds": interval,
        "outcome": outcome,
        "observations": len(latencies),
        "p50_ms": percentile(latencies, 50) if latencies else None,
        "p95_ms": percentile(latencies, 95) if latencies else None,
    }


async def validate(client, session, asins, interval, observations):
    next_start = time.perf_counter()
    latencies = []
    for _ in range(observations):
        await sleep_until(next_start)
        status, latency_ms = await probe_once(client, session, asins)
        if status != "ok":
            return {
                "interval_seconds": interval,
                "outcome": "rate_limited" if status == "429" else "error",
                "observations": len(latencies),
                "p50_ms": percentile(latencies, 50) if latencies else None,
                "p95_ms": percentile(latencies, 95) if latencies else None,
            }
        latencies.append(latency_ms)
        next_start = advance_deadline(next_start, interval)

    return {
        "interval_seconds": interval,
        "outcome": "clean",
        "observations": len(latencies),
        "p50_ms": percentile(latencies, 50),
        "p95_ms": percentile(latencies, 95),
    }


async def _run_canary(args, client):
    asins = parse_asins(args.asins)
    if not asins:
        raise TVSSConfigError("at least one valid ASIN is required")

    connector = aiohttp.TCPConnector(
        limit=0,
        ttl_dns_cache=300,
        keepalive_timeout=120,
    )
    discovery = []

    async with aiohttp.ClientSession(connector=connector) as session:
        for interval in INTERVALS:
            result = await run_bucket(
                client,
                session,
                asins,
                interval,
                args.discovery_seconds,
            )
            discovery.append(result)
            print(json.dumps({"stage": "discovery", **result}, sort_keys=True))
            if result["outcome"] == "rate_limited":
                break

        clean = [row for row in discovery if row["outcome"] == "clean"]
        if not clean:
            print(
                json.dumps(
                    {
                        "accepted": False,
                        "reason": "no clean discovery interval",
                    },
                    sort_keys=True,
                )
            )
            return 1

        selected = min(row["interval_seconds"] for row in clean)
        validation_history = []
        while selected is not None:
            print(
                json.dumps(
                    {
                        "stage": "quiet",
                        "seconds": args.quiet_seconds,
                        "candidate_interval_seconds": selected,
                    },
                    sort_keys=True,
                )
            )
            await asyncio.sleep(args.quiet_seconds)
            client.disable_proxy_fallback()

            validation = await validate(
                client,
                session,
                asins,
                selected,
                args.validation_observations,
            )
            validation_history.append(validation)
            print(
                json.dumps(
                    {"stage": "validation", **validation},
                    sort_keys=True,
                )
            )
            if validation["outcome"] == "clean":
                break
            if validation["outcome"] != "rate_limited":
                break
            slower_intervals = [
                interval for interval in INTERVALS if interval > selected
            ]
            selected = min(slower_intervals) if slower_intervals else None

    accepted = (
        selected is not None
        and validation["outcome"] == "clean"
        and validation["observations"] == args.validation_observations
        and validation["p50_ms"] <= MAX_P50_MS
    )
    summary = {
        "accepted": accepted,
        "region": os.getenv("RAILWAY_REPLICA_REGION", "local"),
        "selected_interval_seconds": selected,
        "validated_interval_seconds": validation["interval_seconds"],
        "validation_429s": sum(
            row["outcome"] == "rate_limited" for row in validation_history
        ),
        "validation_observations": validation["observations"],
        "p50_ms": validation["p50_ms"],
        "p95_ms": validation["p95_ms"],
        "max_allowed_p50_ms": MAX_P50_MS,
    }
    print(json.dumps({"stage": "summary", **summary}, sort_keys=True))
    return 0 if accepted else 1


async def run(args):
    client = TVSSClient()
    async with exclusive_canary_lease(
        client,
        base_interval=min(INTERVALS),
        calibration=True,
    ):
        return await _run_canary(args, client)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument(
        "--asins",
        default="B0DT7L98J1,B0DTJFSSZG",
        help="Comma-separated ASINs, capped at 20.",
    )
    parser.add_argument(
        "--discovery-seconds",
        type=float,
        default=DISCOVERY_SECONDS,
    )
    parser.add_argument("--quiet-seconds", type=float, default=QUIET_SECONDS)
    parser.add_argument(
        "--validation-observations",
        type=int,
        default=VALIDATION_OBSERVATIONS,
    )
    args = parser.parse_args()
    if not args.confirm:
        parser.error("--confirm is required because this canary uses live credentials")
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
