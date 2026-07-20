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
from credential_governor import (
    CALIBRATION_REQUIRED_OBSERVATIONS,
    CalibrationKey,
    CalibrationSnapshot,
    PostgresCadenceCalibrationStore,
    stable_credential_key,
)
from durable_runtime import exclusive_canary_lease


load_dotenv("endpoint.env")

INTERVALS = (5.0, 3.0, 2.0, 1.5, 1.0, 0.75, 0.5)
DISCOVERY_SECONDS = 60.0
QUIET_SECONDS = 900.0
VALIDATION_OBSERVATIONS = CALIBRATION_REQUIRED_OBSERVATIONS
DIRECT_P50_BASELINE_MS = 109.0
MAX_P50_MS = DIRECT_P50_BASELINE_MS * 1.10
MAX_P95_MS = 223.7


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


def calibration_key_for_client(client, asins, env=None):
    """Build a non-secret, route-bound calibration key for a direct canary."""
    env = os.environ if env is None else env
    configured_id = str(env.get("TVSS_CREDENTIAL_ID", "")).strip()
    identity = configured_id or client.device_udid
    if not identity or (not configured_id and not client.has_stable_device_identity):
        raise TVSSConfigError(
            "TVSS_CREDENTIAL_ID or a stable TVSS device identity is required"
        )
    return CalibrationKey(
        credential_key=stable_credential_key(
            f"{client.marketplace_id}:{identity}",
            salt=str(env.get("TVSS_CREDENTIAL_SALT", "")),
        ),
        marketplace_id=client.marketplace_id,
        region=str(env.get("RAILWAY_REPLICA_REGION", "local")),
        direct_route=True,
        batch_size=len(asins),
    )


def calibration_summary(key, validation, *, validated_at=None):
    """Return a JSON-safe record central runtime wiring can persist verbatim."""
    snapshot = CalibrationSnapshot(
        key=key,
        interval_seconds=float(validation["interval_seconds"]),
        clean_observations=int(validation["observations"]),
        rate_limit_count=int(validation["outcome"] == "rate_limited"),
        network_error_count=int(validation["outcome"] == "error"),
        validated_at=time.time() if validated_at is None else float(validated_at),
    )
    return {
        "credential_hash": key.credential_key,
        "marketplace_id": key.marketplace_id,
        "region": key.region,
        "direct_route": key.direct_route,
        "batch_size": key.batch_size,
        "interval_seconds": snapshot.interval_seconds,
        "clean_observations": snapshot.clean_observations,
        "rate_limit_count": snapshot.rate_limit_count,
        "network_error_count": snapshot.network_error_count,
        "valid": snapshot.is_valid(snapshot.validated_at),
    }


def enforce_direct_route(client):
    """Disable both recovery and network-failure proxy fallback for a canary."""
    client.disable_proxy_fallback()
    client.proxy_pool.allow_network_fallback = False
    if not client.proxy_pool.primary_route.is_direct:
        raise TVSSConfigError(
            "cadence calibration requires direct TVSS routing; set PROXY_MODE=fallback or direct"
        )


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


async def _run_canary(args, client, calibration_store=None):
    asins = parse_asins(args.asins)
    if not asins:
        raise TVSSConfigError("at least one valid ASIN is required")

    # Calibration is direct-only.  Proxies must not create a separate request
    # budget or be used to validate a production sub-five-second cadence.
    enforce_direct_route(client)
    calibration_key = calibration_key_for_client(client, asins)
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
                if calibration_store is not None:
                    await calibration_store.invalidate_credential(
                        calibration_key.credential_key,
                        calibration_key.marketplace_id,
                    )
                print(
                    json.dumps(
                        {
                            "stage": "summary",
                            "accepted": False,
                            "reason": "rate limit invalidated calibration",
                        },
                        sort_keys=True,
                    )
                )
                return 1

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
            if validation["outcome"] == "rate_limited":
                if calibration_store is not None:
                    await calibration_store.invalidate_credential(
                        calibration_key.credential_key,
                        calibration_key.marketplace_id,
                    )
                break
            if validation["outcome"] == "clean":
                break
            break

    accepted = (
        selected is not None
        and validation["outcome"] == "clean"
        and validation["observations"] == args.validation_observations
        and args.validation_observations >= CALIBRATION_REQUIRED_OBSERVATIONS
        and validation["p50_ms"] <= MAX_P50_MS
        and validation["p95_ms"] <= MAX_P95_MS
        and all(
            row["outcome"] == "clean"
            for row in discovery + validation_history
        )
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
        "max_allowed_p95_ms": MAX_P95_MS,
        "direct_route": True,
        "calibration": calibration_summary(calibration_key, validation),
    }
    print(json.dumps({"stage": "summary", **summary}, sort_keys=True))
    if calibration_store is not None:
        if accepted:
            await calibration_store.record_validation(
                calibration_key,
                validation["interval_seconds"],
                validation["observations"],
                rate_limit_count=0,
                network_error_count=0,
            )
    return 0 if accepted else 1


async def run(args):
    client = TVSSClient()
    async with exclusive_canary_lease(
        client,
        base_interval=min(INTERVALS),
        calibration=True,
    ) as lease:
        calibration_store = PostgresCadenceCalibrationStore(
            lease["store"].pool
        )
        await calibration_store.initialize()
        return await _run_canary(args, client, calibration_store)


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
