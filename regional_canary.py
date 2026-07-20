#!/usr/bin/env python3
"""Validate one selected cadence in a second Railway region."""

import argparse
import asyncio
import json
import os
import time
from uuid import uuid4

import aiohttp
from dotenv import load_dotenv

from amazon_tvss import TVSSClient
from cadence_canary import (
    MAX_P50_MS,
    MAX_P95_MS,
    calibration_key_for_client,
    calibration_summary,
    enforce_direct_route,
    parse_asins,
    percentile,
    validate,
)
from durable_runtime import exclusive_canary_lease
from credential_governor import (
    CALIBRATION_REQUIRED_OBSERVATIONS,
    PostgresCadenceCalibrationStore,
)


load_dotenv("endpoint.env")


async def measure_postgres_commit(pool, observations=20):
    latencies = []
    monitor_id = f"regional-canary-{uuid4()}"
    async with pool.acquire() as connection:
        try:
            for index in range(observations):
                started = time.perf_counter()
                async with connection.transaction():
                    await connection.execute(
                        """
                        INSERT INTO product_states (
                            monitor_id, marketplace_id, asin,
                            seller_policy_hash
                        ) VALUES ($1, 'regional-canary', $2, 'latency')
                        """,
                        monitor_id,
                        f"CANARY{index:04d}",
                    )
                latencies.append(
                    (time.perf_counter() - started) * 1000.0
                )
        finally:
            await connection.execute(
                "DELETE FROM product_states WHERE monitor_id=$1",
                monitor_id,
            )
    return percentile(latencies, 95)


async def measure_http_latency(session, url, *, method, observations=10):
    if not url:
        return None
    latencies = []
    for index in range(observations):
        started = time.perf_counter()
        kwargs = {}
        if method == "POST":
            kwargs["json"] = {
                "type": "regional_canary",
                "sequence": index,
            }
        async with session.request(
            method,
            url,
            timeout=aiohttp.ClientTimeout(total=3),
            **kwargs,
        ) as response:
            response.release()
            if response.status >= 400:
                raise RuntimeError(
                    f"regional latency endpoint returned HTTP {response.status}"
                )
        latencies.append((time.perf_counter() - started) * 1000.0)
    return percentile(latencies, 95)


async def run(args):
    asins = parse_asins(args.asins)
    if not asins:
        raise RuntimeError("at least one valid ASIN is required")

    print(
        json.dumps(
            {
                "stage": "regional_quiet",
                "seconds": args.quiet_seconds,
                "interval_seconds": args.interval,
            },
            sort_keys=True,
        )
    )
    await asyncio.sleep(args.quiet_seconds)

    client = TVSSClient()
    enforce_direct_route(client)
    calibration_key = calibration_key_for_client(client, asins)
    connector = aiohttp.TCPConnector(
        limit=0,
        ttl_dns_cache=300,
        keepalive_timeout=120,
    )
    async with exclusive_canary_lease(
        client,
        base_interval=args.interval,
        calibration=True,
    ) as lease:
        async with aiohttp.ClientSession(connector=connector) as session:
            result = await validate(
                client,
                session,
                asins,
                args.interval,
                args.observations,
            )
            calibration_store = PostgresCadenceCalibrationStore(
                lease["store"].pool
            )
            if result["outcome"] == "rate_limited":
                await calibration_store.invalidate_credential(
                    calibration_key.credential_key,
                    calibration_key.marketplace_id,
                )
            postgres_commit_p95 = await measure_postgres_commit(
                lease["store"].pool
            )
            webhook_connect_p95 = await measure_http_latency(
                session,
                str(os.getenv("REGIONAL_CANARY_WEBHOOK_WARMUP_URL", "")).strip(),
                method="GET",
            )
            detect_to_accept_p95 = await measure_http_latency(
                session,
                str(os.getenv("REGIONAL_CANARY_ACCEPTANCE_URL", "")).strip(),
                method="POST",
            )

        accepted = (
            result["outcome"] == "clean"
            and result["observations"] == args.observations
            and args.observations >= CALIBRATION_REQUIRED_OBSERVATIONS
            and result["p50_ms"] <= MAX_P50_MS
            and result["p95_ms"] <= MAX_P95_MS
        )
        if accepted:
            await calibration_store.record_validation(
                calibration_key,
                result["interval_seconds"],
                result["observations"],
            )
    print(
        json.dumps(
            {
                "stage": "regional_validation",
                "accepted": accepted,
                "region": os.getenv("RAILWAY_REPLICA_REGION", "local"),
                "database_region": os.getenv("DATABASE_REGION", "unknown"),
                "direct_route": True,
                "max_allowed_p50_ms": MAX_P50_MS,
                "max_allowed_p95_ms": MAX_P95_MS,
                "postgres_commit_p95_ms": postgres_commit_p95,
                "webhook_connect_p95_ms": webhook_connect_p95,
                "detect_to_accept_p95_ms": detect_to_accept_p95,
                "calibration": calibration_summary(calibration_key, result),
                **result,
            },
            sort_keys=True,
        )
    )
    return 0 if accepted else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--asins", required=True)
    parser.add_argument("--interval", type=float, required=True)
    parser.add_argument("--quiet-seconds", type=float, default=900.0)
    parser.add_argument(
        "--observations",
        type=int,
        default=CALIBRATION_REQUIRED_OBSERVATIONS,
    )
    args = parser.parse_args()
    if not args.confirm:
        parser.error("--confirm is required because this canary uses live credentials")
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
