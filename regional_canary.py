#!/usr/bin/env python3
"""Validate one selected cadence in a second Railway region."""

import argparse
import asyncio
import json
import os

import aiohttp
from dotenv import load_dotenv

from amazon_tvss import TVSSClient
from cadence_canary import MAX_P50_MS, parse_asins, validate


load_dotenv("endpoint.env")


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
    connector = aiohttp.TCPConnector(
        limit=0,
        ttl_dns_cache=300,
        keepalive_timeout=120,
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        result = await validate(
            client,
            session,
            asins,
            args.interval,
            args.observations,
        )

    accepted = (
        result["outcome"] == "clean"
        and result["observations"] == args.observations
        and result["p50_ms"] <= MAX_P50_MS
    )
    print(
        json.dumps(
            {
                "stage": "regional_validation",
                "accepted": accepted,
                "region": os.getenv("RAILWAY_REPLICA_REGION", "local"),
                "max_allowed_p50_ms": MAX_P50_MS,
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
    parser.add_argument("--observations", type=int, default=60)
    args = parser.parse_args()
    if not args.confirm:
        parser.error("--confirm is required because this canary uses live credentials")
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
