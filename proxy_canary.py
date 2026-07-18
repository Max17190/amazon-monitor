#!/usr/bin/env python3
"""Rank local proxy routes without TVSS, then optionally probe the best three."""

import argparse
import asyncio
import json
import os
import time

import aiohttp
from dotenv import load_dotenv

from amazon_tvss import TVSSClient, TVSSRateLimitError
from tvss_runtime import ProxyPool, load_proxy_urls


load_dotenv("endpoint.env")

HEALTH_URL = "https://api.ipify.org?format=json"


async def check_route(session, pool, route, timeout_seconds):
    started = time.perf_counter()
    try:
        async with session.get(
            HEALTH_URL,
            proxy=route.url,
            timeout=aiohttp.ClientTimeout(total=timeout_seconds),
        ) as response:
            await response.read()
            latency_ms = (time.perf_counter() - started) * 1000.0
            if response.status == 200:
                pool.record_success(route.route_id, latency_ms)
                return {
                    "route": route.route_id,
                    "healthy": True,
                    "latency_ms": round(latency_ms, 3),
                }
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass

    pool.record_failure(route.route_id, quarantine_seconds=60.0)
    return {
        "route": route.route_id,
        "healthy": False,
        "latency_ms": None,
    }


async def run(args):
    env = dict(os.environ)
    if args.proxy_file:
        env["PROXY_POOL_FILE"] = args.proxy_file
    proxy_urls = load_proxy_urls(env)
    pool = ProxyPool(proxy_urls)
    if not pool.has_proxies:
        raise RuntimeError("no proxy routes were configured")

    connector = aiohttp.TCPConnector(
        limit=pool.proxy_count,
        ttl_dns_cache=300,
        keepalive_timeout=120,
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        health = await asyncio.gather(
            *(
                check_route(session, pool, route, args.timeout_seconds)
                for route in pool.proxy_routes()
            )
        )

    for result in health:
        print(json.dumps({"stage": "proxy_health", **result}, sort_keys=True))

    ranked_ids = pool.ranked_route_ids()
    top_routes = [
        route
        for route_id in ranked_ids[:3]
        for route in pool.proxy_routes()
        if route.route_id == route_id
    ]
    print(
        json.dumps(
            {
                "stage": "proxy_summary",
                "configured": pool.proxy_count,
                "healthy": len(ranked_ids),
                "selected_routes": [route.route_id for route in top_routes],
            },
            sort_keys=True,
        )
    )

    if not args.tvss_confirm:
        return 0

    asins = [
        value.strip().upper()
        for value in args.asins.split(",")
        if len(value.strip()) == 10
    ][:20]
    if not asins:
        raise RuntimeError("at least one valid ASIN is required for TVSS checks")

    client = TVSSClient()
    tvss_connector = aiohttp.TCPConnector(
        limit=1,
        ttl_dns_cache=300,
        keepalive_timeout=120,
    )
    async with aiohttp.ClientSession(connector=tvss_connector) as session:
        for index, route in enumerate(top_routes):
            if index:
                await asyncio.sleep(args.tvss_spacing)
            client.proxy_pool = ProxyPool(
                [route.url],
                mode="always",
                allow_network_fallback=False,
            )
            started = time.perf_counter()
            try:
                await client.batch_products(session, asins)
                outcome = "ok"
            except TVSSRateLimitError:
                outcome = "rate_limited"
            except Exception:
                outcome = "error"
            latency_ms = (time.perf_counter() - started) * 1000.0
            print(
                json.dumps(
                    {
                        "stage": "proxy_tvss",
                        "route": route.route_id,
                        "outcome": outcome,
                        "latency_ms": round(latency_ms, 3),
                    },
                    sort_keys=True,
                )
            )
            if outcome == "rate_limited":
                break

    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--proxy-file",
        default=".firecrawl/webshare-proxies.txt",
    )
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--tvss-confirm", action="store_true")
    parser.add_argument("--tvss-spacing", type=float, default=10.0)
    parser.add_argument(
        "--asins",
        default="B0DT7L98J1,B0DTJFSSZG",
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
