#!/usr/bin/env python3
"""Probe the TVSS basicproducts batch endpoint.

Compares the response shape of:
  GET /marketplaces/{id}/products/{asin}            (single, full detail)
  GET /marketplaces/{id}/basicproducts/{a1,a2,...}   (batch, lighter)

Run:
  1. Create endpoint.env with your TVSS_COOKIE_HEADER (and TVSS_ACCESS_TOKEN
     if at-main is not in the cookie).
  2. Set TEST_ASINS below to real ASINs — ideally one in-stock and one OOS.
  3. python test_batch_endpoint.py

Output: side-by-side key comparison showing which fields each endpoint
returns, so we can determine whether basicproducts carries a usable
stock signal.
"""

import asyncio
import json
import os
import sys

import aiohttp
from dotenv import load_dotenv

load_dotenv("endpoint.env")

# ── Configure these ──────────────────────────────────────────────────
# Put at least one ASIN you know is in-stock and one you know is OOS.
TEST_ASINS = os.getenv("TEST_ASINS", "B0DT7L98J1,B0DTJFSSZG").split(",")
# ─────────────────────────────────────────────────────────────────────

from amazon_tvss import TVSSClient, TVSSConfigError


async def fetch_single(client, session, asin):
    """Hit the full /products/{asin} endpoint."""
    url = f"{client._product_url_prefix}{asin}?sif_profile=tvss"
    return await client._request(session, "GET", url)


async def fetch_batch(client, session, asins):
    """Hit the /basicproducts/{a1,a2,...} batch endpoint."""
    joined = ",".join(asins)
    url = (
        f"{client.base_url}/marketplaces/{client.marketplace_id}"
        f"/basicproducts/{joined}?get-deals=false&sif_profile=tvss"
    )
    return await client._request(session, "GET", url)


def deep_keys(obj, prefix=""):
    """Recursively collect all keys in a dict/list structure."""
    keys = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            full = f"{prefix}.{k}" if prefix else k
            keys.add(full)
            keys |= deep_keys(v, full)
    elif isinstance(obj, list) and obj:
        keys |= deep_keys(obj[0], f"{prefix}[]")
    return keys


STOCK_FIELDS = [
    "productAvailabilityDetails",
    "offerId",
    "offerListingId",
    "buyingOptionId",
    "canAddToCart",
    "isBuyable",
    "buyable",
    "available",
    "merchantInfo",
    "merchantInfo.soldByAmazon",
    "merchantInfo.merchantName",
]


async def main():
    try:
        client = TVSSClient()
    except TVSSConfigError as exc:
        print(f"ERROR: {exc}")
        print("Create endpoint.env with TVSS_COOKIE_HEADER to run this test.")
        sys.exit(1)

    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=120)
    async with aiohttp.ClientSession(connector=connector) as session:

        # ── Single product requests ──────────────────────────────────
        print(f"=== SINGLE PRODUCT ENDPOINT (/products/{{asin}}) ===\n")
        singles = {}
        for asin in TEST_ASINS:
            asin = asin.strip()
            try:
                data = await fetch_single(client, session, asin)
                singles[asin] = data
                print(f"  {asin}: HTTP OK, {len(json.dumps(data))} bytes")
                # Print stock-relevant fields
                for field in STOCK_FIELDS:
                    parts = field.split(".")
                    val = data
                    for p in parts:
                        if isinstance(val, dict):
                            val = val.get(p)
                        else:
                            val = None
                            break
                    if val is not None:
                        print(f"    {field} = {json.dumps(val)[:120]}")
                print()
            except Exception as exc:
                print(f"  {asin}: FAILED — {exc}\n")

        # ── Batch request ────────────────────────────────────────────
        clean_asins = [a.strip() for a in TEST_ASINS]
        print(f"=== BATCH ENDPOINT (/basicproducts/{','.join(clean_asins)}) ===\n")
        try:
            batch_data = await fetch_batch(client, session, clean_asins)
            print(f"  HTTP OK, {len(json.dumps(batch_data))} bytes")
            print(f"  Response type: {type(batch_data).__name__}")
            print()

            # If it's a list, iterate items
            items = batch_data if isinstance(batch_data, list) else [batch_data]
            for i, item in enumerate(items):
                if not isinstance(item, dict):
                    print(f"  Item {i}: {type(item).__name__} — {str(item)[:200]}")
                    continue

                asin = (
                    item.get("asin")
                    or item.get("basicProduct", {}).get("asin")
                    or item.get("ASIN")
                    or f"item-{i}"
                )
                print(f"  --- {asin} ---")

                # Print ALL top-level keys
                print(f"  Top-level keys: {sorted(item.keys())}")

                # Check for stock-relevant fields
                found_any = False
                for field in STOCK_FIELDS:
                    parts = field.split(".")
                    val = item
                    for p in parts:
                        if isinstance(val, dict):
                            val = val.get(p)
                        else:
                            val = None
                            break
                    if val is not None:
                        print(f"    {field} = {json.dumps(val)[:120]}")
                        found_any = True

                if not found_any:
                    print(f"    (no stock-relevant fields found)")

                # Print all keys for analysis
                all_keys = deep_keys(item)
                print(f"  All nested keys ({len(all_keys)}):")
                for k in sorted(all_keys):
                    print(f"    {k}")
                print()

        except Exception as exc:
            print(f"  BATCH FAILED — {exc}")
            print()
            import traceback
            traceback.print_exc()

        # ── Comparison ───────────────────────────────────────────────
        print("=== COMPARISON ===\n")
        if singles:
            first_asin = list(singles.keys())[0]
            single_keys = deep_keys(singles[first_asin])
            print(f"  Single /products/{first_asin}: {len(single_keys)} nested keys")
        else:
            print("  (no successful single requests)")

        print()
        print("If basicproducts contains ANY of these fields, a hybrid")
        print("batch-then-confirm strategy is viable:")
        for f in STOCK_FIELDS:
            print(f"  - {f}")


if __name__ == "__main__":
    asyncio.run(main())
