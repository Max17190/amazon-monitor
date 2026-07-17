#!/usr/bin/env python3
"""Calibrate TVSS chunk size and polling interval against your cookies.

Phase A: latency-vs-batch-size sweep up to the 50-ASIN hard cap.
Phase B: throughput probe at a range of intervals; aborts a bucket on the
         first 3 consecutive 429s so a failing setting does not burn cookies.

Output: paste-ready endpoint.env lines for TVSS_BATCH_CHUNK_SIZE,
TVSS_BATCH_CONCURRENCY, and POLL_INTERVAL_SECONDS.

Usage:
  python benchmark.py --confirm --asins B0DT7L98J1,B0DTJFSSZG,...
"""

import argparse
import asyncio
import os
import random
import string
import sys
import time

import aiohttp
from dotenv import load_dotenv

load_dotenv("endpoint.env")

from amazon_tvss import TVSSClient, TVSSConfigError


PHASE_A_SIZES = [1, 10, 25, 50]
PHASE_A_REPS = 3
PHASE_A_SPACING = 5.0           # seconds between Phase A probes

PHASE_B_INTERVALS = [5.0, 3.0, 2.0, 1.5, 1.0]   # slow → fast
PHASE_B_DURATION = 60.0         # seconds per bucket
PHASE_B_BUCKET_COOLDOWN = 30.0  # silent cooldown between buckets
PHASE_B_ABORT_429 = 3           # consecutive 429s to abort a bucket


def synthetic(n, seed):
    rng = random.Random(seed)
    seen = set()
    out = []
    while len(out) < n:
        s = "B0" + "".join(rng.choices(string.ascii_uppercase + string.digits, k=8))
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def classify_error(exc):
    msg = str(exc)
    if "rate limited" in msg or "429" in msg:
        return "429"
    if "400" in msg:
        return "400"
    return "err"


async def probe_once(client, session, asins):
    t0 = time.perf_counter()
    try:
        await client.batch_products(session, asins)
        return ("ok", (time.perf_counter() - t0) * 1000.0, None)
    except Exception as exc:
        return (
            classify_error(exc),
            (time.perf_counter() - t0) * 1000.0,
            str(exc)[:80],
        )


async def phase_a(client, session, base_asins, sizes, reps, spacing):
    print(f"\n=== Phase A: latency vs batch size (sizes={sizes}, reps={reps}) ===")
    results = {}
    for size in sizes:
        if size > 50:
            print(f"  size={size}: skipped (>50 hard cap)")
            continue
        # Pad with synthetic ASINs so URL+parsing path is exercised at full size.
        if len(base_asins) >= size:
            asins = base_asins[:size]
        else:
            asins = list(base_asins) + synthetic(size - len(base_asins), seed=size)
        latencies = []
        errors = 0
        for r in range(reps):
            status, latency_ms, err_msg = await probe_once(client, session, asins)
            if status == "ok":
                latencies.append(latency_ms)
                marker = "OK"
            else:
                errors += 1
                marker = status.upper()
            print(
                f"  size={size:3d} rep={r + 1}/{reps} {marker:>4} "
                f"latency={latency_ms:5.0f}ms"
                + (f"  ({err_msg})" if err_msg else "")
            )
            await asyncio.sleep(spacing)
        if latencies:
            latencies.sort()
            p50 = latencies[len(latencies) // 2]
            p95 = latencies[max(0, int(len(latencies) * 0.95) - 1)]
            results[size] = {"p50": p50, "p95": p95, "errors": errors, "n": len(latencies)}
        else:
            results[size] = {"p50": None, "p95": None, "errors": errors, "n": 0}
    return results


async def phase_b(client, session, asins_chunk, intervals, duration, cooldown, abort_429):
    print(
        f"\n=== Phase B: throughput probe (intervals={intervals}, "
        f"duration={duration}s/bucket) ==="
    )
    results = {}
    for iv in intervals:
        await asyncio.sleep(cooldown)
        print(f"\n  Bucket interval={iv}s — running for up to {duration}s")
        n_ok = n_429 = n_other = 0
        consecutive_429 = 0
        latencies = []
        aborted = False
        deadline = time.perf_counter() + duration
        while time.perf_counter() < deadline:
            status, latency_ms, _ = await probe_once(client, session, asins_chunk)
            if status == "ok":
                n_ok += 1
                consecutive_429 = 0
                latencies.append(latency_ms)
            elif status == "429":
                n_429 += 1
                consecutive_429 += 1
                if consecutive_429 >= abort_429:
                    print(
                        f"    abort: {abort_429} consecutive 429s at iv={iv}s; "
                        "this interval is too aggressive"
                    )
                    aborted = True
                    break
            else:
                n_other += 1
                consecutive_429 = 0
            sleep = iv * (1 + random.uniform(-0.15, 0.15))
            await asyncio.sleep(sleep)
        total = n_ok + n_429 + n_other
        rate_429 = (100.0 * n_429 / total) if total else 0.0
        latencies.sort()
        p50 = latencies[len(latencies) // 2] if latencies else None
        p95 = latencies[max(0, int(len(latencies) * 0.95) - 1)] if latencies else None
        results[iv] = {
            "n": total,
            "n_ok": n_ok,
            "n_429": n_429,
            "n_other": n_other,
            "rate_429": rate_429,
            "p50": p50,
            "p95": p95,
            "aborted": aborted,
        }
        print(
            f"    iv={iv}s: total={total} ok={n_ok} 429={n_429}({rate_429:.1f}%) "
            f"other={n_other} p50={p50}ms p95={p95}ms"
            + (" ABORTED" if aborted else "")
        )
    return results


def recommend(phase_a_result, phase_b_result):
    # Pick largest size with no errors AND tail not blown up (p95 < 1.5×p50)
    chunk_size = 50
    candidates = []
    for size, r in sorted(phase_a_result.items()):
        if r["n"] == 0:
            continue
        if r["errors"] > 0:
            continue
        if r["p50"] is None or r["p95"] is None:
            continue
        if r["p95"] >= 1.5 * r["p50"] and r["n"] >= 3:
            continue
        candidates.append(size)
    if candidates:
        chunk_size = max(candidates)

    # Pick smallest interval where 429 rate < 1% AND not aborted
    interval = max(phase_b_result.keys()) if phase_b_result else 2.0
    healthy = [
        iv
        for iv, r in phase_b_result.items()
        if not r["aborted"] and r["n"] > 0 and r["rate_429"] < 1.0
    ]
    if healthy:
        interval = min(healthy)

    # Concurrency: keep at 1 unless future calibration extends to it.
    concurrency = 1
    return chunk_size, concurrency, interval


def estimate_budget(n_base, sizes, reps, intervals, duration):
    a = sum(reps for s in sizes if s <= 50)
    b = sum(int(duration / iv) for iv in intervals)
    return a + b


def parse_asins(raw):
    out = []
    for tok in raw.replace(",", " ").split():
        tok = tok.strip().upper()
        if len(tok) == 10 and tok.isalnum():
            out.append(tok)
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required. Confirms you understand the request budget against your cookies.",
    )
    parser.add_argument(
        "--asins",
        default="B0DT7L98J1,B0DTJFSSZG",
        help="Comma-separated test ASINs (real, ideally a mix of in-stock and OOS).",
    )
    parser.add_argument(
        "--max-asins",
        type=int,
        default=10,
        help="Cap on test fleet size (synthetic padding fills the rest).",
    )
    args = parser.parse_args()

    base_asins = parse_asins(args.asins)
    if not base_asins:
        print("ERROR: --asins must contain at least one valid 10-character ASIN.")
        sys.exit(2)
    base_asins = base_asins[: args.max_asins]

    budget = estimate_budget(
        len(base_asins), PHASE_A_SIZES, PHASE_A_REPS, PHASE_B_INTERVALS, PHASE_B_DURATION
    )

    if not args.confirm:
        print(__doc__)
        print()
        print(f"Estimated request budget against your cookies: ~{budget} requests.")
        print(f"Phase A: latency sweep, sizes {PHASE_A_SIZES}, {PHASE_A_REPS} reps each")
        print(
            f"Phase B: throughput at intervals {PHASE_B_INTERVALS} for "
            f"{PHASE_B_DURATION:.0f}s each (early-aborts on 429 storm)"
        )
        print()
        print("Re-run with --confirm to start.")
        sys.exit(0)

    asyncio.run(_run(base_asins))


async def _run(base_asins):
    try:
        client = TVSSClient()
    except TVSSConfigError as exc:
        print(f"ERROR: {exc}")
        print("Run `python main.py login` first, or set TVSS_COOKIE_HEADER.")
        sys.exit(1)

    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=120, keepalive_timeout=30)
    async with aiohttp.ClientSession(connector=connector) as session:
        a = await phase_a(
            client,
            session,
            base_asins,
            PHASE_A_SIZES,
            PHASE_A_REPS,
            PHASE_A_SPACING,
        )

        # Phase B uses a 50-ASIN chunk (real + synthetic) at concurrency=1 so
        # we are measuring the cookie's tolerance for sustained polling, not
        # the URL-length path (already covered by Phase A).
        chunk = list(base_asins) + synthetic(max(0, 50 - len(base_asins)), seed=42)
        chunk = chunk[:50]
        b = await phase_b(
            client,
            session,
            chunk,
            PHASE_B_INTERVALS,
            PHASE_B_DURATION,
            PHASE_B_BUCKET_COOLDOWN,
            PHASE_B_ABORT_429,
        )

    chunk_size, concurrency, interval = recommend(a, b)
    today = time.strftime("%Y-%m-%d")
    print()
    print("=" * 60)
    print(f"# Calibrated {today} — paste into endpoint.env")
    print(f"TVSS_BATCH_CHUNK_SIZE={chunk_size}")
    print(f"TVSS_BATCH_CONCURRENCY={concurrency}")
    print(f"POLL_INTERVAL_SECONDS={interval}")
    print("=" * 60)


if __name__ == "__main__":
    main()
