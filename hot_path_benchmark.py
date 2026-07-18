#!/usr/bin/env python3
"""Benchmark response bytes through one alert task scheduling operation."""

import argparse
import asyncio
import json
import math
import time
from pathlib import Path

from amazon_tvss import ObservationStatus, TVSSClient
from main import AlertState


FIXTURE = Path(__file__).parent / "fixtures" / "tvss_basicproducts_20.json"
ASINS = [f"B{index:09d}" for index in range(1, 21)]
TRANSITION_ASIN = ASINS[-1]


async def _scheduled_alert():
    await asyncio.sleep(0)


def percentile(values, percentile_value):
    ordered = sorted(values)
    index = max(
        0,
        min(
            len(ordered) - 1,
            math.ceil((percentile_value / 100.0) * len(ordered)) - 1,
        ),
    )
    return ordered[index]


async def run(iterations):
    response_bytes = FIXTURE.read_bytes()
    state = AlertState()
    for asin in ASINS:
        state.commit(asin, False)

    samples_ms = []
    for _ in range(iterations):
        state.commit(TRANSITION_ASIN, False)
        started_ns = time.perf_counter_ns()
        observations = TVSSClient.decode_batch_response(response_bytes, ASINS)
        scheduled = []

        for asin, observation in observations.items():
            if observation.status is ObservationStatus.UNKNOWN:
                continue
            if observation.status is ObservationStatus.OUT_OF_STOCK:
                state.commit(asin, False)
                continue
            if state.reserve_transition(asin, observation.status):
                scheduled.append(asyncio.create_task(_scheduled_alert()))

        samples_ms.append((time.perf_counter_ns() - started_ns) / 1_000_000)
        if len(scheduled) != 1:
            raise RuntimeError(
                f"fixture must schedule exactly one transition, got {len(scheduled)}"
            )
        state.finish_transition(TRANSITION_ASIN, delivered=False)
        await asyncio.gather(*scheduled)

    result = {
        "iterations": iterations,
        "asins": len(ASINS),
        "transitions_per_iteration": 1,
        "p50_ms": round(percentile(samples_ms, 50), 6),
        "p95_ms": round(percentile(samples_ms, 95), 6),
        "p99_ms": round(percentile(samples_ms, 99), 6),
        "max_ms": round(max(samples_ms), 6),
        "acceptance_p95_ms": 0.1,
    }
    result["accepted"] = result["p95_ms"] < result["acceptance_p95_ms"]
    print(json.dumps(result, sort_keys=True))
    return 0 if result["accepted"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=10_000)
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    raise SystemExit(asyncio.run(run(args.iterations)))


if __name__ == "__main__":
    main()
