#!/usr/bin/env python3
"""Run an isolated ABBA canary for confirmation-slot scheduling latency."""

from __future__ import annotations

import argparse
import asyncio
import json
import random

from credential_governor import (
    InMemoryCredentialGovernor,
    RequestClass,
)
from performance_compare import compare


async def _sample_variant(variant, phases, interval_seconds):
    samples = []
    mismatches = 0
    for index, phase in enumerate(phases):
        clock_value = [0.0]
        governor = InMemoryCredentialGovernor(
            clock=lambda: clock_value[0],
            base_interval=interval_seconds,
        )
        key = f"confirmation-slot-canary-{variant}-{index}"
        owner = f"owner-{variant}-{index}"
        acquired = await governor.acquire_leader(
            key,
            owner,
            ttl_seconds=4 * interval_seconds,
        )
        if not acquired:
            raise RuntimeError("isolated canary could not acquire its lease")

        poll = await governor.acquire_permit(
            key,
            RequestClass.POLL,
            owner_id=owner,
        )
        if poll.wait_seconds:
            mismatches += 1

        clock_value[0] = float(phase)
        if variant == "candidate":
            confirmation = (
                await governor.acquire_borrowed_confirmation_permit(
                    key,
                    owner_id=owner,
                )
            )
        else:
            confirmation = await governor.acquire_permit(
                key,
                RequestClass.CONFIRM,
                owner_id=owner,
            )

        snapshot = await governor.snapshot(key)
        expected_next_request = 2 * interval_seconds
        if abs(snapshot.next_request_at - expected_next_request) > 1e-9:
            mismatches += 1
        samples.append(confirmation.wait_seconds * 1000.0)

    return samples, mismatches


async def run_abba(
    *,
    interval_seconds=5.0,
    observations_per_block=60,
    seed=17,
):
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if observations_per_block < 1:
        raise ValueError("observations_per_block must be positive")

    rng = random.Random(seed)
    phases = [
        rng.uniform(0.0, interval_seconds)
        for _ in range(observations_per_block)
    ]
    records = []
    for block, variant in enumerate(
        ("control", "candidate", "candidate", "control"),
        start=1,
    ):
        samples, mismatches = await _sample_variant(
            variant,
            phases,
            interval_seconds,
        )
        records.append(
            {
                "stage": "confirmation_slot_canary",
                "experiment_id": "confirmation-slot-scheduling",
                "block": block,
                "variant": variant,
                "outcome": "clean" if not mismatches else "mismatch",
                "error_count": 0,
                "rate_limit_count": 0,
                "mismatch_count": mismatches,
                "samples": {
                    "confirmation_start_delay_ms": samples,
                },
            }
        )
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--observations-per-block", type=int, default=60)
    parser.add_argument("--seed", type=int, default=17)
    args = parser.parse_args()

    records = asyncio.run(
        run_abba(
            interval_seconds=args.interval_seconds,
            observations_per_block=args.observations_per_block,
            seed=args.seed,
        )
    )
    for record in records:
        print(json.dumps(record, sort_keys=True))
    result = compare(
        records,
        "confirmation_start_delay_ms",
        min_samples=2 * args.observations_per_block,
        min_absolute_improvement_ms=500.0,
        min_relative_improvement=0.05,
        max_p99_regression=0.0,
    )
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(0 if result["accepted"] else 1)


if __name__ == "__main__":
    main()
