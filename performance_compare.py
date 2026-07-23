#!/usr/bin/env python3
"""Compare control and candidate latency samples from ABBA canary blocks."""

from __future__ import annotations

import argparse
import json
import math
import random
from pathlib import Path


def percentile(values, percentile_value):
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    index = max(
        0,
        min(
            len(ordered) - 1,
            math.ceil((float(percentile_value) / 100.0) * len(ordered)) - 1,
        ),
    )
    return ordered[index]


def read_records(paths):
    records = []
    for path in paths:
        for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            record = json.loads(raw_line)
            message = record.get("message")
            if isinstance(message, str) and "{" in message:
                prefix, payload = message.split("{", 1)
                if prefix.strip().endswith(
                    ("regional_validation", "performance_result")
                ):
                    record = json.loads("{" + payload)
            if isinstance(record, dict) and "samples" in record:
                records.append(record)
    return records


def select_experiment(records, experiment_id=None):
    present_ids = {
        str(record["experiment_id"])
        for record in records
        if record.get("experiment_id") not in (None, "")
    }
    missing_count = sum(
        record.get("experiment_id") in (None, "")
        for record in records
    )
    if present_ids and missing_count:
        raise ValueError(
            "cannot combine records with and without experiment_id"
        )
    if experiment_id is not None:
        selected = [
            record
            for record in records
            if str(record.get("experiment_id")) == str(experiment_id)
        ]
        if not selected:
            raise ValueError(
                f"experiment_id {experiment_id!r} has no records"
            )
        return selected, str(experiment_id)
    if len(present_ids) > 1:
        raise ValueError(
            "multiple experiment_id values require an explicit selection"
        )
    resolved = next(iter(present_ids), None)
    return list(records), resolved


def validate_abba(records, control, candidate):
    by_block = {}
    for record in records:
        block = int(record["block"])
        variant = str(record["variant"])
        if block in by_block:
            raise ValueError(f"duplicate block {block}")
        by_block[block] = variant
    block_ids = sorted(by_block)
    expected_ids = list(range(1, len(block_ids) + 1))
    if block_ids != expected_ids:
        raise ValueError(
            "ABBA block numbers must be contiguous and start at 1"
        )
    ordered = [by_block[index] for index in block_ids]
    if len(ordered) < 4 or len(ordered) % 4:
        raise ValueError("ABBA comparison requires a multiple of four blocks")
    expected = [control, candidate, candidate, control]
    for offset in range(0, len(ordered), 4):
        if ordered[offset : offset + 4] != expected:
            raise ValueError(
                f"blocks {offset + 1}-{offset + 4} are not ABBA"
            )


def bootstrap_median_delta(control, candidate, iterations=2000, seed=7):
    rng = random.Random(seed)
    deltas = []
    for _ in range(int(iterations)):
        control_sample = [
            control[rng.randrange(len(control))] for _ in range(len(control))
        ]
        candidate_sample = [
            candidate[rng.randrange(len(candidate))]
            for _ in range(len(candidate))
        ]
        deltas.append(
            percentile(candidate_sample, 50)
            - percentile(control_sample, 50)
        )
    return (
        percentile(deltas, 2.5),
        percentile(deltas, 97.5),
    )


def compare(
    records,
    metric,
    *,
    control="control",
    candidate="candidate",
    min_samples=120,
    min_absolute_improvement_ms=10.0,
    min_relative_improvement=0.05,
    max_p99_regression=0.05,
    experiment_id=None,
):
    records, resolved_experiment_id = select_experiment(
        records,
        experiment_id,
    )
    validate_abba(records, control, candidate)
    failures = [
        record
        for record in records
        if record.get("outcome", "clean") != "clean"
        or int(record.get("error_count", 0))
        or int(record.get("rate_limit_count", 0))
        or int(record.get("mismatch_count", 0))
    ]
    samples = {control: [], candidate: []}
    for record in records:
        variant = str(record["variant"])
        if variant not in samples:
            continue
        values = record.get("samples", {}).get(metric, [])
        samples[variant].extend(float(value) for value in values)

    control_values = samples[control]
    candidate_values = samples[candidate]
    if len(control_values) < min_samples or len(candidate_values) < min_samples:
        raise ValueError(
            f"{metric} requires at least {min_samples} samples per variant"
        )

    control_stats = {
        "count": len(control_values),
        "p50_ms": percentile(control_values, 50),
        "p95_ms": percentile(control_values, 95),
        "p99_ms": percentile(control_values, 99),
        "max_ms": max(control_values),
    }
    candidate_stats = {
        "count": len(candidate_values),
        "p50_ms": percentile(candidate_values, 50),
        "p95_ms": percentile(candidate_values, 95),
        "p99_ms": percentile(candidate_values, 99),
        "max_ms": max(candidate_values),
    }
    absolute_improvement = (
        control_stats["p95_ms"] - candidate_stats["p95_ms"]
    )
    if control_stats["p95_ms"] == 0:
        relative_improvement = None
        relative_gate_passed = False
    else:
        relative_improvement = (
            absolute_improvement / control_stats["p95_ms"]
        )
        relative_gate_passed = (
            relative_improvement >= float(min_relative_improvement)
        )
    if control_stats["p99_ms"] == 0:
        if candidate_stats["p99_ms"] == 0:
            p99_regression = 0.0
            p99_gate_passed = True
        else:
            p99_regression = None
            p99_gate_passed = False
    else:
        p99_regression = (
            candidate_stats["p99_ms"] - control_stats["p99_ms"]
        ) / control_stats["p99_ms"]
        p99_gate_passed = (
            p99_regression <= float(max_p99_regression)
        )
    median_delta_ci = bootstrap_median_delta(
        control_values,
        candidate_values,
    )
    accepted = (
        not failures
        and absolute_improvement >= float(min_absolute_improvement_ms)
        and relative_gate_passed
        and p99_gate_passed
        and median_delta_ci[1] < 0
    )
    return {
        "stage": "performance_comparison",
        "experiment_id": resolved_experiment_id,
        "accepted": accepted,
        "metric": metric,
        "control": control_stats,
        "candidate": candidate_stats,
        "p95_absolute_improvement_ms": absolute_improvement,
        "p95_relative_improvement": relative_improvement,
        "p99_regression": p99_regression,
        "median_delta_95ci_ms": list(median_delta_ci),
        "failed_blocks": len(failures),
        "gates": {
            "min_samples": int(min_samples),
            "min_absolute_improvement_ms": float(
                min_absolute_improvement_ms
            ),
            "min_relative_improvement": float(min_relative_improvement),
            "max_p99_regression": float(max_p99_regression),
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+")
    parser.add_argument("--metric", required=True)
    parser.add_argument("--control", default="control")
    parser.add_argument("--candidate", default="candidate")
    parser.add_argument("--experiment-id")
    parser.add_argument("--min-samples", type=int, default=120)
    parser.add_argument(
        "--min-absolute-improvement-ms",
        type=float,
        default=10.0,
    )
    parser.add_argument(
        "--min-relative-improvement",
        type=float,
        default=0.05,
    )
    parser.add_argument(
        "--max-p99-regression",
        type=float,
        default=0.05,
    )
    args = parser.parse_args()
    result = compare(
        read_records(args.files),
        args.metric,
        control=args.control,
        candidate=args.candidate,
        min_samples=args.min_samples,
        min_absolute_improvement_ms=args.min_absolute_improvement_ms,
        min_relative_improvement=args.min_relative_improvement,
        max_p99_regression=args.max_p99_regression,
        experiment_id=args.experiment_id,
    )
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(0 if result["accepted"] else 1)


if __name__ == "__main__":
    main()
