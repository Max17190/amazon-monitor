#!/usr/bin/env python3
"""Measure the durable 20-ASIN commit and push-delivery latency path."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import aiohttp
from aiohttp import web

from alert_delivery import AlertDeliveryWorker, GenericWebhookSender, OutboxWakeup
from amazon_tvss import TVSSClient
from credential_governor import PostgresCredentialGovernor
from durable_runtime import PostgresOutboxRepository
from durable_store import (
    AlertWrite,
    BatchStockDecision,
    PostgresStore,
    ScopeKey,
    TargetWrite,
    TransitionWrite,
)
from observability import DeliveryHealth, DeliveryMetrics
from webhooks import WebhookTarget


FIXTURE = Path(__file__).parent / "fixtures" / "tvss_basicproducts_20.json"
ASINS = [f"B{index:09d}" for index in range(1, 21)]


def percentile(values, percentile_value):
    ordered = sorted(values)
    index = round((percentile_value / 100.0) * (len(ordered) - 1))
    return ordered[max(0, min(len(ordered) - 1, index))]


def build_decisions(monitor_id, iteration, target_id):
    observed_at = datetime.now(timezone.utc)
    decisions = []
    expected_ids = []
    for index, asin in enumerate(ASINS):
        scope = ScopeKey(monitor_id, "benchmark", asin, "benchmark-policy")
        transition_id = uuid4()
        alert_id = uuid4()
        delivery_id = uuid4()
        transition = TransitionWrite(
            transition_id=transition_id,
            stock_epoch=iteration + 1,
            signal_type="offer_detected",
            confirmed=False,
            evidence_hash=f"benchmark-{iteration}-{index}",
            evidence={"offer_id": f"offer-{iteration}-{index}"},
            detected_at=observed_at,
        )
        alert = AlertWrite(
            alert_id=alert_id,
            payload={
                "asin": asin,
                "alert_id": str(alert_id),
                "transition_id": str(transition_id),
                "detected_at": observed_at.isoformat(),
                "confirmed": False,
                "signal": "offer_detected",
            },
        )
        state = {
            "scope_key": (
                f"{monitor_id}:benchmark:{asin}:benchmark-policy"
            ),
            "state": "BUYABLE_UNCONFIRMED",
            "last_sequence": iteration * len(ASINS) + index + 1,
            "last_observed_at": observed_at.isoformat(),
            "last_evidence_hash": transition.evidence_hash,
            "seller_policy_hash": "benchmark-policy",
            "strong_oos_count": 0,
            "last_strong_oos_at": None,
            "epoch": iteration + 1,
            "armed_for_restock": True,
            "initialized": True,
        }
        decisions.append(
            BatchStockDecision(
                scope=scope,
                state_record=state,
                expected_version=None if iteration == 0 else iteration,
                evidence=transition.evidence,
                transition=transition,
                alert=alert,
                targets=(
                    TargetWrite(
                        target_id=target_id,
                        target_kind="generic",
                        delivery_id=delivery_id,
                    ),
                ),
            )
        )
        expected_ids.append(str(delivery_id))
    return tuple(decisions), tuple(expected_ids)


async def cleanup(store, monitor_id, credential_key):
    async with store.pool.acquire() as connection:
        async with connection.transaction():
            transition_ids = await connection.fetch(
                "SELECT transition_id FROM stock_transitions WHERE monitor_id=$1",
                monitor_id,
            )
            ids = [row["transition_id"] for row in transition_ids]
            if ids:
                await connection.execute(
                    "DELETE FROM alert_events WHERE transition_id=ANY($1::uuid[])",
                    ids,
                )
                await connection.execute(
                    "DELETE FROM stock_transitions WHERE transition_id=ANY($1::uuid[])",
                    ids,
                )
            await connection.execute(
                "DELETE FROM product_states WHERE monitor_id=$1",
                monitor_id,
            )
            await connection.execute(
                "DELETE FROM credential_governor WHERE credential_key=$1",
                credential_key,
            )


async def wait_for_delivery_completion(store, delivery_ids):
    pending = len(delivery_ids)
    deadline = time.monotonic() + 3
    while pending:
        async with store.pool.acquire() as connection:
            pending = await connection.fetchval(
                """
                SELECT COUNT(*)
                FROM alert_deliveries
                WHERE delivery_id=ANY($1::uuid[])
                  AND status <> 'succeeded'
                """,
                [UUID(value) for value in delivery_ids],
            )
        if pending:
            if time.monotonic() >= deadline:
                raise TimeoutError("delivery rows did not reach succeeded")
            await asyncio.sleep(0)


async def run(database_url, iterations, warmup_iterations=10):
    monitor_id = f"latency-benchmark-{uuid4()}"
    store = await PostgresStore.connect(
        database_url, min_size=2, max_size=8
    )
    await store.migrate()
    credential_key = f"benchmark-{uuid4()}"
    lease_owner = f"benchmark-{uuid4()}"
    governor = PostgresCredentialGovernor(store.pool, base_interval=5)
    await governor.initialize()
    if not await governor.acquire_leader(
        credential_key, lease_owner, 3600
    ):
        raise RuntimeError("benchmark could not acquire its credential lease")
    accepted_at = {}
    accepted_event = asyncio.Event()

    async def accept(request):
        delivery_id = request.headers["X-Alert-Delivery-Id"]
        await request.read()
        accepted_at[delivery_id] = time.perf_counter()
        accepted_event.set()
        return web.Response(status=204)

    app = web.Application()
    app.router.add_post("/alert", accept)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    target = WebhookTarget(
        "BENCHMARK",
        f"http://127.0.0.1:{port}/alert",
        kind="generic",
    )
    wakeup = OutboxWakeup()
    metrics = DeliveryMetrics()
    health = DeliveryHealth(repository_ready=True)
    connector = aiohttp.TCPConnector(limit=32, keepalive_timeout=120)
    commit_ms = []
    commit_to_attempt_ms = []
    response_to_accept_ms = []
    worker = None
    worker_task = None
    response_bytes = FIXTURE.read_bytes()
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            repository = PostgresOutboxRepository(
                store,
                {target.name: target},
                monitor_id,
                global_concurrency=32,
                per_target_concurrency=2,
            )

            class InstrumentedSender:
                def __init__(self):
                    self.sender = GenericWebhookSender(session)
                    self.attempted_at = {}

                async def send(self, delivery):
                    self.attempted_at[delivery.delivery_id] = time.perf_counter()
                    return await self.sender.send(delivery)

            sender = InstrumentedSender()
            worker = AlertDeliveryWorker(
                repository,
                sender,
                metrics=metrics,
                health=health,
                concurrency=32,
                per_target_concurrency=2,
                wakeup=wakeup,
                fallback_poll_seconds=1,
            )
            worker_task = asyncio.create_task(worker.run())
            await asyncio.sleep(0)
            total_iterations = warmup_iterations + iterations
            for iteration in range(total_iterations):
                accepted_event.clear()
                response_started = time.perf_counter()
                TVSSClient.decode_batch_response(response_bytes, ASINS)
                decisions, expected = build_decisions(
                    monitor_id, iteration, target.name
                )
                result = await store.commit_stock_decisions(
                    decisions,
                    lease_credential_key=credential_key,
                    lease_owner=lease_owner,
                )
                committed_at = time.perf_counter()
                wakeup.wake(str(value) for value in result.delivery_ids)
                deadline = time.monotonic() + 3
                while not all(value in accepted_at for value in expected):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("delivery burst was not accepted")
                    await asyncio.wait_for(
                        accepted_event.wait(), timeout=remaining
                    )
                    accepted_event.clear()
                first_attempt = min(sender.attempted_at[value] for value in expected)
                first_accept = min(accepted_at[value] for value in expected)
                await wait_for_delivery_completion(store, expected)
                if iteration >= warmup_iterations:
                    commit_ms.append(
                        (committed_at - response_started) * 1000
                    )
                    commit_to_attempt_ms.append(
                        (first_attempt - committed_at) * 1000
                    )
                    response_to_accept_ms.append(
                        (first_accept - response_started) * 1000
                    )
    finally:
        if worker is not None:
            worker.stop()
        if worker_task is not None:
            await asyncio.gather(worker_task, return_exceptions=True)
        await cleanup(store, monitor_id, credential_key)
        await runner.cleanup()
        await store.close()

    result = {
        "iterations": iterations,
        "warmup_iterations": warmup_iterations,
        "asins": len(ASINS),
        "response_to_commit_p95_ms": round(percentile(commit_ms, 95), 3),
        "commit_to_first_attempt_p95_ms": round(
            percentile(commit_to_attempt_ms, 95), 3
        ),
        "response_to_local_accept_p95_ms": round(
            percentile(response_to_accept_ms, 95), 3
        ),
        "acceptance": {
            "response_to_commit_p95_ms": 10,
            "commit_to_first_attempt_p95_ms": 10,
            "response_to_local_accept_p95_ms": 25,
        },
    }
    result["accepted"] = (
        result["response_to_commit_p95_ms"] < 10
        and result["commit_to_first_attempt_p95_ms"] < 10
        and result["response_to_local_accept_p95_ms"] < 25
    )
    print(json.dumps(result, sort_keys=True))
    return 0 if result["accepted"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.getenv("TEST_DATABASE_URL", ""),
    )
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--warmup-iterations", type=int, default=10)
    parser.add_argument("--confirm-database-writes", action="store_true")
    args = parser.parse_args()
    if not args.database_url:
        parser.error("--database-url or TEST_DATABASE_URL is required")
    if not args.confirm_database_writes:
        parser.error("--confirm-database-writes is required")
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    if args.warmup_iterations < 0:
        parser.error("--warmup-iterations cannot be negative")
    raise SystemExit(
        asyncio.run(
            run(
                args.database_url,
                args.iterations,
                args.warmup_iterations,
            )
        )
    )


if __name__ == "__main__":
    main()
