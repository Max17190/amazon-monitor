import asyncio
import unittest
from datetime import UTC, datetime, timedelta

import aiohttp

from alert_delivery import (
    AlertDelivery, AlertDeliveryWorker, CircuitBreaker, DeliveryAttempt, DeliveryStatus, DeliveryTarget,
    ErrorClass, GenericWebhookSender, classify_response, decorrelated_jitter, parse_retry_after,
)
from observability import DeliveryHealth, DeliveryMetrics, HealthStatus


class MemoryOutbox:
    def __init__(self, rows):
        self.rows, self.calls = list(rows), []

    async def claim_due(self, *, limit, now, lease_seconds):
        return self.rows[:limit]

    async def succeed(self, delivery_id, **kwargs):
        self.calls.append(("success", delivery_id, kwargs))

    async def retry(self, delivery_id, **kwargs):
        self.calls.append(("retry", delivery_id, kwargs))

    async def dead_letter(self, delivery_id, **kwargs):
        self.calls.append(("dead", delivery_id, kwargs))


class Sender:
    def __init__(self, result): self.result = result
    async def send(self, delivery): return self.result


def row(*, attempts=0, created_at=100.0, delivery_id="delivery-1", target_id="discord-main"):
    return AlertDelivery(delivery_id, f"alert-{delivery_id}", DeliveryTarget(target_id, "https://example.test"),
                         {"asin": "B000000001"}, created_at, attempts=attempts, status=DeliveryStatus.LEASED)


class FakeResponse:
    def __init__(self, status=204, headers=None):
        self.status = status
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class FakeSession:
    def __init__(self, response=None):
        self.response = response or FakeResponse()
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


class RetryTests(unittest.TestCase):
    def test_retry_classes(self):
        self.assertTrue(classify_response(408).retryable)
        self.assertTrue(classify_response(425).retryable)
        self.assertTrue(classify_response(429).retryable)
        self.assertTrue(classify_response(503).retryable)
        self.assertFalse(classify_response(400).retryable)
        self.assertEqual(classify_response(None, aiohttp.ClientConnectionError()).error_class, ErrorClass.NETWORK)

    def test_retry_after_and_jitter(self):
        self.assertEqual(parse_retry_after("3"), 3.0)
        now = datetime(2026, 1, 1, tzinfo=UTC)
        self.assertEqual(parse_retry_after("Thu, 01 Jan 2026 00:00:03 GMT", now=now), 3.0)
        self.assertEqual(decorrelated_jitter(1, rng=lambda low, high: high), 3.0)

    def test_circuit_breaker_opens_after_five_failures(self):
        breaker = CircuitBreaker()
        for _ in range(4): self.assertFalse(breaker.record_failure(10))
        self.assertTrue(breaker.record_failure(10))
        self.assertFalse(breaker.allow(69))
        self.assertTrue(breaker.allow(70))


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_and_low_cardinality_metric(self):
        repository = MemoryOutbox([row()])
        metrics = DeliveryMetrics()
        worker = AlertDeliveryWorker(repository, Sender(DeliveryAttempt(True, 204)), metrics=metrics, clock=lambda: 110)
        self.assertEqual(await worker.run_once(), 1)
        self.assertEqual(repository.calls[0][0], "success")
        self.assertEqual(metrics.counter("alert_delivery_total", {"target": "discord-main", "kind": "generic", "outcome": "success"}), 1)

    async def test_retry_honors_retry_after(self):
        repository = MemoryOutbox([row()])
        worker = AlertDeliveryWorker(repository, Sender(DeliveryAttempt(False, 429, retry_after_seconds=12)),
                                     clock=lambda: 110, rng=lambda low, high: low)
        await worker.run_once()
        kind, _, values = repository.calls[0]
        self.assertEqual(kind, "retry")
        self.assertEqual(values["next_attempt_at"], 122)
        self.assertEqual(values["error_class"], ErrorClass.RATE_LIMITED)

    async def test_terminal_and_exhausted_attempts_go_to_dlq(self):
        repository = MemoryOutbox([row()])
        worker = AlertDeliveryWorker(repository, Sender(DeliveryAttempt(False, 400)), clock=lambda: 110)
        await worker.run_once()
        self.assertEqual(repository.calls[0][0], "dead")
        repository = MemoryOutbox([row(attempts=9)])
        worker = AlertDeliveryWorker(repository, Sender(DeliveryAttempt(False, 503)), clock=lambda: 110)
        await worker.run_once()
        self.assertEqual(repository.calls[0][0], "dead")

    async def test_retry_is_not_scheduled_past_delivery_lifetime(self):
        repository = MemoryOutbox([row(created_at=100)])
        worker = AlertDeliveryWorker(
            repository,
            Sender(
                DeliveryAttempt(
                    False,
                    429,
                    retry_after_seconds=20,
                )
            ),
            clock=lambda: 990,
            max_age_seconds=900,
        )
        await worker.run_once()
        self.assertEqual(repository.calls[0][0], "dead")

    async def test_circuit_short_circuits_target(self):
        repository = MemoryOutbox([row()])
        worker = AlertDeliveryWorker(repository, Sender(DeliveryAttempt(False, 503)), clock=lambda: 110)
        breaker = CircuitBreaker(failures=5, open_until=120)
        worker._breakers["discord-main"] = breaker
        await worker.run_once()
        self.assertEqual(repository.calls[0][0], "retry")
        self.assertEqual(repository.calls[0][2]["detail"], "circuit_open")

    async def test_stop_changes_health_and_prevents_claim(self):
        health = DeliveryHealth(repository_ready=True, worker_running=True)
        worker = AlertDeliveryWorker(MemoryOutbox([row()]), Sender(DeliveryAttempt(True)), health=health)
        worker.stop()
        self.assertEqual(health.status, HealthStatus.STOPPING)
        self.assertEqual(await worker.run_once(), 0)

    async def test_target_failure_does_not_abandon_healthy_target(self):
        repository = MemoryOutbox(
            [
                row(delivery_id="good", target_id="good-target"),
                row(delivery_id="bad", target_id="bad-target"),
            ]
        )

        class TargetSender:
            async def send(self, delivery):
                if delivery.target.target_id == "good-target":
                    return DeliveryAttempt(True, 204)
                return DeliveryAttempt(False, 503)

        worker = AlertDeliveryWorker(
            repository,
            TargetSender(),
            clock=lambda: 110,
            rng=lambda low, high: low,
        )
        await worker.run_once()
        self.assertEqual(
            {call[0] for call in repository.calls},
            {"success", "retry"},
        )

    async def test_twenty_delivery_burst_is_processed(self):
        rows = [
            row(
                delivery_id=f"delivery-{index}",
                target_id=f"target-{index}",
            )
            for index in range(20)
        ]
        repository = MemoryOutbox(rows)
        worker = AlertDeliveryWorker(
            repository,
            Sender(DeliveryAttempt(True, 204)),
            concurrency=32,
            clock=lambda: 110,
        )
        self.assertEqual(await worker.run_once(), 20)
        self.assertEqual(
            sum(call[0] == "success" for call in repository.calls),
            20,
        )


class GenericSenderTests(unittest.IsolatedAsyncioTestCase):
    async def test_identity_and_trace_headers_are_stable(self):
        session = FakeSession()
        sender = GenericWebhookSender(session)
        delivery = row()
        delivery.trace_context = (
            "00-0123456789abcdef0123456789abcdef-"
            "0123456789abcdef-01"
        )
        result = await sender.send(delivery)
        self.assertTrue(result.success)
        _, values = session.calls[0]
        self.assertEqual(
            values["headers"]["Idempotency-Key"],
            delivery.idempotency_key,
        )
        self.assertEqual(
            values["headers"]["X-Alert-Id"],
            delivery.alert_id,
        )
        self.assertEqual(
            values["headers"]["X-Alert-Delivery-Id"],
            delivery.delivery_id,
        )
        self.assertEqual(
            values["headers"]["traceparent"],
            delivery.trace_context,
        )
        self.assertEqual(values["timeout"].total, 3.0)


if __name__ == "__main__":
    unittest.main()
