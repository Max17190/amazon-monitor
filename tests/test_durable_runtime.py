import json
import os
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from uuid import uuid4

from amazon_tvss import BatchObservation, ObservationStatus
from durable_runtime import (
    DurableStockCoordinator,
    PostgresOutboxRepository,
    _batch_evidence,
    _batch_product_payload,
    _build_group_maps,
    validate_durable_configuration,
)
from durable_store import ScopeKey
from observability import DeliveryMetrics
from stock_state import EvidenceSource, StockEvidence
from webhooks import WebhookTarget


class FakeStockStore:
    def __init__(self):
        self.rows = {}
        self.transitions = []
        self.verifications = []

    async def load_product_state(self, scope):
        return self.rows.get(scope)

    async def commit_stock_decision(
        self,
        scope,
        state_record,
        expected_version,
        transition=None,
        alert=None,
        targets=(),
        evidence=None,
    ):
        version = 1 if expected_version is None else expected_version + 1
        row = dict(state_record)
        row["version"] = version
        row["last_evidence"] = evidence or {}
        self.rows[scope] = row
        created = transition is not None
        if created:
            self.transitions.append((transition, alert, tuple(targets)))
        return {
            "version": version,
            "transition_created": created,
            "deliveries_created": len(tuple(targets)) if created else 0,
        }

    async def enqueue_verification(
        self,
        scope,
        source_sequence,
        evidence,
        ttl_seconds,
    ):
        self.verifications.append((scope, source_sequence, evidence))
        return uuid4()


class FakeOutboxStore:
    def __init__(self, rows):
        self.rows = rows

    async def claim_deliveries(
        self,
        worker_id,
        limit,
        lease_seconds,
        **_limits,
    ):
        return self.rows[:limit]


class DurableConfigurationTests(unittest.TestCase):
    def config(self, interval=5.0):
        return types.SimpleNamespace(poll_interval_seconds=interval)

    def test_requires_database_and_monitor_identity(self):
        with self.assertRaisesRegex(Exception, "DATABASE_URL"):
            validate_durable_configuration(self.config(), env={})
        with self.assertRaisesRegex(Exception, "MONITOR_ID"):
            validate_durable_configuration(
                self.config(),
                env={"DATABASE_URL": "postgresql://db"},
            )

    def test_rejects_sub_five_second_production_interval(self):
        env = {
            "DATABASE_URL": "postgresql://db",
            "MONITOR_ID": "test",
        }
        with self.assertRaisesRegex(Exception, "at least 5"):
            validate_durable_configuration(self.config(4.99), env=env)
        env["TVSS_CALIBRATION_MODE"] = "true"
        self.assertEqual(
            validate_durable_configuration(self.config(0.5), env=env),
            "test",
        )

    def test_rejects_unsafe_lease_and_rearm_configuration(self):
        env = {
            "DATABASE_URL": "postgresql://db",
            "MONITOR_ID": "test",
            "TVSS_LEADER_LEASE_SECONDS": "10",
            "TVSS_LEADER_RENEW_SECONDS": "10",
        }
        with self.assertRaisesRegex(Exception, "lease"):
            validate_durable_configuration(self.config(), env=env)
        env["TVSS_LEADER_LEASE_SECONDS"] = "30"
        env["STOCK_OOS_REARM_COUNT"] = "1"
        with self.assertRaisesRegex(Exception, "REARM"):
            validate_durable_configuration(self.config(), env=env)


class GroupFanoutTests(unittest.TestCase):
    def test_unions_groups_and_targets_for_duplicate_asin(self):
        target_a = WebhookTarget("A", "https://a.example", kind="generic")
        target_b = WebhookTarget("B", "https://b.example", kind="generic")
        groups = [
            types.SimpleNamespace(
                name="GPUs",
                asins=["B000000001"],
                webhook_names=["A"],
            ),
            types.SimpleNamespace(
                name="Drops",
                asins=["B000000001"],
                webhook_names=["A", "B"],
            ),
        ]
        config = types.SimpleNamespace(groups=groups)
        asin_groups, asin_targets = _build_group_maps(
            config,
            {"A": target_a, "B": target_b},
        )
        self.assertEqual(asin_groups["B000000001"], ["GPUs", "Drops"])
        self.assertEqual(
            [target.name for target in asin_targets["B000000001"]],
            ["A", "B"],
        )


class DurableStockIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.target_a = WebhookTarget(
            "A", "https://a.example", kind="generic"
        )
        self.target_b = WebhookTarget(
            "B", "https://b.example", kind="generic"
        )
        self.config = types.SimpleNamespace(
            poll_interval_seconds=5.0,
            require_amazon_seller=True,
            fast_alert=False,
        )
        self.store = FakeStockStore()
        self.coordinator = DurableStockCoordinator(
            self.store,
            self.config,
            "monitor",
            "market",
            "amazon.com",
            {"B000000001": ["GPUs", "Drops"]},
            {"B000000001": [self.target_a, self.target_b]},
            DeliveryMetrics(),
        )
        self.asin = "B000000001"
        self.started = datetime(2026, 7, 18, tzinfo=timezone.utc)

    def evidence(self, sequence, source, **values):
        defaults = {
            "scope_key": self.coordinator.scope_name(self.asin),
            "sequence": sequence,
            "observed_at": self.started + timedelta(seconds=sequence * 5),
            "source": source,
            "response_complete": True,
        }
        defaults.update(values)
        return StockEvidence(**defaults)

    async def test_confirmed_transition_and_target_fanout_are_atomic(self):
        product = {
            "asin": self.asin,
            "title": "Product",
            "link": "https://www.amazon.com/dp/B000000001",
            "images": [],
            "source": "tvss",
        }
        for sequence in (1, 2, 3):
            await self.coordinator.process(
                self.evidence(
                    sequence,
                    EvidenceSource.BATCH,
                    offer_explicitly_null=True,
                    availability_condition="OUT_OF_STOCK",
                ),
                product,
            )
        batch_buyable = await self.coordinator.process(
            self.evidence(
                4,
                EvidenceSource.BATCH,
                offer_id="offer",
                availability_condition="IN_STOCK",
            ),
            product,
        )
        self.assertEqual(batch_buyable.classification.state.value, "BUYABLE_UNCONFIRMED")
        self.assertEqual(len(self.store.transitions), 0)
        self.assertEqual(len(self.store.verifications), 1)

        confirmed_product = {
            **product,
            "price": "$10.00",
            "seller": "Amazon.com",
            "seller_verified": True,
        }
        decision = await self.coordinator.process(
            self.evidence(
                5,
                EvidenceSource.FULL_PRODUCT,
                offer_id="offer",
                sold_by_amazon=True,
                seller_name="Amazon.com",
                availability_status="IN_STOCK",
            ),
            confirmed_product,
        )
        self.assertIsNotNone(decision.event)
        self.assertEqual(len(self.store.transitions), 1)
        transition, alert, targets = self.store.transitions[0]
        self.assertTrue(transition.confirmed)
        self.assertEqual(alert.payload["groups"], ["GPUs", "Drops"])
        self.assertEqual({target.target_id for target in targets}, {"A", "B"})

        await self.coordinator.process(
            self.evidence(
                6,
                EvidenceSource.FULL_PRODUCT,
                offer_id="offer-2",
                sold_by_amazon=True,
                seller_name="Amazon.com",
                availability_status="IN_STOCK",
            ),
            confirmed_product,
        )
        self.assertEqual(len(self.store.transitions), 1)


class EvidenceAdapterTests(unittest.TestCase):
    def test_batch_offer_is_complete_without_availability_text(self):
        coordinator = types.SimpleNamespace(
            scope_name=lambda asin: f"scope:{asin}"
        )
        observation = BatchObservation(
            asin="B000000001",
            status=ObservationStatus.IN_STOCK,
            offer_id="offer",
            response_complete=True,
        )
        evidence = _batch_evidence(
            coordinator,
            observation.asin,
            observation,
            1,
            datetime.now(timezone.utc),
        )
        self.assertTrue(evidence.response_complete)
        self.assertEqual(evidence.offer_id, "offer")


class OutboxAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolves_target_without_persisting_webhook_url(self):
        now = datetime.now(timezone.utc)
        row = {
            "delivery_id": uuid4(),
            "alert_id": uuid4(),
            "target_id": "PRIMARY",
            "target_kind": "generic",
            "payload": {"asin": "B000000001"},
            "alert_created_at": now,
            "attempts": 1,
            "next_attempt_at": now,
            "previous_backoff_seconds": 2.0,
        }
        target = WebhookTarget(
            "PRIMARY", "https://secret.example/webhook", kind="generic"
        )
        repository = PostgresOutboxRepository(
            FakeOutboxStore([row]),
            {"PRIMARY": target},
            "worker",
        )
        claimed = await repository.claim_due(
            limit=1,
            now=now.timestamp(),
            lease_seconds=30,
        )
        self.assertEqual(len(claimed), 1)
        self.assertEqual(
            claimed[0].target.url,
            "https://secret.example/webhook",
        )
        self.assertNotIn("url", row)


if __name__ == "__main__":
    unittest.main()
