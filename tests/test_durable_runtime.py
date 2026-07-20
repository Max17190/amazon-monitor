import asyncio
import json
import inspect
import os
import time
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from uuid import uuid4

from amazon_tvss import BatchObservation, ObservationStatus
from durable_runtime import (
    ConfirmationThrottle,
    DEFAULT_VERIFICATION_TTL_SECONDS,
    DurableStockCoordinator,
    PostgresOutboxRepository,
    _batch_evidence,
    _batch_product_payload,
    _build_group_maps,
    _leader_supervisor,
    _outbox_notification_listener,
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
        self.verification_ttls = []
        self.bulk_loads = 0
        self.committed_scopes = []
        self.commit_options = {}

    async def load_product_state(self, scope):
        return self.rows.get(scope)

    async def load_product_states(self, scopes):
        self.bulk_loads += 1
        return {
            scope: self.rows[scope]
            for scope in scopes
            if scope in self.rows
        }

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
        self.verification_ttls.append(ttl_seconds)
        return uuid4()

    async def commit_stock_decisions(
        self,
        decisions,
        **options,
    ):
        from durable_store import BatchCommitResult

        decisions = tuple(decisions)
        self.commit_options = options
        self.committed_scopes = [item.scope for item in decisions]
        next_rows = dict(self.rows)
        transitions = []
        verifications = []
        versions = {}
        transition_ids = []
        delivery_ids = []
        for item in decisions:
            current = next_rows.get(item.scope)
            current_version = current.get("version") if current else None
            if current_version != item.expected_version:
                from durable_store import StateConflict
                raise StateConflict("fake optimistic conflict")
            row = dict(item.state_record)
            row["version"] = 1 if current_version is None else current_version + 1
            row["last_evidence"] = item.evidence
            next_rows[item.scope] = row
            versions[item.scope] = row["version"]
            if item.transition is not None:
                transitions.append((item.transition, item.alert, item.targets))
                transition_ids.append(item.transition.transition_id)
                delivery_ids.extend(target.delivery_id for target in item.targets)
            if item.verification is not None:
                verifications.append((item.scope, item.verification.source_sequence, item.verification.evidence))
        self.rows = next_rows
        self.transitions.extend(transitions)
        self.verifications.extend(verifications)
        return BatchCommitResult(
            versions,
            tuple(transition_ids),
            tuple(delivery_ids),
            tuple(uuid4() for _ in verifications),
            (
                tuple(delivery_ids)
                if options.get("prelease_worker_id") is not None
                else ()
            ),
        )


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

    def test_accepts_requested_sub_five_interval_for_calibrated_fallback(self):
        env = {
            "DATABASE_URL": "postgresql://db",
            "MONITOR_ID": "test",
        }
        self.assertEqual(
            validate_durable_configuration(self.config(0.5), env=env),
            "test",
        )
        with self.assertRaisesRegex(Exception, "positive"):
            validate_durable_configuration(self.config(0), env=env)

    def test_fast_alert_confirmation_uses_one_slot_per_twelve_polls(self):
        throttle = ConfirmationThrottle(12)
        for _ in range(11):
            throttle.note_poll()
            self.assertFalse(throttle.due(fast_alert=True))
        throttle.note_poll()
        self.assertTrue(throttle.due(fast_alert=True))
        throttle.consumed()
        self.assertFalse(throttle.due(fast_alert=True))
        self.assertTrue(throttle.due(fast_alert=False))

    def test_fast_alert_default_confirmation_ttl_covers_throttled_window(self):
        self.assertGreaterEqual(DEFAULT_VERIFICATION_TTL_SECONDS, 65.0)

    def test_leader_supervisor_accepts_calibration_authority_arguments(self):
        inspect.signature(_leader_supervisor).bind(
            *([None] * 15)
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

    async def test_batch_uses_one_bulk_load_and_commits_fanout_once(self):
        second = "B000000002"
        self.coordinator.asin_groups[second] = ["GPUs"]
        self.coordinator.asin_targets[second] = [self.target_a]
        product = {
            "asin": self.asin, "title": "Product", "images": [], "source": "tvss"
        }
        second_product = {**product, "asin": second}
        observations = []
        # Submit the non-buyable item first to prove the fast path reorders
        # independent batch decisions before its durable commit.
        for asin, item, offer in (
            (second, second_product, None),
            (self.asin, product, "offer"),
        ):
            observations.append((
                StockEvidence(
                    scope_key=self.coordinator.scope_name(asin), sequence=1,
                    observed_at=self.started, source=EvidenceSource.BATCH,
                    response_complete=True, offer_id=offer,
                    offer_explicitly_null=offer is None,
                    availability_condition=("IN_STOCK" if offer else "OUT_OF_STOCK"),
                ),
                item,
            ))
        decisions, result = await self.coordinator.process_batch(observations)
        self.assertEqual(len(decisions), 2)
        self.assertEqual(self.store.bulk_loads, 1)
        self.assertEqual(self.store.committed_scopes[0], self.coordinator.scope(self.asin))
        self.assertEqual(len(result.versions), 2)
        self.assertEqual(len(self.store.rows), 2)
        self.assertEqual(len(self.store.verifications), 1)

    async def test_commit_preleased_rows_are_queued_without_a_second_claim(self):
        from alert_delivery import OutboxWakeup

        wakeup = OutboxWakeup()
        self.coordinator.outbox_wakeup = wakeup
        self.coordinator.delivery_worker_id = "worker"
        product = {
            "asin": self.asin,
            "title": "Product",
            "link": "https://www.amazon.com/dp/B000000001",
            "images": [],
            "source": "tvss",
        }
        for sequence in (1, 2, 3):
            await self.coordinator.process_batch(
                ((
                    self.evidence(
                        sequence,
                        EvidenceSource.BATCH,
                        offer_explicitly_null=True,
                        availability_condition="OUT_OF_STOCK",
                    ),
                    product,
                ),)
            )
        await self.coordinator.process_batch(
            ((
                self.evidence(
                    4,
                    EvidenceSource.BATCH,
                    offer_id="offer",
                    availability_condition="IN_STOCK",
                ),
                product,
            ),)
        )
        await self.coordinator.process_batch(
            ((
                self.evidence(
                    5,
                    EvidenceSource.FULL_PRODUCT,
                    offer_id="offer",
                    sold_by_amazon=True,
                    seller_name="Amazon.com",
                    availability_status="IN_STOCK",
                ),
                {
                    **product,
                    "price": "$10.00",
                    "seller": "Amazon.com",
                    "seller_verified": True,
                },
            ),)
        )

        preleased = wakeup.take_preleased()
        self.assertEqual(len(preleased), 2)
        self.assertEqual(
            {item.target.target_id for item in preleased},
            {"A", "B"},
        )
        self.assertEqual(
            {item.target.url for item in preleased},
            {"https://a.example", "https://b.example"},
        )
        self.assertEqual(wakeup.take_preferred_delivery_ids(), ())
        self.assertEqual(
            self.store.commit_options["prelease_worker_id"],
            "worker",
        )

    async def test_fast_alert_promotes_short_confirmation_ttl_to_cadence_window(self):
        self.coordinator.config.fast_alert = True
        product = {
            "asin": self.asin, "title": "Product", "images": [], "source": "tvss"
        }
        with patch.dict(
            os.environ,
            {"STOCK_CONFIRM_TTL_SECONDS": "30", "FAST_ALERT_CONFIRM_EVERY_POLLS": "12"},
            clear=False,
        ):
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
            await self.coordinator.process(
                self.evidence(
                    4,
                    EvidenceSource.BATCH,
                    offer_id="offer",
                    availability_condition="IN_STOCK",
                ),
                product,
            )
        self.assertEqual(self.store.verification_ttls, [65.0])


class OutboxNotificationListenerTests(unittest.IsolatedAsyncioTestCase):
    class Connection:
        def __init__(self):
            self.listeners = []
            self.termination_listeners = []
            self.removed_listeners = []
            self.released = False

        async def add_listener(self, channel, callback):
            self.listeners.append((channel, callback))

        async def remove_listener(self, channel, callback):
            if self.released:
                raise AssertionError("listener removed after pool release")
            self.removed_listeners.append((channel, callback))
            self.listeners.remove((channel, callback))

        def add_termination_listener(self, callback):
            self.termination_listeners.append(callback)

        def remove_termination_listener(self, callback):
            if self.released:
                raise AssertionError(
                    "termination listener removed after pool release"
                )
            self.termination_listeners.remove(callback)

        def terminate(self):
            for callback in tuple(self.termination_listeners):
                callback(self)

    class Pool:
        def __init__(self, connections):
            self.connections = iter(connections)
            self.acquired = []

        def acquire(self):
            pool = self

            class Acquisition:
                async def __aenter__(self):
                    self.connection = next(pool.connections)
                    pool.acquired.append(self.connection)
                    return self.connection

                async def __aexit__(self, *_args):
                    if self.connection.listeners:
                        raise AssertionError(
                            "pool released connection with LISTEN callback"
                        )
                    if self.connection.termination_listeners:
                        raise AssertionError(
                            "pool released connection with termination callback"
                        )
                    self.connection.released = True
                    return False

            return Acquisition()

    async def test_reconnects_and_reregisters_listener_after_connection_loss(self):
        first = self.Connection()
        second = self.Connection()
        store = types.SimpleNamespace(pool=self.Pool([first, second]))
        stop_event = asyncio.Event()
        wakeup = types.SimpleNamespace(wake=lambda _ids=(): None)
        retry_delays = []

        async def retry_wait(_stop_event, seconds):
            retry_delays.append(seconds)

        task = asyncio.create_task(
            _outbox_notification_listener(
                store,
                wakeup,
                stop_event,
                reconnect_initial_seconds=0.25,
                reconnect_max_seconds=1.0,
                reconnect_wait=retry_wait,
            )
        )
        for _ in range(20):
            if first.listeners:
                break
            await asyncio.sleep(0)
        self.assertEqual(first.listeners[0][0], "alert_outbox_ready")
        first.terminate()
        for _ in range(20):
            if second.listeners:
                break
            await asyncio.sleep(0)
        self.assertEqual(second.listeners[0][0], "alert_outbox_ready")
        self.assertEqual(retry_delays, [0.25])
        stop_event.set()
        await task
        self.assertEqual(len(second.removed_listeners), 1)
        self.assertTrue(first.released)
        self.assertTrue(second.released)


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

    async def test_forwards_preferred_delivery_ids_to_store(self):
        class RecordingStore(FakeOutboxStore):
            async def claim_deliveries(self, *args, **kwargs):
                self.preferred = kwargs["preferred_delivery_ids"]
                return []

        store = RecordingStore([])
        repository = PostgresOutboxRepository(store, {}, "worker")
        await repository.claim_due(
            limit=1,
            now=time.time(),
            lease_seconds=30,
            preferred_delivery_ids=("preferred",),
        )
        self.assertEqual(store.preferred, ("preferred",))


if __name__ == "__main__":
    unittest.main()
