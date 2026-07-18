import asyncio
import os
import unittest
from datetime import datetime, timezone
from uuid import uuid4

from credential_governor import PostgresCredentialGovernor, RequestClass
from durable_store import (
    AlertWrite,
    PostgresStore,
    ScopeKey,
    TargetWrite,
    TransitionWrite,
)


TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")


@unittest.skipUnless(
    TEST_DATABASE_URL,
    "TEST_DATABASE_URL is required for Postgres integration tests",
)
class PostgresIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = await PostgresStore.connect(
            TEST_DATABASE_URL,
            min_size=1,
            max_size=8,
        )
        await asyncio.gather(self.store.migrate(), self.store.migrate())
        async with self.store.pool.acquire() as connection:
            await connection.execute(
                """
                TRUNCATE
                    alert_dead_letters,
                    alert_delivery_attempts,
                    alert_deliveries,
                    alert_events,
                    alert_target_circuits,
                    stock_verification_jobs,
                    stock_transitions,
                    product_states,
                    credential_governor
                RESTART IDENTITY CASCADE
                """
            )

    async def asyncTearDown(self):
        await self.store.close()

    @staticmethod
    def state(sequence=1):
        observed_at = datetime.now(timezone.utc)
        return {
            "scope_key": "monitor:market:B000000001:policy",
            "state": "IN_STOCK_CONFIRMED",
            "last_sequence": sequence,
            "last_observed_at": observed_at.isoformat(),
            "last_evidence_hash": "evidence",
            "seller_policy_hash": "policy",
            "strong_oos_count": 0,
            "last_strong_oos_at": None,
            "epoch": 1,
            "armed_for_restock": False,
            "initialized": True,
        }

    @staticmethod
    def writes(targets=("one", "two"), target_kind="generic"):
        detected_at = datetime.now(timezone.utc)
        transition = TransitionWrite(
            transition_id=uuid4(),
            stock_epoch=1,
            signal_type="restock_confirmed",
            confirmed=True,
            evidence_hash="evidence",
            evidence={"offer_id": "offer"},
            detected_at=detected_at,
        )
        alert = AlertWrite(
            alert_id=uuid4(),
            payload={
                "asin": "B000000001",
                "transition_id": str(transition.transition_id),
            },
            trace_context=(
                "00-0123456789abcdef0123456789abcdef-"
                "0123456789abcdef-01"
            ),
        )
        deliveries = tuple(
            TargetWrite(
                target_id=target,
                target_kind=target_kind,
                delivery_id=uuid4(),
            )
            for target in targets
        )
        return transition, alert, deliveries

    async def test_migration_is_idempotent_under_concurrent_startup(self):
        async with self.store.pool.acquire() as connection:
            versions = await connection.fetchval(
                "SELECT COUNT(*) FROM monitor_schema_migrations"
            )
            circuits = await connection.fetchval(
                """
                SELECT to_regclass(
                    'public.alert_target_circuits'
                ) IS NOT NULL
                """
            )
        self.assertEqual(versions, 1)
        self.assertTrue(circuits)

    async def test_transition_and_delivery_intents_commit_atomically(self):
        scope = ScopeKey(
            "monitor",
            "market",
            "B000000001",
            "policy",
        )
        transition, alert, targets = self.writes()
        result = await self.store.commit_stock_decision(
            scope,
            self.state(),
            None,
            transition=transition,
            alert=alert,
            targets=targets,
            evidence={"offer_id": "offer"},
        )
        self.assertTrue(result["transition_created"])
        self.assertEqual(result["deliveries_created"], 2)

        async with self.store.pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT COUNT(*) FROM product_states) AS states,
                    (SELECT COUNT(*) FROM stock_transitions) AS transitions,
                    (SELECT COUNT(*) FROM alert_events) AS alerts,
                    (SELECT COUNT(*) FROM alert_deliveries) AS deliveries
                """
            )
        self.assertEqual(dict(counts), {
            "states": 1,
            "transitions": 1,
            "alerts": 1,
            "deliveries": 2,
        })

    async def test_invalid_delivery_rolls_back_state_and_transition(self):
        scope = ScopeKey(
            "monitor",
            "market",
            "B000000001",
            "policy",
        )
        transition, alert, targets = self.writes(
            targets=("invalid",),
            target_kind="unsupported",
        )
        with self.assertRaises(Exception):
            await self.store.commit_stock_decision(
                scope,
                self.state(),
                None,
                transition=transition,
                alert=alert,
                targets=targets,
                evidence={"offer_id": "offer"},
            )
        async with self.store.pool.acquire() as connection:
            rows = await connection.fetchval(
                """
                SELECT
                    (SELECT COUNT(*) FROM product_states)
                    + (SELECT COUNT(*) FROM stock_transitions)
                    + (SELECT COUNT(*) FROM alert_events)
                """
            )
        self.assertEqual(rows, 0)

    async def test_credential_lease_and_cooldown_survive_new_governor(self):
        first = PostgresCredentialGovernor(
            self.store.pool,
            base_interval=5,
        )
        await first.initialize()
        key = "tvss-integration"
        self.assertTrue(await first.acquire_leader(key, "replica-a", 30))
        self.assertFalse(await first.acquire_leader(key, "replica-b", 30))
        permit = await first.acquire_permit(key, RequestClass.POLL)
        limited = await first.record_result(permit, 429)

        restarted = PostgresCredentialGovernor(
            self.store.pool,
            base_interval=5,
        )
        snapshot = await restarted.snapshot(key)
        self.assertEqual(snapshot.blocked_until, limited.blocked_until)
        self.assertTrue(snapshot.half_open_pending)

    async def test_claim_limits_are_global_and_per_target(self):
        for index in range(3):
            asin = f"B00000000{index + 1}"
            scope = ScopeKey(
                "monitor",
                "market",
                asin,
                "policy",
            )
            transition, alert, targets = self.writes(targets=("same",))
            state = self.state(sequence=index + 1)
            state["scope_key"] = f"monitor:market:{asin}:policy"
            await self.store.commit_stock_decision(
                scope,
                state,
                None,
                transition=transition,
                alert=alert,
                targets=targets,
                evidence={"offer_id": "offer"},
            )
        first = await self.store.claim_deliveries(
            "replica-a",
            limit=32,
            lease_seconds=30,
            global_limit=32,
            per_target_limit=2,
        )
        second = await self.store.claim_deliveries(
            "replica-b",
            limit=32,
            lease_seconds=30,
            global_limit=32,
            per_target_limit=2,
        )
        self.assertEqual(len(first), 2)
        self.assertEqual(len(second), 0)

    async def test_persisted_target_circuit_opens_after_five_failures(self):
        scope = ScopeKey(
            "monitor",
            "market",
            "B000000001",
            "policy",
        )
        transition, alert, targets = self.writes(targets=("same",))
        await self.store.commit_stock_decision(
            scope,
            self.state(),
            None,
            transition=transition,
            alert=alert,
            targets=targets,
            evidence={"offer_id": "offer"},
        )
        delivery_id = targets[0].delivery_id
        for attempt in range(5):
            claimed = await self.store.claim_deliveries(
                f"replica-{attempt}",
                limit=1,
                lease_seconds=30,
            )
            self.assertEqual(len(claimed), 1)
            await self.store.reschedule_delivery(
                delivery_id,
                delay_seconds=0,
                duration_ms=1,
                error_class="upstream",
                http_status=503,
            )
        blocked = await self.store.claim_deliveries(
            "replica-final",
            limit=1,
            lease_seconds=30,
        )
        self.assertEqual(blocked, [])
        backlog = await self.store.delivery_backlog()
        self.assertEqual(backlog["open_circuits"], 1)

    async def test_replay_and_suppress_refresh_alert_lifecycle(self):
        scope = ScopeKey(
            "monitor",
            "market",
            "B000000001",
            "policy",
        )
        transition, alert, targets = self.writes(targets=("same",))
        await self.store.commit_stock_decision(
            scope,
            self.state(),
            None,
            transition=transition,
            alert=alert,
            targets=targets,
            evidence={"offer_id": "offer"},
        )
        delivery_id = targets[0].delivery_id
        await self.store.claim_deliveries(
            "replica-a",
            limit=1,
            lease_seconds=30,
        )
        await self.store.dead_letter_delivery(
            delivery_id,
            reason="upstream",
            duration_ms=1,
            error_class="upstream",
            http_status=503,
        )
        self.assertTrue(await self.store.replay_delivery(delivery_id))
        async with self.store.pool.acquire() as connection:
            lifecycle = await connection.fetchval(
                """
                SELECT lifecycle
                FROM alert_events
                WHERE alert_id = $1
                """,
                alert.alert_id,
            )
        self.assertEqual(lifecycle, "accepted")
        self.assertTrue(await self.store.suppress_delivery(delivery_id))
        async with self.store.pool.acquire() as connection:
            lifecycle = await connection.fetchval(
                """
                SELECT lifecycle
                FROM alert_events
                WHERE alert_id = $1
                """,
                alert.alert_id,
            )
        self.assertEqual(lifecycle, "suppressed")


if __name__ == "__main__":
    unittest.main()
