import asyncio
import json
import os
import unittest
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

from credential_governor import (
    CalibrationKey,
    CredentialLeaseFenceLost,
    PostgresCadenceCalibrationStore,
    PostgresCredentialGovernor,
    RequestClass,
)
from durable_store import (
    AlertWrite,
    BatchStockDecision,
    PostgresStore,
    ScopeKey,
    TargetWrite,
    TransitionWrite,
    VerificationWrite,
    DELIVERY_CLAIM_LOCK_ID,
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
                    credential_cadence_calibrations,
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
            migrations_rls_enabled = await connection.fetchval(
                """
                SELECT relrowsecurity
                FROM pg_class
                WHERE oid = 'public.monitor_schema_migrations'::regclass
                """
            )
            circuits = await connection.fetchval(
                """
                SELECT to_regclass(
                    'public.alert_target_circuits'
                ) IS NOT NULL
                """
            )
        self.assertEqual(versions, 4)
        self.assertTrue(migrations_rls_enabled)
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

    async def test_batch_commit_returns_ids_and_rolls_back_every_scope(self):
        first = ScopeKey("monitor", "market", "B000000001", "policy")
        second = ScopeKey("monitor", "market", "B000000002", "policy")
        transition, alert, targets = self.writes()
        second_transition, second_alert, second_targets = self.writes(
            targets=("invalid",), target_kind="unsupported"
        )
        with self.assertRaises(Exception):
            await self.store.commit_stock_decisions((
                BatchStockDecision(
                    first, self.state(), None, {"offer_id": "offer"},
                    transition, alert, targets,
                    VerificationWrite(1, {"offer_id": "offer"}),
                ),
                BatchStockDecision(
                    second,
                    {**self.state(), "scope_key": "monitor:market:B000000002:policy"},
                    None, {"offer_id": "offer"}, second_transition,
                    second_alert, second_targets,
                ),
            ))
        async with self.store.pool.acquire() as connection:
            self.assertEqual(
                await connection.fetchval(
                    "SELECT COUNT(*) FROM product_states"
                ),
                0,
            )

        second_transition, second_alert, second_targets = self.writes(
            targets=("three",)
        )
        result = await self.store.commit_stock_decisions((
            BatchStockDecision(
                first, self.state(), None, {"offer_id": "offer"},
                transition, alert, targets,
                VerificationWrite(1, {"offer_id": "offer"}),
            ),
            BatchStockDecision(
                second,
                {**self.state(), "scope_key": "monitor:market:B000000002:policy"},
                None, {"offer_id": "offer"}, second_transition,
                second_alert, second_targets,
            ),
        ))
        self.assertEqual(set(result.transition_ids), {
            transition.transition_id, second_transition.transition_id,
        })
        self.assertEqual(set(result.delivery_ids), {
            *(target.delivery_id for target in targets),
            *(target.delivery_id for target in second_targets),
        })
        self.assertEqual(len(result.verification_job_ids), 1)

    async def test_batch_commit_preleases_with_global_and_target_capacity(self):
        decisions = []
        target_ids = ("same", "same", "other", "third")
        writes = []
        for index, target_id in enumerate(target_ids, start=1):
            asin = f"B0000000{index:02d}"
            scope = ScopeKey("monitor", "market", asin, "policy")
            transition, alert, targets = self.writes(targets=(target_id,))
            writes.append((transition, alert, targets))
            decisions.append(
                BatchStockDecision(
                    scope,
                    {
                        **self.state(),
                        "scope_key": f"monitor:market:{asin}:policy",
                    },
                    None,
                    {"offer_id": f"offer-{index}"},
                    transition,
                    alert,
                    targets,
                )
            )

        result = await self.store.commit_stock_decisions(
            decisions,
            prelease_worker_id="commit-worker",
            prelease_lease_seconds=30,
            prelease_global_limit=3,
            prelease_per_target_limit=1,
        )
        self.assertEqual(len(result.delivery_ids), 4)
        self.assertEqual(len(result.preleased_delivery_ids), 3)
        async with self.store.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT delivery_id, target_id, status, attempts, leased_by,
                       first_attempt_at, last_attempt_at
                FROM alert_deliveries
                ORDER BY target_id, delivery_id
                """
            )
        leased = [row for row in rows if row["status"] == "leased"]
        pending = [row for row in rows if row["status"] == "pending"]
        self.assertEqual(
            {row["target_id"] for row in leased},
            {"same", "other", "third"},
        )
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["target_id"], "same")
        self.assertTrue(
            all(
                row["attempts"] == 0
                and row["leased_by"] == "commit-worker"
                and row["first_attempt_at"] is None
                and row["last_attempt_at"] is None
                for row in leased
            )
        )
        self.assertEqual(pending[0]["attempts"], 0)
        self.assertIsNone(pending[0]["leased_by"])
        await self.store.mark_delivery_succeeded(
            leased[0]["delivery_id"],
            duration_ms=1,
            attempts=1,
        )
        async with self.store.pool.acquire() as connection:
            completed = await connection.fetchrow(
                """
                SELECT status, attempts, first_attempt_at, last_attempt_at
                FROM alert_deliveries
                WHERE delivery_id = $1
                """,
                leased[0]["delivery_id"],
            )
        self.assertEqual(completed["status"], "succeeded")
        self.assertEqual(completed["attempts"], 1)
        self.assertIsNotNone(completed["first_attempt_at"])
        self.assertIsNotNone(completed["last_attempt_at"])

    async def test_prelease_respects_existing_shared_capacity(self):
        first_scope = ScopeKey(
            "monitor", "market", "B000000001", "policy"
        )
        first_transition, first_alert, first_targets = self.writes(
            targets=("shared",)
        )
        first = await self.store.commit_stock_decisions(
            (
                BatchStockDecision(
                    first_scope,
                    self.state(),
                    None,
                    {"offer_id": "first"},
                    first_transition,
                    first_alert,
                    first_targets,
                ),
            ),
            prelease_worker_id="first-worker",
            prelease_global_limit=1,
            prelease_per_target_limit=1,
        )
        self.assertEqual(
            first.preleased_delivery_ids,
            (first_targets[0].delivery_id,),
        )

        second_scope = ScopeKey(
            "monitor", "market", "B000000002", "policy"
        )
        second_transition, second_alert, second_targets = self.writes(
            targets=("other",)
        )
        second = await self.store.commit_stock_decisions(
            (
                BatchStockDecision(
                    second_scope,
                    {
                        **self.state(),
                        "scope_key": (
                            "monitor:market:B000000002:policy"
                        ),
                    },
                    None,
                    {"offer_id": "second"},
                    second_transition,
                    second_alert,
                    second_targets,
                ),
            ),
            prelease_worker_id="second-worker",
            prelease_global_limit=1,
            prelease_per_target_limit=1,
        )
        self.assertEqual(second.preleased_delivery_ids, ())
        async with self.store.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT status, attempts, leased_by
                FROM alert_deliveries
                WHERE delivery_id = $1
                """,
                second_targets[0].delivery_id,
            )
        self.assertEqual(row["status"], "pending")
        self.assertEqual(row["attempts"], 0)
        self.assertIsNone(row["leased_by"])

    async def test_cancelled_prelease_is_released_for_immediate_claim(self):
        scope = ScopeKey("monitor", "market", "B000000001", "policy")
        transition, alert, targets = self.writes(targets=("cancelled",))
        result = await self.store.commit_stock_decisions(
            (
                BatchStockDecision(
                    scope,
                    self.state(),
                    None,
                    {"offer_id": "offer"},
                    transition,
                    alert,
                    targets,
                ),
            ),
            prelease_worker_id="cancelled-worker",
            prelease_global_limit=1,
            prelease_per_target_limit=1,
        )
        released = await self.store.release_preleased_deliveries(
            "cancelled-worker",
            [str(result.preleased_delivery_ids[0])],
        )
        self.assertEqual(released, result.preleased_delivery_ids)
        recovered = await self.store.claim_deliveries(
            "recovery-worker",
            limit=1,
            lease_seconds=30,
            global_limit=1,
            per_target_limit=1,
        )
        self.assertEqual(
            [row["delivery_id"] for row in recovered],
            [targets[0].delivery_id],
        )
        self.assertEqual(recovered[0]["attempts"], 1)
        self.assertEqual(recovered[0]["leased_by"], "recovery-worker")

    async def test_prelease_respects_open_target_circuit(self):
        scope = ScopeKey("monitor", "market", "B000000001", "policy")
        transition, alert, targets = self.writes(targets=("open-target",))
        async with self.store.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO alert_target_circuits (
                    target_id, consecutive_failures, open_until
                ) VALUES (
                    'open-target', 5, clock_timestamp() + INTERVAL '1 minute'
                )
                """
            )
        result = await self.store.commit_stock_decisions(
            (
                BatchStockDecision(
                    scope,
                    self.state(),
                    None,
                    {"offer_id": "offer"},
                    transition,
                    alert,
                    targets,
                ),
            ),
            prelease_worker_id="commit-worker",
        )
        self.assertEqual(result.preleased_delivery_ids, ())
        async with self.store.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT status, attempts, leased_by
                FROM alert_deliveries
                WHERE delivery_id = $1
                """,
                targets[0].delivery_id,
            )
        self.assertEqual(row["status"], "pending")
        self.assertEqual(row["attempts"], 0)
        self.assertIsNone(row["leased_by"])

    async def test_expired_commit_prelease_is_recovered_by_claim_loop(self):
        scope = ScopeKey("monitor", "market", "B000000001", "policy")
        transition, alert, targets = self.writes(targets=("recovery",))
        result = await self.store.commit_stock_decisions(
            (
                BatchStockDecision(
                    scope,
                    self.state(),
                    None,
                    {"offer_id": "offer"},
                    transition,
                    alert,
                    targets,
                ),
            ),
            prelease_worker_id="commit-worker",
            prelease_lease_seconds=30,
        )
        self.assertEqual(result.preleased_delivery_ids, (targets[0].delivery_id,))
        self.assertEqual(
            await self.store.claim_deliveries(
                "recovery-worker",
                limit=1,
                lease_seconds=30,
                global_limit=1,
                per_target_limit=1,
            ),
            [],
        )
        async with self.store.pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE alert_deliveries
                SET leased_until = clock_timestamp() - INTERVAL '1 second'
                WHERE delivery_id = $1
                """,
                targets[0].delivery_id,
            )
        recovered = await self.store.claim_deliveries(
            "recovery-worker",
            limit=1,
            lease_seconds=30,
            global_limit=1,
            per_target_limit=1,
        )
        self.assertEqual(
            [row["delivery_id"] for row in recovered],
            [targets[0].delivery_id],
        )
        self.assertEqual(recovered[0]["leased_by"], "recovery-worker")
        self.assertEqual(recovered[0]["attempts"], 1)

    async def test_batch_commit_notifies_only_after_delivery_commit(self):
        scope = ScopeKey("monitor", "market", "B000000001", "policy")
        transition, alert, targets = self.writes()
        received = []
        visible_delivery_ids = []
        notified = asyncio.Event()
        async with self.store.pool.acquire() as listener:
            def on_notification(_connection, _pid, _channel, payload):
                received.append(json.loads(payload))
                visible_delivery_ids.append(asyncio.create_task(
                    self._visible_delivery_ids(received[-1]["delivery_ids"])
                ))
                notified.set()

            await listener.add_listener("alert_outbox_ready", on_notification)
            try:
                result = await self.store.commit_stock_decisions((
                    BatchStockDecision(
                        scope, self.state(), None, {"offer_id": "offer"},
                        transition, alert, targets,
                    ),
                ))
                await asyncio.wait_for(notified.wait(), timeout=1.0)
            finally:
                await listener.remove_listener("alert_outbox_ready", on_notification)
        self.assertEqual(
            set(received[0]["delivery_ids"]),
            {str(delivery_id) for delivery_id in result.delivery_ids},
        )
        self.assertEqual(len(received), 1)
        visible = await asyncio.gather(*visible_delivery_ids)
        self.assertEqual(
            set(visible[0]),
            set(received[0]["delivery_ids"]),
        )
        async with self.store.pool.acquire() as connection:
            self.assertEqual(
                await connection.fetchval("SELECT COUNT(*) FROM alert_deliveries"),
                len(result.delivery_ids),
            )

    async def _visible_delivery_ids(self, delivery_ids):
        async with self.store.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT delivery_id::text
                FROM alert_deliveries
                WHERE delivery_id = ANY($1::uuid[])
                """,
                delivery_ids,
            )
        return [row["delivery_id"] for row in rows]

    async def test_batch_commit_uses_bounded_set_based_statements(self):
        scope_one = ScopeKey("monitor", "market", "B000000001", "policy")
        scope_two = ScopeKey("monitor", "market", "B000000002", "policy")
        first_transition, first_alert, first_targets = self.writes()
        second_transition, second_alert, second_targets = self.writes(
            targets=("three", "four")
        )
        async with self.store.pool.acquire() as connection:
            if not hasattr(connection, "add_query_logger"):
                self.skipTest("asyncpg query logger unavailable")

            class SingleConnectionPool:
                @asynccontextmanager
                async def acquire(self):
                    yield connection

            original_pool = self.store.pool
            self.store.pool = SingleConnectionPool()
            queries = []
            query_logger = lambda record: queries.append(record.query)
            connection.add_query_logger(query_logger)
            try:
                result = await self.store.commit_stock_decisions((
                    BatchStockDecision(
                        scope_one, self.state(), None, {"offer_id": "one"},
                        first_transition, first_alert, first_targets,
                        VerificationWrite(1, {"offer_id": "one"}),
                    ),
                    BatchStockDecision(
                        scope_two,
                        {**self.state(), "scope_key": "monitor:market:B000000002:policy"},
                        None, {"offer_id": "two"}, second_transition,
                        second_alert, second_targets,
                    ),
                ))
                await asyncio.sleep(0)
            finally:
                connection.remove_query_logger(query_logger)
                self.store.pool = original_pool
        self.assertEqual(len(result.transition_ids), 2)
        bulk_queries = [
            query for query in queries
            if any(table in query for table in (
                "product_states", "stock_transitions", "alert_events",
                "stock_verification_jobs",
            ))
        ]
        self.assertEqual(len(bulk_queries), 1)
        self.assertTrue(all("jsonb_to_recordset" in query for query in bulk_queries))

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

    async def test_calibration_survives_restart_and_429_invalidation(self):
        key = CalibrationKey(
            "tvss-calibrated",
            "market",
            "region-a",
            True,
            20,
        )
        calibrations = PostgresCadenceCalibrationStore(self.store.pool)
        await calibrations.record_validation(key, 0.5, 120)
        restarted = PostgresCadenceCalibrationStore(self.store.pool)
        loaded = await restarted.load(key)
        self.assertEqual(loaded.interval_seconds, 0.5)
        self.assertIsNone(loaded.invalidated_at)
        self.assertEqual(
            await restarted.invalidate_credential(
                key.credential_key, key.marketplace_id
            ),
            1,
        )
        self.assertIsNotNone((await restarted.load(key)).invalidated_at)

    async def test_leader_activation_rechecks_expiry_and_last_429(self):
        key = CalibrationKey(
            "tvss-activation",
            "market",
            "region-a",
            True,
            20,
        )
        governor = PostgresCredentialGovernor(
            self.store.pool, base_interval=5
        )
        self.assertTrue(
            await governor.acquire_leader(
                key.credential_key, "replica", 30
            )
        )
        calibrations = PostgresCadenceCalibrationStore(self.store.pool)
        await calibrations.record_validation(key, 0.5, 120)
        effective, _ = await calibrations.activate_for_leader(
            key, 0.5, "replica"
        )
        self.assertEqual(effective, 0.5)

        permit = await governor.acquire_permit(
            key.credential_key,
            RequestClass.POLL,
            owner_id="replica",
        )
        await governor.record_result(
            permit, 429, owner_id="replica"
        )
        effective, _ = await calibrations.activate_for_leader(
            key, 0.5, "replica"
        )
        self.assertEqual(effective, 5.0)

        await calibrations.record_validation(key, 0.5, 120)
        effective, _ = await calibrations.activate_for_leader(
            key, 0.5, "replica"
        )
        self.assertEqual(effective, 0.5)
        effective, _ = await calibrations.activate_for_leader(
            key,
            0.5,
            "replica",
            validity_seconds=0,
        )
        self.assertEqual(effective, 5.0)

    async def test_stale_credential_leader_cannot_commit_stock_or_outbox(self):
        governor = PostgresCredentialGovernor(self.store.pool, base_interval=5)
        key = "tvss-commit-fence"
        self.assertTrue(await governor.acquire_leader(key, "replica-old", 30))
        async with self.store.pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE credential_governor
                SET lease_owner = 'replica-new',
                    lease_expires_at =
                        EXTRACT(EPOCH FROM clock_timestamp()) + 30
                WHERE credential_key = $1
                """,
                key,
            )

        scope = ScopeKey("monitor", "market", "B000000001", "policy")
        transition, alert, targets = self.writes()
        with self.assertRaises(CredentialLeaseFenceLost):
            await self.store.commit_stock_decision(
                scope,
                self.state(),
                None,
                transition=transition,
                alert=alert,
                targets=targets,
                evidence={"offer_id": "offer"},
                lease_credential_key=key,
                lease_owner="replica-old",
            )

        async with self.store.pool.acquire() as connection:
            persisted = await connection.fetchval(
                """
                SELECT
                    (SELECT COUNT(*) FROM product_states)
                    + (SELECT COUNT(*) FROM stock_transitions)
                    + (SELECT COUNT(*) FROM alert_events)
                    + (SELECT COUNT(*) FROM alert_deliveries)
                """
            )
        self.assertEqual(persisted, 0)

    async def test_verification_claim_is_scoped_to_monitor_and_policy(self):
        scope = ScopeKey("monitor-a", "market", "B000000001", "policy-a")
        job_id = await self.store.enqueue_verification(
            scope,
            source_sequence=1,
            evidence={"offer_id": "offer"},
            ttl_seconds=30,
        )
        self.assertIsNotNone(job_id)

        wrong_monitor = await self.store.claim_verification_jobs(
            "worker-a", "monitor-b", "market", "policy-a", limit=1
        )
        wrong_policy = await self.store.claim_verification_jobs(
            "worker-a", "monitor-a", "market", "policy-b", limit=1
        )
        claimed = await self.store.claim_verification_jobs(
            "worker-a", "monitor-a", "market", "policy-a", limit=1
        )

        self.assertEqual(wrong_monitor, [])
        self.assertEqual(wrong_policy, [])
        self.assertEqual([row["job_id"] for row in claimed], [job_id])

    async def test_stale_verification_worker_cannot_finish_reclaimed_job(self):
        scope = ScopeKey("monitor", "market", "B000000001", "policy")
        job_id = await self.store.enqueue_verification(
            scope,
            source_sequence=1,
            evidence={"offer_id": "offer"},
            ttl_seconds=30,
        )
        first = await self.store.claim_verification_jobs(
            "worker-a", "monitor", "market", "policy", limit=1
        )
        self.assertEqual(len(first), 1)
        async with self.store.pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE stock_verification_jobs
                SET leased_until = clock_timestamp() - INTERVAL '1 second'
                WHERE job_id = $1
                """,
                job_id,
            )
        second = await self.store.claim_verification_jobs(
            "worker-b", "monitor", "market", "policy", limit=1
        )
        self.assertEqual(len(second), 1)

        self.assertFalse(
            await self.store.finish_verification(
                job_id, "worker-a", success=True
            )
        )
        self.assertTrue(
            await self.store.finish_verification(
                job_id, "worker-b", success=True
            )
        )
        async with self.store.pool.acquire() as connection:
            status = await connection.fetchval(
                "SELECT status FROM stock_verification_jobs WHERE job_id = $1",
                job_id,
            )
        self.assertEqual(status, "complete")

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

    async def test_saturated_target_does_not_hide_another_due_target(self):
        for index in range(10):
            asin = f"SAME{index:06d}"
            transition, alert, targets = self.writes(targets=("same",))
            state = self.state(sequence=index + 1)
            state["scope_key"] = f"monitor:market:{asin}:policy"
            await self.store.commit_stock_decision(
                ScopeKey("monitor", "market", asin, "policy"),
                state,
                None,
                transition=transition,
                alert=alert,
                targets=targets,
                evidence={"offer_id": "offer"},
            )
        other_asin = "OTHER00001"
        transition, alert, targets = self.writes(targets=("other",))
        state = self.state(sequence=11)
        state["scope_key"] = (
            f"monitor:market:{other_asin}:policy"
        )
        await self.store.commit_stock_decision(
            ScopeKey("monitor", "market", other_asin, "policy"),
            state,
            None,
            transition=transition,
            alert=alert,
            targets=targets,
            evidence={"offer_id": "offer"},
        )
        first = await self.store.claim_deliveries(
            "replica-a",
            limit=1,
            lease_seconds=30,
            per_target_limit=1,
        )
        second = await self.store.claim_deliveries(
            "replica-b",
            limit=1,
            lease_seconds=30,
            per_target_limit=1,
        )
        self.assertEqual(first[0]["target_id"], "same")
        self.assertEqual(second[0]["target_id"], "other")

    async def test_malformed_notification_id_falls_back_to_due_scan(self):
        scope = ScopeKey("monitor", "market", "B000000001", "policy")
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
        claimed = await self.store.claim_deliveries(
            "replica",
            limit=1,
            lease_seconds=30,
            preferred_delivery_ids=("not-a-uuid",),
        )
        self.assertEqual(len(claimed), 1)

    async def test_claim_query_has_an_executable_analyzed_plan(self):
        scope = ScopeKey("monitor", "market", "B000000001", "policy")
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
        async with self.store.pool.acquire() as connection:
            class SingleConnectionPool:
                @asynccontextmanager
                async def acquire(self):
                    yield connection

            queries = []
            logger = lambda record: queries.append(record.query)
            original_pool = self.store.pool
            self.store.pool = SingleConnectionPool()
            connection.add_query_logger(logger)
            try:
                await self.store.claim_deliveries(
                    "replica",
                    limit=1,
                    lease_seconds=30,
                )
                await asyncio.sleep(0)
            finally:
                connection.remove_query_logger(logger)
                self.store.pool = original_pool
            claim_query = next(
                query for query in queries if "WITH claim_lock" in query
            )
            plan = await connection.fetchval(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + claim_query,
                DELIVERY_CLAIM_LOCK_ID,
                1,
                "explain",
                30.0,
                32,
                2,
                [],
            )
        if isinstance(plan, str):
            plan = json.loads(plan)
        self.assertIn("Execution Time", plan[0])

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
