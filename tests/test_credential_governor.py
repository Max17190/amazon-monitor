import asyncio
import unittest

from credential_governor import (
    BASE_429_COOLDOWN_SECONDS,
    DEFAULT_INTERVAL_SECONDS,
    CredentialLeaseFenceLost,
    HalfOpenPollRequired,
    InMemoryCredentialGovernor,
    PostgresCredentialGovernor,
    RECOVERY_SUCCESS_COUNT,
    RequestClass,
    stable_credential_key,
)


class FakeClock:
    def __init__(self, now=100.0):
        self.now = now

    def __call__(self):
        return self.now


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


class FakeAsyncpgConnection:
    """Small asyncpg-shaped fake that persists the governor row in memory."""

    def __init__(self):
        self.rows = {}
        self.executed = []

    def transaction(self):
        return _AsyncContext(self)

    async def fetchrow(self, _sql, key):
        row = self.rows.get(key)
        return dict(row) if row is not None else None

    async def execute(self, sql, *args):
        self.executed.append(sql)
        if sql.startswith("INSERT INTO credential_governor"):
            key, interval = args
            self.rows[key] = {
                "interval_seconds": interval, "next_request_at": 0.0, "blocked_until": 0.0,
                "generation": 0, "consecutive_429": 0, "success_streak": 0,
                "half_open_pending": False, "lease_owner": None, "lease_expires_at": 0.0,
            }
        elif sql.startswith("UPDATE credential_governor"):
            key = args[0]
            self.rows[key] = {
                "interval_seconds": args[1], "next_request_at": args[2], "blocked_until": args[3],
                "generation": args[4], "consecutive_429": args[5], "success_streak": args[6],
                "half_open_pending": args[7], "lease_owner": args[8], "lease_expires_at": args[9],
            }


class FakeAsyncpgPool:
    def __init__(self):
        self.connection = FakeAsyncpgConnection()

    def acquire(self):
        return _AsyncContext(self.connection)


class ConcurrentCreatorConnection(FakeAsyncpgConnection):
    """Simulate another replica creating the row after the initial read."""

    def __init__(self):
        super().__init__()
        self.competing_insert = True

    async def execute(self, sql, *args):
        if sql.startswith("INSERT INTO credential_governor") and self.competing_insert:
            self.competing_insert = False
            self.rows[args[0]] = {
                "interval_seconds": 17.0, "next_request_at": 0.0, "blocked_until": 0.0,
                "generation": 0, "consecutive_429": 0, "success_streak": 0,
                "half_open_pending": False, "lease_owner": None, "lease_expires_at": 0.0,
            }
            if "ON CONFLICT (credential_key) DO NOTHING" not in sql:
                raise RuntimeError("duplicate key value violates unique constraint")
            self.executed.append(sql)
            return
        await super().execute(sql, *args)


class ConcurrentCreatorPool:
    def __init__(self):
        self.connection = ConcurrentCreatorConnection()

    def acquire(self):
        return _AsyncContext(self.connection)


class CredentialGovernorTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.governor = InMemoryCredentialGovernor(clock=self.clock)
        self.key = stable_credential_key("cookie=private", "test-salt")

    def await_(self, coroutine):
        return asyncio.run(coroutine)

    def test_credential_key_is_stable_and_does_not_include_secret(self):
        self.assertEqual(self.key, stable_credential_key("cookie=private", "test-salt"))
        self.assertNotEqual(self.key, stable_credential_key("cookie=other", "test-salt"))
        self.assertNotIn("private", self.key)

    def test_leader_lease_is_exclusive_then_reusable_after_expiry(self):
        self.assertTrue(self.await_(self.governor.acquire_leader(self.key, "one", 10)))
        self.assertFalse(self.await_(self.governor.acquire_leader(self.key, "two", 10)))
        self.assertTrue(self.await_(self.governor.renew_leader(self.key, "one", 20)))
        self.clock.now += 21
        self.assertTrue(self.await_(self.governor.acquire_leader(self.key, "two", 10)))
        self.assertFalse(self.await_(self.governor.release_leader(self.key, "one")))
        self.assertTrue(self.await_(self.governor.release_leader(self.key, "two")))

    def test_expired_leader_is_fenced_before_request_or_result_commit(self):
        self.assertTrue(
            self.await_(self.governor.acquire_leader(self.key, "old", 5))
        )
        permit = self.await_(
            self.governor.acquire_permit(
                self.key,
                RequestClass.POLL,
                owner_id="old",
            )
        )

        self.clock.now += 6
        self.assertTrue(
            self.await_(self.governor.acquire_leader(self.key, "new", 30))
        )
        with self.assertRaises(CredentialLeaseFenceLost):
            self.await_(self.governor.ensure_leader(self.key, "old"))
        with self.assertRaises(CredentialLeaseFenceLost):
            self.await_(
                self.governor.acquire_permit(
                    self.key,
                    RequestClass.POLL,
                    owner_id="old",
                )
            )
        with self.assertRaises(CredentialLeaseFenceLost):
            self.await_(self.governor.record_result(permit, 200))

    def test_permits_share_one_credential_cadence(self):
        first = self.await_(self.governor.acquire_permit(self.key, RequestClass.POLL))
        second = self.await_(self.governor.acquire_permit(self.key, RequestClass.CONFIRM))
        self.assertEqual(first.scheduled_at, 100.0)
        self.assertEqual(second.scheduled_at, 105.0)
        self.assertEqual(second.wait_seconds, 5.0)

    def test_429_persists_cooldown_doubles_interval_and_allows_one_probe(self):
        permit = self.await_(self.governor.acquire_permit(self.key, RequestClass.POLL))
        snapshot = self.await_(self.governor.record_result(permit, 429))
        self.assertEqual(snapshot.interval_seconds, DEFAULT_INTERVAL_SECONDS * 2)
        self.assertEqual(snapshot.blocked_until, 100.0 + BASE_429_COOLDOWN_SECONDS)
        self.assertTrue(snapshot.half_open_pending)

        probe = self.await_(self.governor.acquire_permit(self.key, RequestClass.POLL))
        later = self.await_(self.governor.acquire_permit(self.key, RequestClass.POLL))
        self.assertTrue(probe.half_open_probe)
        self.assertFalse(later.half_open_probe)
        self.assertEqual(probe.scheduled_at, snapshot.blocked_until)
        self.assertEqual(later.scheduled_at, probe.scheduled_at + snapshot.interval_seconds)

    def test_half_open_recovery_rejects_non_poll_traffic(self):
        permit = self.await_(
            self.governor.acquire_permit(self.key, RequestClass.POLL)
        )
        self.await_(self.governor.record_result(permit, 429))
        with self.assertRaises(HalfOpenPollRequired):
            self.await_(
                self.governor.acquire_permit(
                    self.key,
                    RequestClass.CONFIRM,
                )
            )
        probe = self.await_(
            self.governor.acquire_permit(self.key, RequestClass.POLL)
        )
        self.assertTrue(probe.half_open_probe)

    def test_stale_result_cannot_clear_newer_429_state(self):
        first = self.await_(self.governor.acquire_permit(self.key, RequestClass.POLL))
        second = self.await_(self.governor.acquire_permit(self.key, RequestClass.POLL))
        limited = self.await_(self.governor.record_result(second, 429))
        after_stale_success = self.await_(self.governor.record_result(first, 200))
        self.assertEqual(after_stale_success.generation, limited.generation)
        self.assertEqual(after_stale_success.blocked_until, limited.blocked_until)

    def test_retry_after_longer_than_adaptive_cap_is_honored(self):
        permit = self.await_(
            self.governor.acquire_permit(self.key, RequestClass.POLL)
        )
        snapshot = self.await_(
            self.governor.record_result(
                permit,
                429,
                retry_after_seconds=7200,
            )
        )
        self.assertEqual(snapshot.blocked_until, self.clock.now + 7200)

    def test_poll_success_recovers_only_after_full_window(self):
        first = self.await_(self.governor.acquire_permit(self.key, RequestClass.POLL))
        self.await_(self.governor.record_result(first, 429))
        self.clock.now += BASE_429_COOLDOWN_SECONDS
        for _ in range(RECOVERY_SUCCESS_COUNT - 1):
            permit = self.await_(self.governor.acquire_permit(self.key, RequestClass.POLL))
            self.await_(self.governor.record_result(permit, 200))
            self.clock.now = permit.scheduled_at + 10
        before = self.await_(self.governor.snapshot(self.key))
        self.assertEqual(before.interval_seconds, 10.0)
        permit = self.await_(self.governor.acquire_permit(self.key, RequestClass.POLL))
        after = self.await_(self.governor.record_result(permit, 200))
        self.assertEqual(after.interval_seconds, 9.75)

    def test_confirm_success_does_not_accelerate_recovery(self):
        permit = self.await_(self.governor.acquire_permit(self.key, RequestClass.POLL))
        self.await_(self.governor.record_result(permit, 429))
        self.clock.now += BASE_429_COOLDOWN_SECONDS
        self.await_(self.governor.acquire_permit(self.key, RequestClass.POLL))
        for _ in range(RECOVERY_SUCCESS_COUNT + 2):
            confirm = self.await_(self.governor.acquire_permit(self.key, RequestClass.CONFIRM))
            self.await_(self.governor.record_result(confirm, 200))
            self.clock.now = confirm.scheduled_at + 10
        self.assertEqual(self.await_(self.governor.snapshot(self.key)).interval_seconds, 10.0)

    def test_postgres_governor_persists_state_through_asyncpg_style_pool(self):
        pool = FakeAsyncpgPool()
        postgres = PostgresCredentialGovernor(pool, clock=self.clock)
        self.await_(postgres.initialize())
        self.assertTrue(self.await_(postgres.acquire_leader(self.key, "replica-a", 30)))
        permit = self.await_(postgres.acquire_permit(self.key, RequestClass.POLL))
        limited = self.await_(postgres.record_result(permit, 429, retry_after_seconds=1200))

        restarted = PostgresCredentialGovernor(pool, clock=self.clock)
        next_permit = self.await_(restarted.acquire_permit(self.key, RequestClass.POLL))
        self.assertEqual(next_permit.scheduled_at, limited.blocked_until)
        self.assertTrue(next_permit.half_open_probe)

    def test_postgres_governor_initialization_is_safe_when_another_replica_wins_insert(self):
        pool = ConcurrentCreatorPool()
        governor = PostgresCredentialGovernor(pool, clock=self.clock)

        self.assertTrue(self.await_(governor.acquire_leader(self.key, "replica-a", 30)))
        self.assertEqual(
            self.await_(governor.snapshot(self.key)).interval_seconds,
            17.0,
        )
        self.assertTrue(
            any(
                "ON CONFLICT (credential_key) DO NOTHING" in sql
                for sql in pool.connection.executed
            )
        )


if __name__ == "__main__":
    unittest.main()
