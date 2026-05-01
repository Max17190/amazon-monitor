import asyncio
import json
import logging
import os
import sys
import types
import unittest
from unittest.mock import patch

from amazon_tvss import TVSSClient

logging.disable(logging.CRITICAL)

if "discord" not in sys.modules:
    discord_stub = types.ModuleType("discord")

    class HTTPException(Exception):
        status = None

    class Color:
        @staticmethod
        def purple():
            return 0

    class Embed:
        def __init__(self, *args, **kwargs):
            self.fields = []

        def add_field(self, *args, **kwargs):
            self.fields.append((args, kwargs))

        def set_thumbnail(self, *args, **kwargs):
            return None

        def set_footer(self, *args, **kwargs):
            return None

    class Webhook:
        @classmethod
        def from_url(cls, *args, **kwargs):
            return cls()

        async def send(self, *args, **kwargs):
            return None

    discord_stub.HTTPException = HTTPException
    discord_stub.Color = Color
    discord_stub.Embed = Embed
    discord_stub.Webhook = Webhook
    sys.modules["discord"] = discord_stub

from main import (
    AlertDispatcher,
    AlertState,
    AuthFailureWatch,
    MonitorConfigError,
    build_generic_payload,
    load_monitor_config,
)
from webhooks import WebhookTarget, load_webhook_targets


class MonitorConfigTests(unittest.TestCase):
    def test_load_monitor_config_from_env_json(self):
        targets = {
            "PRIMARY": WebhookTarget(
                name="PRIMARY",
                url="https://discord.example/webhook",
                role_id="123",
            )
        }
        env = {
            "MONITOR_CONFIG_JSON": json.dumps(
                {
                    "groups": [
                        {
                            "name": "NVIDIA",
                            "asins": ["b0dt7l98j1", "B0DTJFSSZG"],
                            "webhooks": ["PRIMARY"],
                        }
                    ]
                }
            ),
            "POLL_INTERVAL_SECONDS": "2",
        }

        config = load_monitor_config(env=env, webhook_targets=targets)

        self.assertEqual(config.poll_interval_seconds, 2.0)
        self.assertEqual(config.groups[0].name, "NVIDIA")
        self.assertEqual(config.groups[0].asins, ["B0DT7L98J1", "B0DTJFSSZG"])
        self.assertEqual(config.groups[0].webhook_names, ["PRIMARY"])

    def test_rejects_invalid_asin(self):
        targets = {
            "PRIMARY": WebhookTarget(
                name="PRIMARY",
                url="https://discord.example/webhook",
            )
        }
        env = {
            "MONITOR_CONFIG_JSON": json.dumps(
                {
                    "groups": [
                        {
                            "name": "Bad",
                            "asins": ["BAD"],
                            "webhooks": ["PRIMARY"],
                        }
                    ]
                }
            )
        }

        with self.assertRaises(MonitorConfigError):
            load_monitor_config(env=env, webhook_targets=targets)

    def test_rejects_unknown_webhook(self):
        env = {
            "MONITOR_CONFIG_JSON": json.dumps(
                {
                    "groups": [
                        {
                            "name": "NVIDIA",
                            "asins": ["B0DT7L98J1"],
                            "webhooks": ["MISSING"],
                        }
                    ]
                }
            )
        }

        with self.assertRaises(MonitorConfigError):
            load_monitor_config(env=env, webhook_targets={})

    def test_rejects_zero_poll_interval(self):
        targets = {
            "PRIMARY": WebhookTarget(
                name="PRIMARY",
                url="https://discord.example/webhook",
            )
        }
        env = {
            "MONITOR_CONFIG_JSON": json.dumps(
                {
                    "groups": [
                        {
                            "name": "NVIDIA",
                            "asins": ["B0DT7L98J1"],
                            "webhooks": ["PRIMARY"],
                        }
                    ]
                }
            ),
            "POLL_INTERVAL_SECONDS": "0",
        }

        with self.assertRaises(MonitorConfigError):
            load_monitor_config(env=env, webhook_targets=targets)

    def test_require_amazon_seller_default_true(self):
        targets = {
            "PRIMARY": WebhookTarget(name="PRIMARY", url="https://x/y"),
        }
        env = {
            "MONITOR_CONFIG_JSON": json.dumps(
                {"groups": [{"name": "G", "asins": ["B0DT7L98J1"], "webhooks": ["PRIMARY"]}]}
            ),
        }
        config = load_monitor_config(env=env, webhook_targets=targets)
        self.assertTrue(config.require_amazon_seller)

    def test_require_amazon_seller_parses_truthy(self):
        targets = {
            "PRIMARY": WebhookTarget(name="PRIMARY", url="https://x/y"),
        }
        for raw in ("1", "true", "TRUE", "yes", "on"):
            env = {
                "MONITOR_CONFIG_JSON": json.dumps(
                    {"groups": [{"name": "G", "asins": ["B0DT7L98J1"], "webhooks": ["PRIMARY"]}]}
                ),
                "MONITOR_REQUIRE_AMAZON_SELLER": raw,
            }
            config = load_monitor_config(env=env, webhook_targets=targets)
            self.assertTrue(config.require_amazon_seller, f"value {raw!r} should parse truthy")

    def test_require_amazon_seller_can_be_disabled(self):
        targets = {
            "PRIMARY": WebhookTarget(name="PRIMARY", url="https://x/y"),
        }
        for raw in ("0", "false", "FALSE", "no", "off"):
            env = {
                "MONITOR_CONFIG_JSON": json.dumps(
                    {"groups": [{"name": "G", "asins": ["B0DT7L98J1"], "webhooks": ["PRIMARY"]}]}
                ),
                "MONITOR_REQUIRE_AMAZON_SELLER": raw,
            }
            config = load_monitor_config(env=env, webhook_targets=targets)
            self.assertFalse(config.require_amazon_seller, f"value {raw!r} should parse falsy")


class WebhookTargetTests(unittest.TestCase):
    def test_load_webhook_targets_discord_default(self):
        targets = load_webhook_targets(
            {
                "WEBHOOK_PRIMARY_URL": "https://discord.example/webhook",
                "WEBHOOK_PRIMARY_ROLE_ID": "123",
            }
        )

        self.assertEqual(targets["PRIMARY"].url, "https://discord.example/webhook")
        self.assertEqual(targets["PRIMARY"].role_id, "123")
        self.assertEqual(targets["PRIMARY"].kind, "discord")

    def test_load_webhook_targets_generic_kind(self):
        targets = load_webhook_targets(
            {
                "WEBHOOK_NOTIFY_URL": "https://example.com/hook",
                "WEBHOOK_NOTIFY_KIND": "generic",
            }
        )

        self.assertEqual(targets["NOTIFY"].kind, "generic")
        self.assertEqual(targets["NOTIFY"].url, "https://example.com/hook")

    def test_load_webhook_targets_unknown_kind_falls_back_to_discord(self):
        targets = load_webhook_targets(
            {
                "WEBHOOK_FOO_URL": "https://example.com/hook",
                "WEBHOOK_FOO_KIND": "slack",
            }
        )

        self.assertEqual(targets["FOO"].kind, "discord")


class AlertStateTests(unittest.TestCase):
    def test_alerts_only_on_false_to_true_transition(self):
        state = AlertState()

        self.assertFalse(state.observe("B0DT7L98J1", False))
        self.assertTrue(state.observe("B0DT7L98J1", True))
        self.assertFalse(state.observe("B0DT7L98J1", True))
        self.assertFalse(state.observe("B0DT7L98J1", False))
        self.assertTrue(state.observe("B0DT7L98J1", True))

    def test_first_observation_does_not_alert_even_when_in_stock(self):
        state = AlertState()

        self.assertFalse(state.observe("B0DT7L98J1", True))
        self.assertFalse(state.observe("B0DT7L98J1", True))
        self.assertFalse(state.observe("B0DT7L98J1", False))
        self.assertTrue(state.observe("B0DT7L98J1", True))

    def test_peek_does_not_mutate_state(self):
        state = AlertState()
        state.commit("X", False)

        self.assertTrue(state.peek("X", True))
        self.assertTrue(state.peek("X", True))
        self.assertTrue(state.peek("X", True))

        state.commit("X", True)
        self.assertFalse(state.peek("X", True))

    def test_peek_returns_false_for_unseen_asin(self):
        state = AlertState()
        self.assertFalse(state.peek("UNSEEN", True))


class GenericPayloadTests(unittest.TestCase):
    def test_payload_contains_documented_fields(self):
        product = {
            "asin": "B0DT7L98J1",
            "title": "Test",
            "in_stock": True,
            "price": "$99.00",
            "link": "https://www.amazon.com/dp/B0DT7L98J1",
            "images": ["https://example.com/img.jpg"],
            "seller": "Amazon.com",
            "source": "tvss",
        }

        payload = build_generic_payload(product, group_name="NVIDIA")

        self.assertEqual(payload["asin"], "B0DT7L98J1")
        self.assertEqual(payload["title"], "Test")
        self.assertTrue(payload["in_stock"])
        self.assertEqual(payload["price"], "$99.00")
        self.assertEqual(payload["link"], "https://www.amazon.com/dp/B0DT7L98J1")
        self.assertEqual(payload["image"], "https://example.com/img.jpg")
        self.assertEqual(payload["seller"], "Amazon.com")
        self.assertEqual(payload["source"], "tvss")
        self.assertEqual(payload["group"], "NVIDIA")
        self.assertRegex(payload["ts"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

    def test_payload_handles_missing_image(self):
        payload = build_generic_payload({"asin": "X", "in_stock": False}, "G")
        self.assertIsNone(payload["image"])
        self.assertFalse(payload["in_stock"])

    def test_payload_uses_provided_timestamp(self):
        from datetime import datetime, timezone
        ts = datetime(2026, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        payload = build_generic_payload({"asin": "X", "in_stock": True}, "G", ts=ts)
        self.assertEqual(payload["ts"], "2026-01-15T12:30:45Z")


class FakeClock:
    def __init__(self, t=0.0):
        self.t = float(t)

    def __call__(self):
        return self.t


class AuthFailureWatchTests(unittest.TestCase):
    def test_not_tripped_at_start(self):
        clock = FakeClock(0.0)
        watch = AuthFailureWatch(grace_seconds=10.0, clock=clock)
        self.assertFalse(watch.is_tripped())

    def test_not_tripped_without_auth_failure_even_after_grace(self):
        clock = FakeClock(0.0)
        watch = AuthFailureWatch(grace_seconds=10.0, clock=clock)
        clock.t = 1000.0
        watch.record_other_failure()
        self.assertFalse(watch.is_tripped())

    def test_trips_only_after_grace_with_auth_failure(self):
        clock = FakeClock(0.0)
        watch = AuthFailureWatch(grace_seconds=10.0, clock=clock)
        watch.record_auth_failure()
        clock.t = 5.0
        self.assertFalse(watch.is_tripped())
        clock.t = 11.0
        self.assertTrue(watch.is_tripped())

    def test_success_clears_armed_auth_failure(self):
        clock = FakeClock(0.0)
        watch = AuthFailureWatch(grace_seconds=10.0, clock=clock)
        watch.record_auth_failure()
        clock.t = 5.0
        watch.record_success()
        clock.t = 100.0
        self.assertFalse(watch.is_tripped())

    def test_other_failure_does_not_arm(self):
        clock = FakeClock(0.0)
        watch = AuthFailureWatch(grace_seconds=1.0, clock=clock)
        watch.record_other_failure()
        clock.t = 100.0
        self.assertFalse(watch.is_tripped())


class DispatcherBackoffTests(unittest.TestCase):
    def test_backoff_is_per_target(self):
        clock = FakeClock(0.0)
        dispatcher = AlertDispatcher(session=None, clock=clock)

        a = WebhookTarget(name="A", url="https://a", kind="discord")
        b = WebhookTarget(name="B", url="https://b", kind="discord")

        dispatcher._target_backoff_until["A"] = 10.0

        self.assertTrue(dispatcher._is_target_backed_off(a))
        self.assertFalse(dispatcher._is_target_backed_off(b))

        clock.t = 11.0
        self.assertFalse(dispatcher._is_target_backed_off(a))


class DispatcherDeliveredTests(unittest.TestCase):
    def _run(self, coro):
        return asyncio.run(coro)

    def test_send_with_no_targets_returns_false(self):
        dispatcher = AlertDispatcher(session=None, clock=FakeClock(0.0))
        result = self._run(dispatcher.send_notification({"asin": "X"}, []))
        self.assertFalse(result)

    def test_send_returns_true_on_success(self):
        dispatcher = AlertDispatcher(session=None, clock=FakeClock(0.0))

        async def ok(target, embed):
            return None

        dispatcher._send_discord = ok
        targets = [WebhookTarget(name="A", url="https://a", kind="discord")]
        self.assertTrue(self._run(dispatcher.send_notification({"asin": "X"}, targets)))

    def test_send_returns_false_when_all_targets_fail(self):
        dispatcher = AlertDispatcher(session=None, clock=FakeClock(0.0))

        async def boom(target, embed):
            raise RuntimeError("boom")

        dispatcher._send_discord = boom
        targets = [WebhookTarget(name="A", url="https://a", kind="discord")]
        self.assertFalse(self._run(dispatcher.send_notification({"asin": "X"}, targets)))

    def test_send_returns_true_when_partial_success(self):
        dispatcher = AlertDispatcher(session=None, clock=FakeClock(0.0))

        calls = {"n": 0}

        async def flaky(target, embed):
            calls["n"] += 1
            if target.name == "A":
                raise RuntimeError("boom")
            return None

        dispatcher._send_discord = flaky
        targets = [
            WebhookTarget(name="A", url="https://a", kind="discord"),
            WebhookTarget(name="B", url="https://b", kind="discord"),
        ]
        self.assertTrue(self._run(dispatcher.send_notification({"asin": "X"}, targets)))

    def test_skipped_target_does_not_count_as_delivered(self):
        clock = FakeClock(0.0)
        dispatcher = AlertDispatcher(session=None, clock=clock)
        dispatcher._target_backoff_until["A"] = 100.0

        targets = [WebhookTarget(name="A", url="https://a", kind="discord")]
        self.assertFalse(self._run(dispatcher.send_notification({"asin": "X"}, targets)))


class TVSSClientTests(unittest.TestCase):
    def make_client(self):
        with patch.dict(os.environ, {"TVSS_COOKIE_HEADER": "session-id=1"}, clear=False):
            return TVSSClient()

    def test_parse_product_normalizes_monitor_shape(self):
        client = self.make_client()

        product = client._parse_product(
            {
                "asin": "B000000001",
                "title": "Test Product",
                "offerId": "offer-1",
                "price": {"displayString": "$12.99"},
                "merchantInfo": {"merchantName": "Amazon.com", "soldByAmazon": True},
                "productImageUrls": ["https://example.com/image.jpg"],
                "productAvailabilityDetails": {"status": "IN_STOCK"},
            },
            "B000000001",
        )

        self.assertEqual(product["asin"], "B000000001")
        self.assertEqual(product["title"], "Test Product")
        self.assertTrue(product["in_stock"])
        self.assertEqual(product["price"], "$12.99")
        self.assertEqual(product["seller"], "Amazon.com")
        self.assertEqual(product["images"], ["https://example.com/image.jpg"])
        self.assertEqual(product["source"], "tvss")

    def test_parse_product_tolerates_malformed_payload(self):
        client = self.make_client()

        product = client._parse_product(None, "B000000001")

        self.assertEqual(product["asin"], "B000000001")
        self.assertEqual(product["title"], "N/A")
        self.assertFalse(product["in_stock"])
        self.assertEqual(product["images"], [])
        self.assertIsNone(product["price"])
        self.assertEqual(product["availability"], {})


class BatchProductsParseTests(unittest.TestCase):
    """Tests for batch_products response parsing (no network calls)."""

    def make_client(self):
        with patch.dict(os.environ, {"TVSS_COOKIE_HEADER": "session-id=1"}, clear=False):
            return TVSSClient()

    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_batch_response(self, client, response_data):
        """Replace _request with a coro that returns response_data."""
        async def fake_request(session, method, url, json_body=None):
            return response_data
        client._request = fake_request

    def test_in_stock_asin_has_offer(self):
        client = self.make_client()
        self._mock_batch_response(client, {
            "products": [
                {
                    "basicProduct": {"asin": "B0DT7L98J1", "title": "GPU"},
                    "basicOffer": {"offerId": "abc123", "price": "$1999.00", "badge": None},
                },
            ]
        })
        result = self._run(client.batch_products(None, ["B0DT7L98J1"]))
        self.assertTrue(result["B0DT7L98J1"]["has_offer"])
        self.assertEqual(result["B0DT7L98J1"]["price"], "$1999.00")

    def test_out_of_stock_asin_no_offer(self):
        client = self.make_client()
        self._mock_batch_response(client, {
            "products": [
                {
                    "basicProduct": {"asin": "B0DTJFSSZG", "title": "GPU"},
                    "basicOffer": {"offerId": None, "price": None, "badge": None},
                },
            ]
        })
        result = self._run(client.batch_products(None, ["B0DTJFSSZG"]))
        self.assertFalse(result["B0DTJFSSZG"]["has_offer"])
        self.assertIsNone(result["B0DTJFSSZG"]["price"])

    def test_missing_asin_filled_as_no_offer(self):
        client = self.make_client()
        self._mock_batch_response(client, {"products": []})
        result = self._run(client.batch_products(None, ["B000MISSING"]))
        self.assertFalse(result["B000MISSING"]["has_offer"])

    def test_mixed_stock_states(self):
        client = self.make_client()
        self._mock_batch_response(client, {
            "products": [
                {
                    "basicProduct": {"asin": "INSTOCK001"},
                    "basicOffer": {"offerId": "offer1", "price": "$10.00"},
                },
                {
                    "basicProduct": {"asin": "OUTSTOCK01"},
                    "basicOffer": {"offerId": None, "price": None},
                },
            ]
        })
        result = self._run(client.batch_products(None, ["INSTOCK001", "OUTSTOCK01"]))
        self.assertTrue(result["INSTOCK001"]["has_offer"])
        self.assertFalse(result["OUTSTOCK01"]["has_offer"])

    def test_tolerates_null_basic_offer(self):
        client = self.make_client()
        self._mock_batch_response(client, {
            "products": [
                {
                    "basicProduct": {"asin": "B000000001"},
                    "basicOffer": None,
                },
            ]
        })
        result = self._run(client.batch_products(None, ["B000000001"]))
        self.assertFalse(result["B000000001"]["has_offer"])


class AuthModuleTests(unittest.TestCase):
    def test_generate_device_has_required_fields(self):
        from amazon_auth import generate_device
        device = generate_device()
        self.assertEqual(device["domain"], "Device")
        self.assertEqual(device["device_type"], "A3NWHXTQ4EBCZS")
        self.assertEqual(len(device["device_serial"]), 24)
        self.assertTrue(all(c in "0123456789abcdef" for c in device["device_serial"]))

    def test_generate_device_unique_serial(self):
        from amazon_auth import generate_device
        serials = {generate_device()["device_serial"] for _ in range(10)}
        self.assertEqual(len(serials), 10)

    def test_cookies_to_header(self):
        from amazon_auth import cookies_to_header
        cookies = [
            {"name": "session-id", "value": "abc"},
            {"name": "at-main", "value": "xyz"},
        ]
        header = cookies_to_header(cookies)
        self.assertEqual(header, "session-id=abc; at-main=xyz")

    def test_cookies_to_header_skips_empty(self):
        from amazon_auth import cookies_to_header
        cookies = [
            {"name": "good", "value": "val"},
            {"name": "", "value": "skip"},
            {"name": "also-good", "value": "val2"},
        ]
        header = cookies_to_header(cookies)
        self.assertEqual(header, "good=val; also-good=val2")

    def test_save_and_load_auth_state(self):
        import tempfile
        from amazon_auth import save_auth_state, load_auth_state
        state = {
            "state": "authenticated",
            "cookies": [{"name": "session-id", "value": "test"}],
        }
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            save_auth_state(state, path)
            loaded = load_auth_state(path)
            self.assertEqual(loaded["state"], "authenticated")
            self.assertEqual(loaded["cookies"][0]["name"], "session-id")
            # Check file permissions (owner read/write only)
            mode = os.stat(path).st_mode & 0o777
            self.assertEqual(mode, 0o600)
        finally:
            os.unlink(path)

    def test_load_auth_state_returns_none_for_missing_file(self):
        from amazon_auth import load_auth_state
        self.assertIsNone(load_auth_state("/nonexistent/path/auth.json"))

    def test_tvss_client_loads_from_auth_json(self):
        import tempfile
        from amazon_auth import save_auth_state
        state = {
            "state": "authenticated",
            "cookies": [
                {"name": "session-id", "value": "test-sid"},
                {"name": "at-main", "value": "test-token"},
            ],
        }
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            save_auth_state(state, path)
            with patch.dict(os.environ, {"AUTH_STATE_PATH": path}, clear=False):
                # No TVSS_COOKIE_HEADER or TVSS_COOKIES_JSON set
                env_backup = {}
                for key in ("TVSS_COOKIE_HEADER", "AMAZON_COOKIE_HEADER", "TVSS_COOKIES_JSON"):
                    env_backup[key] = os.environ.pop(key, None)
                try:
                    client = TVSSClient()
                    self.assertIn("session-id=test-sid", client.cookie_header)
                    self.assertIn("at-main=test-token", client.cookie_header)
                    self.assertEqual(client.access_token, "test-token")
                finally:
                    for key, val in env_backup.items():
                        if val is not None:
                            os.environ[key] = val
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
