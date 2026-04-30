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

    def test_require_amazon_seller_default_false(self):
        targets = {
            "PRIMARY": WebhookTarget(name="PRIMARY", url="https://x/y"),
        }
        env = {
            "MONITOR_CONFIG_JSON": json.dumps(
                {"groups": [{"name": "G", "asins": ["B0DT7L98J1"], "webhooks": ["PRIMARY"]}]}
            ),
        }
        config = load_monitor_config(env=env, webhook_targets=targets)
        self.assertFalse(config.require_amazon_seller)

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


if __name__ == "__main__":
    unittest.main()
