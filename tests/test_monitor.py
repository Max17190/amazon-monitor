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

from main import AlertState, MonitorConfigError, load_monitor_config
from webhooks import WebhookTarget, load_webhook_targets


class MonitorConfigTests(unittest.TestCase):
    def test_load_monitor_config_from_env_json(self):
        targets = {
            "BLINK_FNF": WebhookTarget(
                name="BLINK_FNF",
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
                            "webhooks": ["BLINK_FNF"],
                        }
                    ]
                }
            ),
            "POLL_INTERVAL_SECONDS": "2",
        }

        config = load_monitor_config(env=env, webhook_targets=targets)

        self.assertEqual(config.backend, "tvss")
        self.assertEqual(config.poll_interval_seconds, 2.0)
        self.assertEqual(config.groups[0].name, "NVIDIA")
        self.assertEqual(config.groups[0].asins, ["B0DT7L98J1", "B0DTJFSSZG"])
        self.assertEqual(config.groups[0].webhook_names, ["BLINK_FNF"])

    def test_rejects_invalid_asin(self):
        targets = {
            "BLINK_FNF": WebhookTarget(
                name="BLINK_FNF",
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
                            "webhooks": ["BLINK_FNF"],
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
            "BLINK_FNF": WebhookTarget(
                name="BLINK_FNF",
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
                            "webhooks": ["BLINK_FNF"],
                        }
                    ]
                }
            ),
            "POLL_INTERVAL_SECONDS": "0",
        }

        with self.assertRaises(MonitorConfigError):
            load_monitor_config(env=env, webhook_targets=targets)


class WebhookTargetTests(unittest.TestCase):
    def test_load_webhook_targets_new_style(self):
        targets = load_webhook_targets(
            {
                "WEBHOOK_BLINK_FNF_URL": "https://discord.example/webhook",
                "WEBHOOK_BLINK_FNF_ROLE_ID": "123",
            }
        )

        self.assertEqual(targets["BLINK_FNF"].url, "https://discord.example/webhook")
        self.assertEqual(targets["BLINK_FNF"].role_id, "123")

    def test_load_webhook_targets_legacy_style(self):
        targets = load_webhook_targets(
            {
                "BLINK_FNF_WEBHOOK_URL": "https://discord.example/legacy",
                "BLINK_FNF_CHANNEL_ID": "456",
            }
        )

        self.assertEqual(targets["BLINK_FNF"].url, "https://discord.example/legacy")
        self.assertEqual(targets["BLINK_FNF"].role_id, "456")


class AlertStateTests(unittest.TestCase):
    def test_alerts_only_on_false_to_true_transition(self):
        state = AlertState()

        self.assertFalse(state.observe("B0DT7L98J1", False))
        self.assertTrue(state.observe("B0DT7L98J1", True))
        self.assertFalse(state.observe("B0DT7L98J1", True))
        self.assertFalse(state.observe("B0DT7L98J1", False))
        self.assertTrue(state.observe("B0DT7L98J1", True))


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

    def test_products_returns_error_for_per_asin_failure(self):
        class FailingClient(TVSSClient):
            def __init__(self):
                self.domain = "amazon.com"

            async def product(self, session, asin):
                raise RuntimeError("boom")

        async def run_test():
            client = FailingClient()
            return await client.products(None, ["B000000001"])

        products = asyncio.run(run_test())

        self.assertEqual(products[0]["asin"], "B000000001")
        self.assertFalse(products[0]["in_stock"])
        self.assertEqual(products[0]["error"], "boom")
        self.assertEqual(products[0]["error_type"], "request")


if __name__ == "__main__":
    unittest.main()
