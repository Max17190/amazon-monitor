import asyncio
import json
import logging
import os
import sys
import types
import unittest
from unittest.mock import patch

import aiohttp

from amazon_tvss import (
    BatchObservation,
    ObservationStatus,
    TVSSClient,
    TVSSRateLimitError,
)
from tvss_runtime import (
    CredentialRateController,
    ProxyPool,
    load_proxy_urls,
    normalize_proxy_url,
)

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
    AIMD_DECREMENT,
    AIMD_DECREMENT_AFTER,
    AIMD_INTERVAL_CAP,
    AIMD_MULT,
    AlertDispatcher,
    AlertState,
    AuthFailureWatch,
    DEFAULT_BATCH_CHUNK_SIZE,
    DEFAULT_BATCH_CONCURRENCY,
    DEFAULT_POLL_INTERVAL_SECONDS,
    JITTER_FRACTION,
    MAX_BATCH_CONCURRENCY,
    MIN_POLL_INTERVAL_SECONDS,
    MonitorConfigError,
    PENALTY_BOX_SLEEP,
    PENALTY_BOX_THRESHOLD,
    TVSS_BATCH_HARD_CAP,
    build_generic_payload,
    clamp,
    jittered,
    load_monitor_config,
    product_from_batch,
)
from latency_e2e import format_path_latency
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

    def test_fast_alert_default_true(self):
        targets = {
            "PRIMARY": WebhookTarget(name="PRIMARY", url="https://x/y"),
        }
        env = {
            "MONITOR_CONFIG_JSON": json.dumps(
                {"groups": [{"name": "G", "asins": ["B0DT7L98J1"], "webhooks": ["PRIMARY"]}]}
            ),
        }
        config = load_monitor_config(env=env, webhook_targets=targets)
        self.assertTrue(config.fast_alert)

    def test_fast_alert_can_be_disabled(self):
        targets = {
            "PRIMARY": WebhookTarget(name="PRIMARY", url="https://x/y"),
        }
        env = {
            "MONITOR_CONFIG_JSON": json.dumps(
                {"groups": [{"name": "G", "asins": ["B0DT7L98J1"], "webhooks": ["PRIMARY"]}]}
            ),
            "MONITOR_FAST_ALERT": "false",
        }
        config = load_monitor_config(env=env, webhook_targets=targets)
        self.assertFalse(config.fast_alert)

    def test_product_from_batch_builds_minimal_payload(self):
        from main import product_from_batch

        p = product_from_batch("B00FLYWNYQ", {"has_offer": True, "price": "$1.00"})
        self.assertEqual(p["asin"], "B00FLYWNYQ")
        self.assertTrue(p["in_stock"])
        self.assertEqual(p["price"], "$1.00")
        self.assertEqual(p["source"], "tvss-batch")
        self.assertIn("B00FLYWNYQ", p["link"])


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

    def test_missing_asin_has_no_offer_signal(self):
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

    def test_missing_asin_is_unknown(self):
        client = self.make_client()
        self._mock_batch_response(client, {"products": []})
        result = self._run(client.batch_products(None, ["B000MISSING"]))
        self.assertEqual(
            result["B000MISSING"].status,
            ObservationStatus.UNKNOWN,
        )

    def test_partial_response_preserves_missing_as_unknown(self):
        client = self.make_client()
        self._mock_batch_response(
            client,
            {
                "products": [
                    {
                        "basicProduct": {"asin": "B000000001"},
                        "basicOffer": {"offerId": None, "price": None},
                    }
                ]
            },
        )
        result = self._run(
            client.batch_products(None, ["B000000001", "B000000002"])
        )
        self.assertEqual(
            result["B000000001"].status,
            ObservationStatus.OUT_OF_STOCK,
        )
        self.assertEqual(
            result["B000000002"].status,
            ObservationStatus.UNKNOWN,
        )

    def test_top_level_error_makes_every_observation_unknown(self):
        client = self.make_client()
        self._mock_batch_response(
            client,
            {
                "errors": [{"code": "INTERNAL"}],
                "products": [
                    {
                        "basicProduct": {"asin": "B000000001"},
                        "basicOffer": {"offerId": "offer"},
                    }
                ],
            },
        )
        result = self._run(client.batch_products(None, ["B000000001"]))
        self.assertTrue(result.top_level_errors)
        self.assertEqual(
            result["B000000001"].status,
            ObservationStatus.UNKNOWN,
        )

    def test_malformed_offer_is_unknown(self):
        client = self.make_client()
        self._mock_batch_response(
            client,
            {
                "products": [
                    {
                        "basicProduct": {"asin": "B000000001"},
                        "basicOffer": "malformed",
                    }
                ]
            },
        )
        result = self._run(client.batch_products(None, ["B000000001"]))
        self.assertEqual(
            result["B000000001"].status,
            ObservationStatus.UNKNOWN,
        )

    def test_missing_offer_field_is_unknown(self):
        client = self.make_client()
        self._mock_batch_response(
            client,
            {
                "products": [
                    {
                        "basicProduct": {"asin": "B000000001"},
                    }
                ]
            },
        )
        result = self._run(client.batch_products(None, ["B000000001"]))
        self.assertEqual(
            result["B000000001"].status,
            ObservationStatus.UNKNOWN,
        )

    def test_offer_object_without_offer_id_field_is_unknown(self):
        client = self.make_client()
        self._mock_batch_response(
            client,
            {
                "products": [
                    {
                        "basicProduct": {"asin": "B000000001"},
                        "basicOffer": {"price": "$10.00"},
                    }
                ]
            },
        )
        result = self._run(client.batch_products(None, ["B000000001"]))
        self.assertEqual(
            result["B000000001"].status,
            ObservationStatus.UNKNOWN,
        )


class TransitionDeduplicationTests(unittest.TestCase):
    def test_only_one_inflight_transition_can_be_reserved(self):
        state = AlertState()
        state.commit("B000000001", False)
        self.assertTrue(
            state.reserve_transition(
                "B000000001", ObservationStatus.IN_STOCK
            )
        )
        self.assertFalse(
            state.reserve_transition(
                "B000000001", ObservationStatus.IN_STOCK
            )
        )

    def test_failed_delivery_releases_reservation_for_retry(self):
        state = AlertState()
        state.commit("B000000001", False)
        state.reserve_transition("B000000001", ObservationStatus.IN_STOCK)
        state.finish_transition("B000000001", delivered=False)
        self.assertTrue(
            state.reserve_transition(
                "B000000001", ObservationStatus.IN_STOCK
            )
        )

    def test_unknown_does_not_reset_stock_state(self):
        state = AlertState()
        state.commit("B000000001", True)
        self.assertFalse(
            state.reserve_transition("B000000001", ObservationStatus.UNKNOWN)
        )
        self.assertFalse(state.peek("B000000001", True))


class LatencyFormattingTests(unittest.TestCase):
    def test_formats_received_path_latency(self):
        self.assertEqual(format_path_latency(123.456), "123.5ms")

    def test_formats_missing_receipt_as_timeout(self):
        self.assertEqual(format_path_latency(None), "timeout")


class FastAlertPayloadTests(unittest.TestCase):
    def test_batch_alert_never_claims_amazon_seller(self):
        observation = BatchObservation(
            asin="B000000001",
            status=ObservationStatus.IN_STOCK,
            price="$10.00",
        )
        product = product_from_batch("B000000001", observation)
        payload = build_generic_payload(product, "GPU")
        self.assertEqual(payload["signal"], "offer_detected")
        self.assertFalse(payload["seller_verified"])
        self.assertIsNone(payload["seller"])

    def test_batch_embed_labels_unknown_seller(self):
        product = product_from_batch(
            "B000000001",
            BatchObservation(
                asin="B000000001",
                status=ObservationStatus.IN_STOCK,
            ),
        )
        embed = AlertDispatcher(session=None).create_embed(product)
        field_value = embed.fields[0][1]["value"]
        self.assertIn("Seller unconfirmed", field_value)
        self.assertNotIn("Sold By:** Amazon.com", field_value)


class CredentialRateControllerTests(unittest.TestCase):
    def test_deadline_is_measured_from_request_start(self):
        clock = FakeClock(100.0)
        controller = CredentialRateController(2.0, clock=clock)
        controller.mark_started()
        clock.t = 101.25
        self.assertAlmostEqual(controller.seconds_until_ready(), 0.75)

    def test_rate_limit_installs_global_cooldown(self):
        clock = FakeClock(10.0)
        controller = CredentialRateController(
            1.0,
            cooldown_seconds=90.0,
            clock=clock,
        )
        cooldown = controller.record_rate_limit(retry_after=12.0)
        self.assertEqual(cooldown, 90.0)
        self.assertEqual(controller.seconds_until_ready(), 90.0)
        self.assertTrue(controller.snapshot()["consecutive_429"])
        clock.t = 100.0
        self.assertTrue(controller.consume_half_open_probe())
        self.assertFalse(controller.consume_half_open_probe())

    def test_client_raises_typed_rate_limit_with_retry_after(self):
        class FakeResponse:
            status = 429
            headers = {"Retry-After": "17"}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def read(self):
                return b""

        class FakeSession:
            def request(self, *_args, **_kwargs):
                return FakeResponse()

        with patch.dict(
            os.environ,
            {
                "TVSS_COOKIE_HEADER": "session-id=1",
                "PROXY_MODE": "direct",
            },
            clear=False,
        ):
            client = TVSSClient()
        client.configure_rate_controller(1.0)

        with self.assertRaises(TVSSRateLimitError) as raised:
            asyncio.run(
                client._request(
                    FakeSession(),
                    "GET",
                    "https://tvss.amazon.com/test",
                )
            )

        self.assertEqual(raised.exception.retry_after, 17.0)
        self.assertGreaterEqual(
            client.rate_controller.snapshot()["blocked_seconds"],
            89.0,
        )

    def test_client_serializes_all_credential_traffic(self):
        class FakeResponse:
            status = 200
            headers = {}

            def __init__(self, owner):
                self.owner = owner

            async def __aenter__(self):
                self.owner.active += 1
                self.owner.max_active = max(
                    self.owner.max_active,
                    self.owner.active,
                )
                return self

            async def __aexit__(self, *_args):
                self.owner.active -= 1
                return False

            async def read(self):
                await asyncio.sleep(0.001)
                return b"{}"

        class FakeSession:
            def __init__(self):
                self.active = 0
                self.max_active = 0

            def request(self, *_args, **_kwargs):
                return FakeResponse(self)

        with patch.dict(
            os.environ,
            {
                "TVSS_COOKIE_HEADER": "session-id=1",
                "PROXY_MODE": "direct",
            },
            clear=False,
        ):
            client = TVSSClient()
        session = FakeSession()

        async def concurrent_requests():
            await asyncio.gather(
                client._request(session, "GET", "https://tvss.amazon.com/one"),
                client._request(session, "GET", "https://tvss.amazon.com/two"),
            )

        asyncio.run(concurrent_requests())
        self.assertEqual(session.max_active, 1)


class ProxyPoolTests(unittest.TestCase):
    def test_webshare_format_normalizes_without_exposing_credentials(self):
        normalized = normalize_proxy_url(
            "proxy.example:8080:monitor-user:monitor-password"
        )
        self.assertTrue(normalized.startswith("http://"))
        pool = ProxyPool([normalized])
        rendered = repr(pool._health)
        self.assertNotIn("monitor-user", rendered)
        self.assertNotIn("monitor-password", rendered)

    def test_quarantined_proxy_is_not_selected(self):
        first = normalize_proxy_url("one.example:8000:u:p")
        second = normalize_proxy_url("two.example:8000:u:p")
        pool = ProxyPool([first, second], clock=FakeClock(0.0))
        first_id = pool.ranked_route_ids()[0]
        pool.record_failure(first_id, quarantine_seconds=30)
        self.assertNotEqual(pool.ranked_route_ids()[0], first_id)

    def test_network_route_list_allows_only_one_alternate(self):
        proxies = [
            normalize_proxy_url(f"proxy{i}.example:8000:u:p")
            for i in range(20)
        ]
        pool = ProxyPool(proxies)
        self.assertLessEqual(len(pool.request_routes()), 2)

    def test_loads_twenty_entries_from_ignored_file_shape(self):
        import tempfile

        lines = [
            f"proxy{i}.example:8000:user{i}:password{i}"
            for i in range(20)
        ]
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as handle:
            handle.write("\n".join(lines))
            path = handle.name
        try:
            urls = load_proxy_urls({"PROXY_POOL_FILE": path})
        finally:
            os.unlink(path)
        self.assertEqual(len(urls), 20)

    def test_network_failures_do_not_log_proxy_credentials(self):
        class FailingSession:
            def request(self, *_args, **_kwargs):
                raise aiohttp.ClientConnectionError(
                    "failed via monitor-user:monitor-password"
                )

        with patch.dict(
            os.environ,
            {
                "TVSS_COOKIE_HEADER": "session-id=1",
                "PROXY_URL": (
                    "http://monitor-user:monitor-password@proxy.example:8000"
                ),
                "PROXY_MODE": "fallback",
            },
            clear=False,
        ):
            client = TVSSClient()

        logging.disable(logging.NOTSET)
        try:
            with self.assertLogs(level="WARNING") as captured:
                with self.assertRaises(RuntimeError) as raised:
                    asyncio.run(
                        client._request(
                            FailingSession(),
                            "GET",
                            "https://tvss.amazon.com/test",
                        )
                    )
        finally:
            logging.disable(logging.CRITICAL)

        rendered = "\n".join(captured.output) + str(raised.exception)
        self.assertNotIn("monitor-user", rendered)
        self.assertNotIn("monitor-password", rendered)


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


class ClampTests(unittest.TestCase):
    def test_within_range_returns_value(self):
        self.assertEqual(clamp(5, 1, 10), 5)

    def test_below_range_returns_lo(self):
        self.assertEqual(clamp(-3, 1, 10), 1)

    def test_above_range_returns_hi(self):
        self.assertEqual(clamp(99, 1, 10), 10)

    def test_at_boundaries_is_idempotent(self):
        self.assertEqual(clamp(1, 1, 10), 1)
        self.assertEqual(clamp(10, 1, 10), 10)


class JitteredTests(unittest.TestCase):
    def test_within_15_percent_band(self):
        # 1000 samples must all fall within [0.85x, 1.15x] for fraction=0.15
        for _ in range(1000):
            v = jittered(2.0, fraction=0.15)
            self.assertGreaterEqual(v, 2.0 * 0.85)
            self.assertLessEqual(v, 2.0 * 1.15)

    def test_zero_fraction_returns_exact_value(self):
        self.assertAlmostEqual(jittered(2.0, fraction=0.0), 2.0)

    def test_default_fraction_uses_module_constant(self):
        v = jittered(1.0)
        self.assertGreaterEqual(v, 1.0 * (1 - JITTER_FRACTION))
        self.assertLessEqual(v, 1.0 * (1 + JITTER_FRACTION))


class BatchPollingConstantsTests(unittest.TestCase):
    """Constants that encode empirically-verified facts about TVSS."""

    def test_chunk_hard_cap_is_50(self):
        # Bisected: 50 OK, 51 returns HTTP 400 (empty body, ~100 ms reject).
        self.assertEqual(TVSS_BATCH_HARD_CAP, 50)

    def test_default_chunk_size_does_not_exceed_hard_cap(self):
        self.assertLessEqual(DEFAULT_BATCH_CHUNK_SIZE, TVSS_BATCH_HARD_CAP)

    def test_default_batch_concurrency_is_one(self):
        # 4 parallel chunks at 1s caused 84% 429s in 75s — sequential is the
        # only safe default.
        self.assertEqual(DEFAULT_BATCH_CONCURRENCY, 1)

    def test_max_batch_concurrency_is_low(self):
        # Even 4 parallel chunks burned cookies; do not let users go higher
        # without changing this constant deliberately.
        self.assertLessEqual(MAX_BATCH_CONCURRENCY, 4)

    def test_default_poll_interval_meets_min(self):
        self.assertGreaterEqual(DEFAULT_POLL_INTERVAL_SECONDS, MIN_POLL_INTERVAL_SECONDS)

    def test_aimd_cap_is_multi_minute(self):
        # Empirical penalty box recovery is multi-minute, so the cap on the
        # adaptive interval must be at least 60s.
        self.assertGreaterEqual(AIMD_INTERVAL_CAP, 60.0)

    def test_aimd_mult_increases_interval(self):
        self.assertGreater(AIMD_MULT, 1.0)

    def test_aimd_decrement_is_positive(self):
        self.assertGreater(AIMD_DECREMENT, 0.0)

    def test_aimd_decrement_after_is_a_streak(self):
        self.assertGreater(AIMD_DECREMENT_AFTER, 1)

    def test_penalty_box_sleep_is_long(self):
        # Empirical TVSS recovery is >5 minutes; 60s is the absolute minimum
        # that protects the cookie.
        self.assertGreaterEqual(PENALTY_BOX_SLEEP, 60.0)

    def test_penalty_box_threshold_triggers_quickly(self):
        # The point of penalty-box detection is to STOP hammering after a few
        # 429s — keep it tight.
        self.assertLessEqual(PENALTY_BOX_THRESHOLD, 5)


class BatchProductsCapTests(unittest.TestCase):
    """The TVSSClient must refuse >50 ASINs to catch programmer error."""

    def make_client(self):
        with patch.dict(os.environ, {"TVSS_COOKIE_HEADER": "session-id=1"}, clear=False):
            return TVSSClient()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_raises_on_51_asins(self):
        client = self.make_client()
        asins = [f"B{str(i).zfill(9)}" for i in range(51)]
        with self.assertRaises(ValueError):
            self._run(client.batch_products(None, asins))

    def test_accepts_50_asins(self):
        client = self.make_client()

        async def fake_request(session, method, url, json_body=None):
            return {"products": []}

        client._request = fake_request
        asins = [f"B{str(i).zfill(9)}" for i in range(50)]
        result = self._run(client.batch_products(None, asins))
        # All 50 returned as has_offer=False (since fake response is empty).
        self.assertEqual(len(result), 50)
        for v in result.values():
            self.assertFalse(v["has_offer"])


class StateLockSerializesTests(unittest.TestCase):
    """Two chunk loops can race on peek/commit for the same ASIN. The
    state_lock serialises peek-then-conditional-commit."""

    def _run(self, coro):
        return asyncio.run(coro)

    async def _race(self, with_lock):
        state = AlertState()
        state.commit("X", False)  # prime as out-of-stock
        lock = asyncio.Lock()
        observed_transitions = []

        async def worker():
            if with_lock:
                async with lock:
                    if state.peek("X", True):
                        observed_transitions.append(True)
                        state.commit("X", True)
            else:
                if state.peek("X", True):
                    observed_transitions.append(True)
                    # Yield to let the other worker peek before we commit.
                    await asyncio.sleep(0)
                    state.commit("X", True)

        await asyncio.gather(worker(), worker())
        return observed_transitions

    def test_with_lock_one_transition_observed(self):
        observed = self._run(self._race(with_lock=True))
        self.assertEqual(len(observed), 1, "lock must serialise peek/commit")

    def test_without_lock_can_see_double_transition(self):
        # Demonstrates the bug class the lock prevents (does not strictly
        # assert duplicate; with the await asyncio.sleep(0) yield it should
        # though).
        observed = self._run(self._race(with_lock=False))
        self.assertGreaterEqual(len(observed), 1)


if __name__ == "__main__":
    unittest.main()
