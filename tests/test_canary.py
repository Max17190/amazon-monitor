import asyncio
import unittest
from unittest.mock import patch

from cadence_canary import (
    advance_deadline,
    calibration_summary,
    run_bucket,
    validate,
)
from credential_governor import CalibrationKey


class DeadlineSchedulingTests(unittest.TestCase):
    def test_preserves_next_future_deadline(self):
        self.assertEqual(advance_deadline(10.0, 5.0, now=12.0), 15.0)

    def test_skips_elapsed_slots_without_catch_up_burst(self):
        self.assertEqual(advance_deadline(10.0, 5.0, now=27.0), 30.0)

    def test_allows_a_deadline_at_the_current_time(self):
        self.assertEqual(advance_deadline(10.0, 5.0, now=15.0), 15.0)

    def test_zero_interval_uses_current_time(self):
        self.assertEqual(advance_deadline(10.0, 0.0, now=12.0), 12.0)


class FirstRateLimitStopsTests(unittest.TestCase):
    def test_discovery_bucket_stops_on_first_429(self):
        calls = []

        async def rate_limited(*_args):
            calls.append(True)
            return "429", 25.0

        with patch("cadence_canary.probe_once", rate_limited):
            result = asyncio.run(
                run_bucket(
                    client=None,
                    session=None,
                    asins=[],
                    interval=0.5,
                    duration=60.0,
                )
            )

        self.assertEqual(result["outcome"], "rate_limited")
        self.assertEqual(result["observations"], 0)
        self.assertEqual(len(calls), 1)

    def test_validation_stops_on_first_429(self):
        outcomes = iter((("ok", 100.0), ("429", 25.0), ("ok", 100.0)))

        async def sequence(*_args):
            return next(outcomes)

        with patch("cadence_canary.probe_once", sequence):
            result = asyncio.run(
                validate(
                    client=None,
                    session=None,
                    asins=[],
                    interval=0.0,
                    observations=60,
                )
            )

        self.assertEqual(result["outcome"], "rate_limited")
        self.assertEqual(result["observations"], 1)


class CalibrationOutputTests(unittest.TestCase):
    def test_machine_readable_summary_requires_complete_clean_direct_validation(self):
        key = CalibrationKey(
            credential_key="tvss-opaque", marketplace_id="market",
            region="us-west", direct_route=True, batch_size=20,
        )
        summary = calibration_summary(key, {
            "interval_seconds": 0.5,
            "outcome": "clean",
            "observations": 120,
        }, validated_at=100)
        self.assertTrue(summary["valid"])
        self.assertNotIn("private", summary["credential_hash"])
        self.assertTrue(summary["direct_route"])

    def test_rate_limited_summary_is_not_valid(self):
        key = CalibrationKey(
            credential_key="tvss-opaque", marketplace_id="market",
            region="us-west", direct_route=True, batch_size=1,
        )
        summary = calibration_summary(key, {
            "interval_seconds": 0.5,
            "outcome": "rate_limited",
            "observations": 119,
        }, validated_at=100)
        self.assertFalse(summary["valid"])
        self.assertEqual(summary["rate_limit_count"], 1)
