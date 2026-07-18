import asyncio
import unittest
from unittest.mock import patch

from cadence_canary import advance_deadline, run_bucket, validate


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
