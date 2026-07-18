import asyncio
import unittest
from unittest.mock import patch

from cadence_canary import run_bucket, validate


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
