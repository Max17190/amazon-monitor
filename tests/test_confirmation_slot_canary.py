import asyncio
import unittest

from confirmation_slot_canary import run_abba
from performance_compare import compare


class ConfirmationSlotCanaryTests(unittest.TestCase):
    def test_candidate_starts_now_without_changing_next_poll_slot(self):
        records = asyncio.run(
            run_abba(
                interval_seconds=5.0,
                observations_per_block=60,
                seed=17,
            )
        )

        result = compare(
            records,
            "confirmation_start_delay_ms",
            min_samples=120,
            min_absolute_improvement_ms=500.0,
            min_relative_improvement=0.05,
            max_p99_regression=0.0,
        )

        self.assertTrue(result["accepted"])
        self.assertEqual(result["failed_blocks"], 0)
        self.assertEqual(result["candidate"]["p95_ms"], 0.0)
        self.assertGreater(result["control"]["p95_ms"], 4000.0)


if __name__ == "__main__":
    unittest.main()
