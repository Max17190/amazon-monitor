import unittest

from performance_compare import compare


def records(order=("control", "candidate", "candidate", "control")):
    result = []
    for block, variant in enumerate(order, start=1):
        base = 100.0 if variant == "control" else 70.0
        result.append(
            {
                "block": block,
                "variant": variant,
                "outcome": "clean",
                "error_count": 0,
                "rate_limit_count": 0,
                "mismatch_count": 0,
                "samples": {
                    "latency_ms": [
                        base + (index % 3) for index in range(60)
                    ]
                },
            }
        )
    return result


class PerformanceCompareTests(unittest.TestCase):
    def test_accepts_complete_abba_with_clear_improvement(self):
        result = compare(
            records(),
            "latency_ms",
            min_samples=120,
            min_absolute_improvement_ms=10,
            min_relative_improvement=0.05,
        )

        self.assertTrue(result["accepted"])
        self.assertLess(result["median_delta_95ci_ms"][1], 0)

    def test_rejects_error_block(self):
        values = records()
        values[2]["error_count"] = 1

        result = compare(values, "latency_ms", min_samples=120)

        self.assertFalse(result["accepted"])
        self.assertEqual(result["failed_blocks"], 1)

    def test_rejects_non_abba_order(self):
        with self.assertRaisesRegex(ValueError, "not ABBA"):
            compare(
                records(("control", "candidate", "control", "candidate")),
                "latency_ms",
                min_samples=120,
            )

    def test_zero_control_percentiles_produce_rejected_gate_result(self):
        values = records()
        for record in values:
            record["samples"]["latency_ms"] = [0.0] * 60

        result = compare(
            values,
            "latency_ms",
            min_samples=120,
        )

        self.assertFalse(result["accepted"])
        self.assertIsNone(result["p95_relative_improvement"])
        self.assertEqual(result["p99_regression"], 0.0)

    def test_rejects_mixed_experiments_without_explicit_selection(self):
        first = records()
        second = records()
        for record in first:
            record["experiment_id"] = "first"
        for record in second:
            record["experiment_id"] = "second"

        with self.assertRaisesRegex(ValueError, "multiple experiment_id"):
            compare(
                first + second,
                "latency_ms",
                min_samples=120,
            )

        result = compare(
            first + second,
            "latency_ms",
            min_samples=120,
            experiment_id="first",
        )
        self.assertEqual(result["experiment_id"], "first")


if __name__ == "__main__":
    unittest.main()
