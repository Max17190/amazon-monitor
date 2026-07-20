import unittest

from observability import DeliveryMetrics


class DeliveryMetricsTests(unittest.TestCase):
    def test_histograms_retain_only_the_latest_samples(self):
        metrics = DeliveryMetrics(histogram_sample_limit=3)

        for value in range(5):
            metrics.observe("latency_seconds", value)

        self.assertEqual(
            list(metrics.histograms[("latency_seconds", ())]),
            [2.0, 3.0, 4.0],
        )

    def test_histogram_limits_apply_per_label_set(self):
        metrics = DeliveryMetrics(histogram_sample_limit=2)

        for value in range(3):
            metrics.observe("latency_seconds", value, {"target": "one"})
            metrics.observe("latency_seconds", value + 10, {"target": "two"})

        self.assertEqual(
            list(metrics.histograms[("latency_seconds", (("target", "one"),))]),
            [1.0, 2.0],
        )
        self.assertEqual(
            list(metrics.histograms[("latency_seconds", (("target", "two"),))]),
            [11.0, 12.0],
        )

    def test_histogram_limit_must_be_positive(self):
        with self.assertRaisesRegex(ValueError, "must be positive"):
            DeliveryMetrics(histogram_sample_limit=0)


if __name__ == "__main__":
    unittest.main()
