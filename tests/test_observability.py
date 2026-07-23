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

    def test_performance_snapshot_includes_percentiles_and_labels(self):
        metrics = DeliveryMetrics()
        for value in (1, 2, 3, 4, 5):
            metrics.observe(
                "latency_seconds",
                value,
                {"request_class": "poll"},
            )
        metrics.increment("requests_total", labels={"outcome": "success"})
        metrics.set_gauge("pool_size", 8)

        snapshot = metrics.performance_snapshot()

        self.assertEqual(snapshot["histograms"][0]["p50"], 3.0)
        self.assertEqual(snapshot["histograms"][0]["p95"], 5.0)
        self.assertEqual(
            snapshot["histograms"][0]["labels"],
            {"request_class": "poll"},
        )
        self.assertEqual(snapshot["counters"][0]["value"], 1)
        self.assertEqual(snapshot["gauges"][0]["value"], 8.0)


if __name__ == "__main__":
    unittest.main()
