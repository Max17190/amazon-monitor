"""Dependency-free observability primitives for durable alert delivery.

The facade intentionally stores only low-cardinality label sets.  An adapter can
export the snapshots to Prometheus or OpenTelemetry without changing workers.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import Mapping


DEFAULT_HISTOGRAM_SAMPLE_LIMIT = 2048


def _labels(labels: Mapping[str, str] | None = None) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((str(key), str(value)) for key, value in (labels or {}).items()))


def percentile(values, percentile_value):
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    index = round((float(percentile_value) / 100.0) * (len(ordered) - 1))
    return ordered[max(0, min(len(ordered) - 1, index))]


class HealthStatus(str, Enum):
    READY = "ready"
    NOT_READY = "not_ready"
    STOPPING = "stopping"


@dataclass
class DeliveryMetrics:
    """In-memory metric facade suitable for tests and lightweight deployments."""

    histogram_sample_limit: int = DEFAULT_HISTOGRAM_SAMPLE_LIMIT
    counters: dict[tuple[str, tuple[tuple[str, str], ...]], int] = field(
        default_factory=lambda: defaultdict(int)
    )
    histograms: dict[
        tuple[str, tuple[tuple[str, str], ...]], deque[float]
    ] = field(init=False)
    gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = field(default_factory=dict)
    _lock: Lock = field(default_factory=Lock, repr=False)

    def __post_init__(self) -> None:
        if self.histogram_sample_limit <= 0:
            raise ValueError("histogram_sample_limit must be positive")
        self.histograms = defaultdict(
            lambda: deque(maxlen=self.histogram_sample_limit)
        )

    def increment(self, name: str, value: int = 1, labels: Mapping[str, str] | None = None) -> None:
        with self._lock:
            self.counters[(name, _labels(labels))] += value

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        with self._lock:
            self.histograms[(name, _labels(labels))].append(float(value))

    def set_gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        with self._lock:
            self.gauges[(name, _labels(labels))] = float(value)

    def counter(self, name: str, labels: Mapping[str, str] | None = None) -> int:
        return self.counters[(name, _labels(labels))]

    def performance_snapshot(self) -> dict[str, object]:
        """Return a JSON-safe, internally consistent rolling metric snapshot."""
        with self._lock:
            counters = [
                {
                    "name": name,
                    "labels": dict(labels),
                    "value": value,
                }
                for (name, labels), value in sorted(self.counters.items())
            ]
            gauges = [
                {
                    "name": name,
                    "labels": dict(labels),
                    "value": value,
                }
                for (name, labels), value in sorted(self.gauges.items())
            ]
            histograms = []
            for (name, labels), samples in sorted(self.histograms.items()):
                values = tuple(samples)
                if not values:
                    continue
                histograms.append(
                    {
                        "name": name,
                        "labels": dict(labels),
                        "count": len(values),
                        "p50": percentile(values, 50),
                        "p95": percentile(values, 95),
                        "p99": percentile(values, 99),
                        "max": max(values),
                        "sum": sum(values),
                    }
                )
        return {
            "counters": counters,
            "gauges": gauges,
            "histograms": histograms,
        }


@dataclass
class DeliveryHealth:
    """Readiness is explicit so callers can expose it through their HTTP health endpoint."""

    repository_ready: bool = False
    worker_running: bool = False
    stopping: bool = False
    last_error: str | None = None

    @property
    def status(self) -> HealthStatus:
        if self.stopping:
            return HealthStatus.STOPPING
        if self.repository_ready and self.worker_running:
            return HealthStatus.READY
        return HealthStatus.NOT_READY

    def snapshot(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "repository_ready": self.repository_ready,
            "worker_running": self.worker_running,
            "stopping": self.stopping,
            "last_error": self.last_error,
        }
