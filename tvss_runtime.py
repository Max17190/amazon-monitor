import asyncio
import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import quote, urlsplit


DIRECT_ROUTE_ID = "direct"


@dataclass(frozen=True)
class ProxyRoute:
    route_id: str
    url: Optional[str] = field(repr=False)

    @property
    def is_direct(self):
        return self.url is None


@dataclass
class ProxyRouteHealth:
    route: ProxyRoute
    successes: int = 0
    failures: int = 0
    ewma_latency_ms: Optional[float] = None
    quarantine_until: float = 0.0

    def available(self, now):
        return now >= self.quarantine_until

    def score(self, now):
        if not self.available(now):
            return float("inf")
        latency = self.ewma_latency_ms if self.ewma_latency_ms is not None else 500.0
        return latency + (self.failures * 250.0)


def normalize_proxy_url(raw_value):
    """Normalize proxy input without ever returning a printable credential summary."""
    raw = str(raw_value or "").strip()
    if not raw or raw.startswith("#"):
        return None

    if "://" in raw:
        parsed = urlsplit(raw)
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("proxy URL must include a valid numeric port") from exc
        if not parsed.hostname or not port:
            raise ValueError("proxy URL must include a host and port")
        return raw

    parts = raw.split(":", 3)
    if len(parts) != 4:
        raise ValueError(
            "proxy must be a URL or use host:port:username:password format"
        )

    host, port, username, password = (part.strip() for part in parts)
    if not host or not port or not username or not password:
        raise ValueError("proxy host, port, username, and password are required")
    if not port.isdigit():
        raise ValueError("proxy port must be numeric")

    return "http://{}:{}@{}:{}".format(
        quote(username, safe=""),
        quote(password, safe=""),
        host,
        port,
    )


def proxy_route_id(proxy_url):
    digest = hashlib.sha256(proxy_url.encode("utf-8")).hexdigest()[:10]
    return "proxy-{}".format(digest)


def _iter_proxy_file(path):
    proxy_path = Path(path).expanduser()
    if not proxy_path.exists():
        raise ValueError("proxy pool file does not exist")
    for raw_line in proxy_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#"):
            yield line


def load_proxy_urls(env=None):
    env = os.environ if env is None else env
    candidates = []

    single = env.get("PROXY_URL")
    if single:
        candidates.append(single)

    raw_json = str(env.get("PROXY_URLS_JSON", "")).strip()
    if raw_json:
        try:
            values = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ValueError("PROXY_URLS_JSON must be valid JSON") from exc
        if not isinstance(values, list):
            raise ValueError("PROXY_URLS_JSON must contain a JSON array")
        candidates.extend(values)

    proxy_file = str(env.get("PROXY_POOL_FILE", "")).strip()
    if proxy_file:
        candidates.extend(_iter_proxy_file(proxy_file))

    if not candidates:
        inherited = env.get("HTTPS_PROXY") or env.get("HTTP_PROXY")
        if inherited:
            candidates.append(inherited)

    normalized = []
    seen = set()
    for candidate in candidates:
        proxy_url = normalize_proxy_url(candidate)
        if proxy_url and proxy_url not in seen:
            normalized.append(proxy_url)
            seen.add(proxy_url)
    return normalized


class ProxyPool:
    """Direct-first proxy routing with bounded fallback and health ranking."""

    def __init__(
        self,
        proxy_urls=None,
        mode="fallback",
        clock=None,
        recovery_successes=10,
        allow_network_fallback=True,
    ):
        self._clock = clock or time.monotonic
        self.mode = str(mode or "fallback").strip().lower()
        self._health: Dict[str, ProxyRouteHealth] = {}
        for proxy_url in proxy_urls or []:
            route = ProxyRoute(proxy_route_id(proxy_url), proxy_url)
            self._health[route.route_id] = ProxyRouteHealth(route=route)
        self._recovery_route_id = None
        self._recovery_remaining = 0
        self._recovery_successes = max(1, int(recovery_successes))
        self.allow_network_fallback = bool(allow_network_fallback)

    @classmethod
    def from_env(cls, env=None, clock=None):
        env = os.environ if env is None else env
        return cls(
            load_proxy_urls(env),
            mode=env.get("PROXY_MODE", "fallback"),
            clock=clock,
            recovery_successes=int(env.get("PROXY_RECOVERY_SUCCESSES", "10")),
        )

    @property
    def has_proxies(self):
        return bool(self._health)

    @property
    def proxy_count(self):
        return len(self._health)

    @property
    def recovery_active(self):
        return self._recovery_route_id is not None

    def proxy_routes(self):
        return tuple(health.route for health in self._health.values())

    def _best_proxy(self, exclude=()):
        now = self._clock()
        excluded = set(exclude)
        candidates = [
            health
            for route_id, health in self._health.items()
            if route_id not in excluded and health.available(now)
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda health: health.score(now)).route

    def ranked_route_ids(self):
        now = self._clock()
        return [
            health.route.route_id
            for health in sorted(
                self._health.values(),
                key=lambda health: health.score(now),
            )
            if health.available(now)
        ]

    def activate_recovery(self):
        route = self._best_proxy()
        if route is None:
            self._recovery_route_id = None
            self._recovery_remaining = 0
            return None
        self._recovery_route_id = route.route_id
        self._recovery_remaining = self._recovery_successes
        return route.route_id

    def deactivate_recovery(self):
        self._recovery_route_id = None
        self._recovery_remaining = 0

    def _primary_route(self):
        if self.mode in ("off", "none", "direct", "disabled"):
            return ProxyRoute(DIRECT_ROUTE_ID, None)

        if self.mode in ("always", "on", "force"):
            return self._best_proxy() or ProxyRoute(DIRECT_ROUTE_ID, None)

        if self._recovery_route_id:
            health = self._health.get(self._recovery_route_id)
            if health and health.available(self._clock()):
                return health.route
            self._recovery_route_id = None
            self._recovery_remaining = 0

        return ProxyRoute(DIRECT_ROUTE_ID, None)

    @property
    def primary_route(self):
        return self._primary_route()

    def request_routes(self):
        """Return a primary route and at most one network-failure fallback."""
        primary = self._primary_route()
        routes = [primary]

        if (
            self.mode in ("off", "none", "direct", "disabled")
            or not self.allow_network_fallback
        ):
            return routes

        if primary.is_direct:
            alternate = self._best_proxy()
        else:
            alternate = ProxyRoute(DIRECT_ROUTE_ID, None)

        if alternate and alternate.route_id != primary.route_id:
            routes.append(alternate)
        return routes

    def record_success(self, route_id, latency_ms):
        if route_id == DIRECT_ROUTE_ID:
            return
        health = self._health.get(route_id)
        if not health:
            return
        health.successes += 1
        latency_ms = float(latency_ms)
        if health.ewma_latency_ms is None:
            health.ewma_latency_ms = latency_ms
        else:
            health.ewma_latency_ms = (health.ewma_latency_ms * 0.8) + (
                latency_ms * 0.2
            )
        if route_id == self._recovery_route_id:
            self._recovery_remaining -= 1
            if self._recovery_remaining <= 0:
                self._recovery_route_id = None
                self._recovery_remaining = 0

    def record_failure(self, route_id, quarantine_seconds=30.0):
        if route_id == DIRECT_ROUTE_ID:
            return
        health = self._health.get(route_id)
        if not health:
            return
        health.failures += 1
        health.quarantine_until = max(
            health.quarantine_until,
            self._clock() + max(0.0, float(quarantine_seconds)),
        )
        if route_id == self._recovery_route_id:
            self._recovery_route_id = None
            self._recovery_remaining = 0


class CredentialRateController:
    """One request budget and one 429 circuit breaker per Amazon credential."""

    def __init__(
        self,
        base_interval,
        max_interval=120.0,
        cooldown_seconds=90.0,
        success_window=30,
        additive_decrease=0.05,
        clock=None,
    ):
        self.base_interval = float(base_interval)
        self.interval = float(base_interval)
        self.max_interval = float(max_interval)
        self.cooldown_seconds = float(cooldown_seconds)
        self.success_window = max(1, int(success_window))
        self.additive_decrease = float(additive_decrease)
        self._clock = clock or time.monotonic
        self.next_deadline = self._clock()
        self.blocked_until = 0.0
        self.success_streak = 0
        self.consecutive_rate_limits = 0
        self._half_open_pending = False
        self._start_lock = None

    def seconds_until_ready(self, now=None):
        now = self._clock() if now is None else float(now)
        ready_at = max(self.next_deadline, self.blocked_until)
        return max(0.0, ready_at - now)

    def mark_started(self, now=None):
        now = self._clock() if now is None else float(now)
        self.next_deadline = now + self.interval
        return now

    async def acquire(self):
        """Reserve one credential-wide request start at the next deadline."""
        if self._start_lock is None:
            self._start_lock = asyncio.Lock()
        async with self._start_lock:
            delay = self.seconds_until_ready()
            if delay:
                await asyncio.sleep(delay)
            half_open = self.consume_half_open_probe()
            started_at = self.mark_started()
            return started_at, delay, half_open

    def record_success(self, now=None):
        now = self._clock() if now is None else float(now)
        self.consecutive_rate_limits = 0
        if self.interval > self.base_interval:
            self.success_streak += 1
            if self.success_streak >= self.success_window:
                self.interval = max(
                    self.base_interval,
                    self.interval - self.additive_decrease,
                )
                self.success_streak = 0
                self.next_deadline = min(self.next_deadline, now + self.interval)
        else:
            self.success_streak = 0

    def record_rate_limit(self, retry_after=None, now=None):
        now = self._clock() if now is None else float(now)
        self.consecutive_rate_limits += 1
        self.success_streak = 0
        self.interval = min(self.interval * 2.0, self.max_interval)
        retry_after = max(0.0, float(retry_after or 0.0))
        multiplier = 2 ** min(self.consecutive_rate_limits - 1, 3)
        cooldown = max(retry_after, self.cooldown_seconds * multiplier)
        self.blocked_until = max(self.blocked_until, now + cooldown)
        self.next_deadline = max(self.next_deadline, self.blocked_until)
        self._half_open_pending = True
        return cooldown

    def consume_half_open_probe(self, now=None):
        now = self._clock() if now is None else float(now)
        if not self._half_open_pending or now < self.blocked_until:
            return False
        self._half_open_pending = False
        return True

    def snapshot(self, now=None):
        now = self._clock() if now is None else float(now)
        return {
            "interval_seconds": self.interval,
            "blocked_seconds": max(0.0, self.blocked_until - now),
            "success_streak": self.success_streak,
            "consecutive_429": self.consecutive_rate_limits,
        }
