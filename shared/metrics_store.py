import threading
import time
from collections import deque
from typing import Any


class MetricsStore:
    """
    Thread-safe in-memory store for DQ pipeline metrics.
    Shared across validator, DLQ processor, and API via import.
    """

    def __init__(self, throughput_window_seconds: int = 60):
        self._lock = threading.Lock()

        # Totals
        self._total: int = 0
        self._valid: int = 0
        self._invalid: int = 0
        self._reprocessed: int = 0

        # Per schema version: {version: {valid, invalid}}
        self._by_version: dict[str, dict[str, int]] = {}

        # Seen valid events â€” used for lightweight in-process dedup
        self._seen_valid_event_ids: set[str] = set()

        # Violations log — last 50 entries
        self._violations: deque[dict[str, Any]] = deque(maxlen=50)

        # DLQ state — dlq_id -> DLQMessage dict
        self._dlq_pending: dict[str, dict[str, Any]] = {}
        self._last_reprocess_at: str | None = None

        # Throughput tracking — timestamps of recent messages (within window)
        self._window_seconds = throughput_window_seconds
        self._message_times: deque[float] = deque()

        # Schema registration log
        self._schema_events: list[dict[str, Any]] = []

    # ── Recording ──────────────────────────────────────────────────────────

    def record_valid(self, version: str) -> None:
        with self._lock:
            self._total += 1
            self._valid += 1
            self._ensure_version(version)
            self._by_version[version]["valid"] += 1
            self._message_times.append(time.time())

    def record_invalid(self, version: str, errors: list[str], message: dict | None = None) -> None:
        with self._lock:
            self._total += 1
            self._invalid += 1
            self._ensure_version(version)
            self._by_version[version]["invalid"] += 1
            self._message_times.append(time.time())
            self._violations.append({
                "schema_version": version,
                "errors": errors,
                "order_id": (message or {}).get("order_id", "unknown"),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })

    def record_reprocessed(self, dlq_id: str, version: str) -> None:
        with self._lock:
            self._reprocessed += 1
            self._valid += 1
            self._ensure_version(version)
            self._by_version[version]["valid"] += 1
            self._dlq_pending.pop(dlq_id, None)
            self._last_reprocess_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def add_dlq_message(self, dlq_msg: dict[str, Any]) -> None:
        with self._lock:
            self._dlq_pending[dlq_msg["dlq_id"]] = dlq_msg

    def record_schema_registered(self, version: str) -> None:
        with self._lock:
            self._schema_events.append({
                "version": version,
                "registered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })

    def has_seen_valid_event(self, event_id: str | None) -> bool:
        if not event_id:
            return False
        with self._lock:
            return event_id in self._seen_valid_event_ids

    def mark_valid_event_seen(self, event_id: str | None) -> None:
        if not event_id:
            return
        with self._lock:
            self._seen_valid_event_ids.add(event_id)

    # ── Queries ────────────────────────────────────────────────────────────

    def get_summary(self) -> dict[str, Any]:
        with self._lock:
            return {
                "total": self._total,
                "valid": self._valid,
                "invalid": self._invalid,
                "reprocessed": self._reprocessed,
                "throughput_per_sec": self._throughput(),
                "by_version": dict(self._by_version),
            }

    def get_violations(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._violations)

    def get_dlq_state(self) -> dict[str, Any]:
        with self._lock:
            return {
                "pending_count": len(self._dlq_pending),
                "last_reprocess_at": self._last_reprocess_at,
                "pending_messages": list(self._dlq_pending.values()),
            }

    def get_version_summary(self, version: str) -> dict[str, Any]:
        with self._lock:
            return self._by_version.get(version, {"valid": 0, "invalid": 0})

    def get_throughput_series(self) -> list[dict]:
        """Return per-second counts for last 60 seconds (for line chart)."""
        with self._lock:
            now = time.time()
            buckets: dict[int, int] = {}
            for t in self._message_times:
                age = int(now - t)
                if age < self._window_seconds:
                    buckets[age] = buckets.get(age, 0) + 1
            series = []
            for i in range(self._window_seconds - 1, -1, -1):
                series.append({"seconds_ago": i, "count": buckets.get(i, 0)})
            return series

    # ── Internal ───────────────────────────────────────────────────────────

    def _ensure_version(self, version: str) -> None:
        if version not in self._by_version:
            self._by_version[version] = {"valid": 0, "invalid": 0}

    def _throughput(self) -> float:
        if not self._message_times:
            return 0.0
        now = time.time()
        # Prune old entries
        while self._message_times and now - self._message_times[0] > self._window_seconds:
            self._message_times.popleft()
        if not self._message_times:
            return 0.0
        elapsed = min(self._window_seconds, now - self._message_times[0])
        elapsed = elapsed if elapsed > 0 else 1
        return round(len(self._message_times) / elapsed, 2)


# Singleton — imported by validator, dlq_processor, and api
store = MetricsStore()
