"""
Write Throttle - Prevents excessive InfluxDB writes during abnormal restarts.

In-memory singleton that tracks last-write timestamps per (measurement, house_id).
First write after process start is always allowed; subsequent writes are only
allowed if min_interval_seconds has elapsed.
"""

from datetime import datetime, timezone


class WriteThrottle:
    _instance = None

    @classmethod
    def get(cls) -> "WriteThrottle":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._last_writes: dict[tuple[str, str], datetime] = {}

    def allow(self, measurement: str, house_id: str, min_interval_seconds: int) -> bool:
        """Return True if enough time has elapsed since last write; update timestamp."""
        key = (measurement, house_id)
        now = datetime.now(timezone.utc)
        last = self._last_writes.get(key)
        if last and (now - last).total_seconds() < min_interval_seconds:
            return False
        self._last_writes[key] = now
        return True
