"""Domain models and bucket helpers."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date, timezone
from typing import Any, Dict, Optional
from uuid import UUID

try:
    from uuid6 import uuid7  # preferred when available
except ImportError:
    import uuid as _uuid

    def uuid7() -> UUID:  # type: ignore[misc]
        return _uuid.uuid1()  # fallback for environments without uuid6


LEVELS = ("DEBUG", "INFO", "WARN", "ERROR", "FATAL")


def day_bucket(ts: datetime) -> str:
    """Return YYYY-MM-DD bucket for a timestamp (UTC)."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d")


def hour_bucket(ts: datetime) -> str:
    """Return YYYY-MM-DD-HH bucket for a timestamp (UTC)."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d-%H")


@dataclass
class Event:
    service_name: str
    level: str
    message: str
    source: str = "unknown"
    user_id: Optional[str] = None
    request_id: Optional[str] = None
    extra: Dict[str, str] = field(default_factory=dict)
    event_time: Optional[datetime] = None
    event_id: Optional[UUID] = None

    def __post_init__(self) -> None:
        if self.event_time is None:
            self.event_time = datetime.now(timezone.utc)
        if self.event_id is None:
            self.event_id = uuid7()
        if self.level not in LEVELS:
            raise ValueError(f"level must be one of {LEVELS}, got {self.level!r}")

    @property
    def day(self) -> str:
        assert self.event_time is not None
        return day_bucket(self.event_time)

    @property
    def hour(self) -> str:
        assert self.event_time is not None
        return hour_bucket(self.event_time)

    def to_row(self) -> Dict[str, Any]:
        return {
            "service_name": self.service_name,
            "day": self.day,
            "hour_bucket": self.hour,
            "event_time": self.event_time,
            "event_id": self.event_id,
            "level": self.level,
            "message": self.message,
            "source": self.source,
            "user_id": self.user_id,
            "request_id": self.request_id,
            "extra": self.extra or {},
        }
