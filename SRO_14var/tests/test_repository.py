"""
At least 3 test scenarios for the event-log repository.

Tests can run against a real Cassandra instance (integration) or be skipped
when the cluster is unavailable.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db import get_session, close_session
from src.models import Event, day_bucket
from src.repository import EventRepository


def cassandra_available() -> bool:
    try:
        session = get_session(create_schema=True)
        session.execute("SELECT now() FROM system.local")
        close_session(session)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not cassandra_available(),
    reason="Cassandra is not reachable — start Docker container first",
)


@pytest.fixture(scope="module")
def repo():
    session = get_session(create_schema=True)
    r = EventRepository(session)
    yield r
    close_session(session)


# ---------- Scenario 1: Create + Read latest ----------
def test_create_and_read_latest(repo: EventRepository):
    now = datetime.now(timezone.utc)
    day = day_bucket(now)
    service = f"test-svc-{uuid4().hex[:8]}"

    events = [
        Event(
            service_name=service,
            level="INFO",
            message=f"test message {i}",
            source="pytest",
            event_time=now - timedelta(seconds=i),
        )
        for i in range(5)
    ]
    repo.create_many(events)

    rows = repo.read_latest(service, day, limit=3)
    assert len(rows) == 3
    # Newest first thanks to CLUSTERING ORDER BY event_time DESC
    times = [r["event_time"] for r in rows]
    assert times == sorted(times, reverse=True)
    assert all(r["service_name"] == service for r in rows)


# ---------- Scenario 2: Filter by level ----------
def test_read_by_level(repo: EventRepository):
    now = datetime.now(timezone.utc)
    day = day_bucket(now)
    service = f"test-level-{uuid4().hex[:8]}"

    repo.create(
        Event(service_name=service, level="ERROR", message="boom", event_time=now)
    )
    repo.create(
        Event(
            service_name=service,
            level="INFO",
            message="ok",
            event_time=now - timedelta(seconds=1),
        )
    )
    repo.create(
        Event(
            service_name=service,
            level="ERROR",
            message="another boom",
            event_time=now - timedelta(seconds=2),
        )
    )

    errors = repo.read_by_level(service, day, "ERROR", limit=10)
    assert len(errors) == 2
    assert all(r["level"] == "ERROR" for r in errors)

    infos = repo.read_by_level(service, day, "INFO", limit=10)
    assert len(infos) == 1
    assert infos[0]["message"] == "ok"


# ---------- Scenario 3: Delete + count ----------
def test_delete_and_count(repo: EventRepository):
    now = datetime.now(timezone.utc)
    day = day_bucket(now)
    service = f"test-del-{uuid4().hex[:8]}"

    e = Event(
        service_name=service,
        level="WARN",
        message="to be deleted",
        event_time=now,
    )
    repo.create(e)

    cnt_before = repo.count_events(service, day)
    assert cnt_before >= 1

    repo.delete(service, day, e.event_time, e.event_id, e.level)

    cnt_after = repo.count_events(service, day)
    assert cnt_after == cnt_before - 1

    remaining = repo.read_latest(service, day, limit=100)
    ids = [r["event_id"] for r in remaining]
    assert e.event_id not in ids


# ---------- Scenario 4 (bonus): cross-day range ----------
def test_time_range_cross_day(repo: EventRepository):
    now = datetime.now(timezone.utc)
    service = f"test-range-{uuid4().hex[:8]}"

    # One event "yesterday", one "today"
    e1 = Event(
        service_name=service,
        level="INFO",
        message="yesterday",
        event_time=now - timedelta(hours=30),
    )
    e2 = Event(
        service_name=service,
        level="INFO",
        message="today",
        event_time=now - timedelta(minutes=5),
    )
    repo.create(e1)
    repo.create(e2)

    start = now - timedelta(hours=36)
    rows = repo.read_time_range(service, start, now)
    messages = {r["message"] for r in rows}
    assert "yesterday" in messages
    assert "today" in messages
