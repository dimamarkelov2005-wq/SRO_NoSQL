"""
Repository implementing CRUD and the main Access Patterns for the event log.

Hot-partition mitigation:
- Default writes go to events_by_service_day (daily bucket).
- For high-volume services we also write to events_by_service_hour.
- Reads for "latest N events" use LIMIT + clustering order DESC.
- Cross-day queries are performed by the application (one query per day bucket).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Sequence
from uuid import UUID

from cassandra.cluster import Session
from cassandra.query import PreparedStatement, SimpleStatement

from src.models import Event, day_bucket, hour_bucket, LEVELS


class EventRepository:
    def __init__(self, session: Session) -> None:
        self.session = session
        self._prepare()

    def _prepare(self) -> None:
        self.insert_day = self.session.prepare(
            """
            INSERT INTO events_by_service_day
                (service_name, day, event_time, event_id, level, message,
                 source, user_id, request_id, extra)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self.insert_hour = self.session.prepare(
            """
            INSERT INTO events_by_service_hour
                (service_name, hour_bucket, event_time, event_id, level, message,
                 source, user_id, request_id, extra)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self.insert_level = self.session.prepare(
            """
            INSERT INTO events_by_level
                (service_name, day, level, event_time, event_id, message,
                 source, user_id, request_id, extra)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self.insert_service = self.session.prepare(
            """
            INSERT INTO services (service_name, description, owner, created_at)
            VALUES (?, ?, ?, ?)
            """
        )
        self.select_latest = self.session.prepare(
            """
            SELECT service_name, day, event_time, event_id, level, message,
                   source, user_id, request_id, extra
            FROM events_by_service_day
            WHERE service_name = ? AND day = ?
            LIMIT ?
            """
        )
        self.select_by_level = self.session.prepare(
            """
            SELECT service_name, day, level, event_time, event_id, message,
                   source, user_id, request_id, extra
            FROM events_by_level
            WHERE service_name = ? AND day = ? AND level = ?
            LIMIT ?
            """
        )
        self.select_range = self.session.prepare(
            """
            SELECT service_name, day, event_time, event_id, level, message,
                   source, user_id, request_id, extra
            FROM events_by_service_day
            WHERE service_name = ? AND day = ?
              AND event_time >= ? AND event_time <= ?
            """
        )
        self.select_hour_latest = self.session.prepare(
            """
            SELECT service_name, hour_bucket, event_time, event_id, level, message,
                   source, user_id, request_id, extra
            FROM events_by_service_hour
            WHERE service_name = ? AND hour_bucket = ?
            LIMIT ?
            """
        )
        self.delete_event = self.session.prepare(
            """
            DELETE FROM events_by_service_day
            WHERE service_name = ? AND day = ?
              AND event_time = ? AND event_id = ?
            """
        )
        self.delete_event_level = self.session.prepare(
            """
            DELETE FROM events_by_level
            WHERE service_name = ? AND day = ? AND level = ?
              AND event_time = ? AND event_id = ?
            """
        )
        self.count_day = self.session.prepare(
            """
            SELECT COUNT(*) AS cnt
            FROM events_by_service_day
            WHERE service_name = ? AND day = ?
            """
        )

    # ------------------------------------------------------------------ CRUD
    def create(self, event: Event, also_hourly: bool = False) -> Event:
        """Insert event into primary + level tables (and optionally hourly)."""
        row = event.to_row()
        self.session.execute(
            self.insert_day,
            (
                row["service_name"],
                row["day"],
                row["event_time"],
                row["event_id"],
                row["level"],
                row["message"],
                row["source"],
                row["user_id"],
                row["request_id"],
                row["extra"],
            ),
        )
        self.session.execute(
            self.insert_level,
            (
                row["service_name"],
                row["day"],
                row["level"],
                row["event_time"],
                row["event_id"],
                row["message"],
                row["source"],
                row["user_id"],
                row["request_id"],
                row["extra"],
            ),
        )
        if also_hourly:
            self.session.execute(
                self.insert_hour,
                (
                    row["service_name"],
                    row["hour_bucket"],
                    row["event_time"],
                    row["event_id"],
                    row["level"],
                    row["message"],
                    row["source"],
                    row["user_id"],
                    row["request_id"],
                    row["extra"],
                ),
            )
        return event

    def create_many(
        self, events: Sequence[Event], also_hourly: bool = False
    ) -> int:
        for e in events:
            self.create(e, also_hourly=also_hourly)
        return len(events)

    def read_latest(
        self, service_name: str, day: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """AP1: last N events of a service on a given day (newest first)."""
        rows = self.session.execute(
            self.select_latest, (service_name, day, limit)
        )
        return list(rows)

    def read_by_level(
        self, service_name: str, day: str, level: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """AP2: events of a specific level for a service on a day."""
        if level not in LEVELS:
            raise ValueError(f"Unknown level: {level}")
        rows = self.session.execute(
            self.select_by_level, (service_name, day, level, limit)
        )
        return list(rows)

    def read_time_range(
        self,
        service_name: str,
        start: datetime,
        end: datetime,
    ) -> List[Dict[str, Any]]:
        """
        AP3 (non-trivial): events in a time window that may span several day buckets.
        Application iterates over each day partition and merges results.
        """
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        results: List[Dict[str, Any]] = []
        current = start.date()
        end_date = end.date()
        while current <= end_date:
            day_str = current.strftime("%Y-%m-%d")
            rows = self.session.execute(
                self.select_range, (service_name, day_str, start, end)
            )
            results.extend(rows)
            current += timedelta(days=1)

        # Already ordered DESC inside each partition; re-sort globally
        results.sort(key=lambda r: r["event_time"], reverse=True)
        return results

    def read_hour_latest(
        self, service_name: str, hour: str, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """AP4: latest events from the hourly-bucketed table (hot-partition demo)."""
        rows = self.session.execute(
            self.select_hour_latest, (service_name, hour, limit)
        )
        return list(rows)

    def update_message(
        self,
        service_name: str,
        day: str,
        event_time: datetime,
        event_id: UUID,
        new_message: str,
        level: str,
    ) -> None:
        """
        Cassandra does not support partial UPDATE of clustering-key rows
        the same way as RDBMS. We re-INSERT (upsert) with the new message.
        """
        # Fetch existing row first is expensive; for demo we upsert known fields
        self.session.execute(
            self.insert_day,
            (
                service_name,
                day,
                event_time,
                event_id,
                level,
                new_message,
                "updated",
                None,
                None,
                {},
            ),
        )
        self.session.execute(
            self.insert_level,
            (
                service_name,
                day,
                level,
                event_time,
                event_id,
                new_message,
                "updated",
                None,
                None,
                {},
            ),
        )

    def delete(
        self,
        service_name: str,
        day: str,
        event_time: datetime,
        event_id: UUID,
        level: str,
    ) -> None:
        """Delete from both primary and level tables."""
        self.session.execute(
            self.delete_event, (service_name, day, event_time, event_id)
        )
        self.session.execute(
            self.delete_event_level,
            (service_name, day, level, event_time, event_id),
        )

    def count_events(self, service_name: str, day: str) -> int:
        """AP5: approximate count of events in a day partition."""
        row = self.session.execute(
            self.count_day, (service_name, day)
        ).one()
        return int(row["cnt"]) if row else 0

    def register_service(
        self, name: str, description: str, owner: str
    ) -> None:
        self.session.execute(
            self.insert_service,
            (name, description, owner, datetime.now(timezone.utc)),
        )

    def list_services(self) -> List[Dict[str, Any]]:
        rows = self.session.execute("SELECT * FROM services")
        return list(rows)
