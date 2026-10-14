#!/usr/bin/env python3
"""
Demonstrate ≥ 8 queries (including 2 non-trivial) against the event log.

Usage (after seed):
    python -m scripts.demo_queries
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db import get_session, close_session
from src.models import day_bucket, hour_bucket
from src.repository import EventRepository


def banner(title: str) -> None:
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def show(rows, limit: int = 5) -> None:
    if not rows:
        print("  (empty)")
        return
    for i, r in enumerate(rows[:limit]):
        ts = r.get("event_time")
        ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
        print(
            f"  [{i+1}] {ts_str} | {r.get('level'):5} | "
            f"{r.get('service_name')} | {r.get('message')[:55]}"
        )
    if len(rows) > limit:
        print(f"  … and {len(rows) - limit} more")


def main() -> None:
    session = get_session(create_schema=False)
    repo = EventRepository(session)
    now = datetime.now(timezone.utc)
    today = day_bucket(now)
    yesterday = day_bucket(now - timedelta(days=1))

    # ---- Q1: List registered services ----
    banner("Q1. List all registered services")
    services = repo.list_services()
    for s in services:
        print(f"  • {s['service_name']}: {s['description']} (owner={s['owner']})")

    # ---- Q2: Latest 5 events of auth-service today ----
    banner("Q2. Latest 5 events — auth-service @ today (AP1)")
    rows = repo.read_latest("auth-service", today, limit=5)
    show(rows)

    # ---- Q3: ERROR events of payment-service ----
    banner("Q3. ERROR events — payment-service @ today (AP2)")
    rows = repo.read_by_level("payment-service", today, "ERROR", limit=10)
    show(rows)

    # ---- Q4: Count events per service today ----
    banner("Q4. Event counts per service @ today (AP5)")
    for s in services:
        cnt = repo.count_events(s["service_name"], today)
        print(f"  {s['service_name']:25} → {cnt}")

    # ---- Q5 (non-trivial): Time-range across multiple day buckets ----
    banner("Q5. [NON-TRIVIAL] Events of order-service in last 48 h (cross-day)")
    start = now - timedelta(hours=48)
    rows = repo.read_time_range("order-service", start, now)
    print(f"  Found {len(rows)} events across day buckets")
    show(rows, limit=8)

    # ---- Q6: Latest events from hourly table (hot-partition mitigation) ----
    banner("Q6. Latest from hourly bucket — api-gateway (AP4)")
    hour = hour_bucket(now)
    rows = repo.read_hour_latest("api-gateway", hour, limit=5)
    show(rows)
    # also try previous hour
    prev_hour = hour_bucket(now - timedelta(hours=1))
    rows2 = repo.read_hour_latest("api-gateway", prev_hour, limit=5)
    print(f"  (previous hour {prev_hour}: {len(rows2)} events)")
    show(rows2)

    # ---- Q7: WARN events of api-gateway ----
    banner("Q7. WARN events — api-gateway @ today")
    rows = repo.read_by_level("api-gateway", today, "WARN", limit=10)
    show(rows)

    # ---- Q8 (non-trivial): Compare partition sizes (day vs conceptual unbounded) ----
    banner("Q8. [NON-TRIVIAL] Partition size control demonstration")
    print("  Without bucketing a single partition would hold ALL history.")
    print("  With daily buckets each partition is bounded to one day:")
    for delta in range(0, 3):
        d = day_bucket(now - timedelta(days=delta))
        cnt = repo.count_events("api-gateway", d)
        print(f"    partition (api-gateway, {d}) → {cnt} rows")
    print("  → Hot-partition risk is limited; queries stay predictable.")

    # ---- Q9: INFO of notification-service ----
    banner("Q9. INFO events — notification-service @ today")
    rows = repo.read_by_level("notification-service", today, "INFO", limit=5)
    show(rows)

    # ---- Q10: Latest of payment-service yesterday (if any) ----
    banner("Q10. Latest events — payment-service @ yesterday")
    rows = repo.read_latest("payment-service", yesterday, limit=5)
    show(rows)

    close_session(session)
    print("\nDemo finished.")


if __name__ == "__main__":
    main()
