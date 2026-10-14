#!/usr/bin/env python3
"""
Generate ≥ 30 synthetic events and load them into Cassandra.

Usage:
    python -m scripts.seed
"""
from __future__ import annotations

import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Allow running as script from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db import get_session, close_session
from src.models import Event, LEVELS
from src.repository import EventRepository

SERVICES = [
    ("auth-service", "Authentication & JWT", "team-identity"),
    ("payment-service", "Payment processing", "team-billing"),
    ("order-service", "Order management", "team-commerce"),
    ("notification-service", "Email/SMS/Push", "team-comms"),
    ("api-gateway", "Public API gateway", "team-platform"),
]

MESSAGES = {
    "DEBUG": [
        "Cache hit for key {key}",
        "SQL query executed in {ms}ms",
        "Request headers validated",
    ],
    "INFO": [
        "User {user} logged in successfully",
        "Order {order} created",
        "Payment intent {pi} confirmed",
        "Notification queued for delivery",
        "Health check passed",
    ],
    "WARN": [
        "Slow query detected: {ms}ms",
        "Rate limit approaching for IP {ip}",
        "Retry attempt {n} for external call",
        "Deprecated endpoint /v1/legacy used",
    ],
    "ERROR": [
        "Failed to connect to payment provider",
        "Database timeout after 5s",
        "Invalid JWT signature",
        "Null pointer in order calculator",
        "External API returned 503",
    ],
    "FATAL": [
        "Out of memory — process terminating",
        "Unrecoverable disk I/O error",
        "Critical configuration missing",
    ],
}

SOURCES = [
    "auth.controller",
    "payment.gateway",
    "order.service",
    "notif.worker",
    "gateway.middleware",
    "db.pool",
    "cache.redis",
]


def random_message(level: str) -> str:
    template = random.choice(MESSAGES[level])
    return template.format(
        key=f"user:{random.randint(1, 999)}",
        ms=random.randint(50, 3000),
        user=f"u{random.randint(1000, 9999)}",
        order=f"ORD-{random.randint(10000, 99999)}",
        pi=f"pi_{random.randint(100000, 999999)}",
        ip=f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}",
        n=random.randint(1, 5),
    )


def generate_events(count: int = 60) -> list[Event]:
    """Produce synthetic events spread over the last 3 days."""
    now = datetime.now(timezone.utc)
    events: list[Event] = []
    for i in range(count):
        service, _, _ = random.choice(SERVICES)
        # Weighted levels: more INFO, fewer FATAL
        level = random.choices(
            LEVELS, weights=[15, 40, 20, 20, 5], k=1
        )[0]
        # Spread over ~72 hours
        offset = timedelta(
            hours=random.uniform(0, 72),
            seconds=random.uniform(0, 3600),
        )
        ts = now - offset
        events.append(
            Event(
                service_name=service,
                level=level,
                message=random_message(level),
                source=random.choice(SOURCES),
                user_id=f"user-{random.randint(1, 200)}" if random.random() > 0.3 else None,
                request_id=f"req-{random.randint(100000, 999999)}",
                extra={
                    "env": random.choice(["dev", "staging", "prod"]),
                    "version": f"1.{random.randint(0, 5)}.{random.randint(0, 20)}",
                },
                event_time=ts,
            )
        )
    return events


def main() -> None:
    print("Connecting to Cassandra…")
    session = get_session(create_schema=True)
    repo = EventRepository(session)

    print("Registering services…")
    for name, desc, owner in SERVICES:
        repo.register_service(name, desc, owner)

    events = generate_events(60)
    print(f"Inserting {len(events)} events (daily + level tables)…")
    # Also write a subset to the hourly table to demonstrate anti-hot-partition
    high_volume = [e for e in events if e.service_name == "api-gateway"]
    repo.create_many(events, also_hourly=False)
    if high_volume:
        print(f"Also writing {len(high_volume)} api-gateway events to hourly buckets…")
        for e in high_volume:
            repo.create(e, also_hourly=True)

    # Summary
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for name, _, _ in SERVICES:
        cnt = repo.count_events(name, today)
        print(f"  {name} @ {today}: {cnt} events")

    close_session(session)
    print("Seed completed successfully.")


if __name__ == "__main__":
    main()
