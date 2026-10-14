"""
CQL schema for the event log service.

Design goals (Variant 14):
- Logs partitioned by service + day (time bucket) → avoids unbounded partitions
- Clustering by event_time DESC + event_id → efficient "latest events" queries
- Extra bucket (hour) option for high-volume services → mitigates hot partitions
- Secondary table for level-based queries (denormalization for Access Patterns)
"""

KEYSPACE_CQL = """
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
"""

# Primary table: events by service and day-bucket
# Partition key: (service_name, day)  — day is YYYY-MM-DD string
# Clustering: event_time DESC, event_id DESC  — newest first
EVENTS_BY_SERVICE_DAY = """
CREATE TABLE IF NOT EXISTS events_by_service_day (
    service_name    text,
    day             text,           -- time bucket: 'YYYY-MM-DD'
    event_time      timestamp,
    event_id        timeuuid,
    level           text,           -- DEBUG, INFO, WARN, ERROR, FATAL
    message         text,
    source          text,           -- module / component
    user_id         text,           -- optional
    request_id      text,           -- optional correlation id
    extra           map<text, text>,
    PRIMARY KEY ((service_name, day), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id DESC)
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_size': '1',
      'compaction_window_unit': 'DAYS'
  }
  AND default_time_to_live = 2592000;   -- 30 days
"""

# Hourly-bucketed table for high-volume services (anti-hot-partition)
# Used when a single service produces very many events per day
EVENTS_BY_SERVICE_HOUR = """
CREATE TABLE IF NOT EXISTS events_by_service_hour (
    service_name    text,
    hour_bucket     text,           -- 'YYYY-MM-DD-HH'
    event_time      timestamp,
    event_id        timeuuid,
    level           text,
    message         text,
    source          text,
    user_id         text,
    request_id      text,
    extra           map<text, text>,
    PRIMARY KEY ((service_name, hour_bucket), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id DESC)
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_size': '1',
      'compaction_window_unit': 'HOURS'
  }
  AND default_time_to_live = 604800;    -- 7 days
"""

# Denormalized table for queries by log level (e.g. all ERROR of a service on a day)
EVENTS_BY_LEVEL = """
CREATE TABLE IF NOT EXISTS events_by_level (
    service_name    text,
    day             text,
    level           text,
    event_time      timestamp,
    event_id        timeuuid,
    message         text,
    source          text,
    user_id         text,
    request_id      text,
    extra           map<text, text>,
    PRIMARY KEY ((service_name, day, level), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id DESC)
  AND default_time_to_live = 2592000;
"""

# Lightweight metadata: list of known services (for demo / discovery)
SERVICES = """
CREATE TABLE IF NOT EXISTS services (
    service_name    text PRIMARY KEY,
    description     text,
    owner           text,
    created_at      timestamp
);
"""

ALL_TABLES = [
    EVENTS_BY_SERVICE_DAY,
    EVENTS_BY_SERVICE_HOUR,
    EVENTS_BY_LEVEL,
    SERVICES,
]
