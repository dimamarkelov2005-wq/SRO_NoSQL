"""Configuration loaded from environment variables."""
import os
from dotenv import load_dotenv

load_dotenv()

CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "event_log")
CASSANDRA_DATACENTER = os.getenv("CASSANDRA_DATACENTER", "datacenter1")
CASSANDRA_USERNAME = os.getenv("CASSANDRA_USERNAME") or None
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD") or None
