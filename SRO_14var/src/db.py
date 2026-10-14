"""Cassandra connection and session helpers."""
from cassandra.cluster import Cluster, Session, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory

from src.config import (
    CASSANDRA_HOSTS,
    CASSANDRA_PORT,
    CASSANDRA_KEYSPACE,
    CASSANDRA_DATACENTER,
    CASSANDRA_USERNAME,
    CASSANDRA_PASSWORD,
)
from src.schema import KEYSPACE_CQL, ALL_TABLES


def create_cluster() -> Cluster:
    auth = None
    if CASSANDRA_USERNAME and CASSANDRA_PASSWORD:
        auth = PlainTextAuthProvider(
            username=CASSANDRA_USERNAME,
            password=CASSANDRA_PASSWORD,
        )

    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(
            DCAwareRoundRobinPolicy(local_dc=CASSANDRA_DATACENTER)
        ),
        row_factory=dict_factory,
        request_timeout=30,
    )

    return Cluster(
        contact_points=CASSANDRA_HOSTS,
        port=CASSANDRA_PORT,
        auth_provider=auth,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        protocol_version=4,
    )


def init_schema(session: Session, keyspace: str = CASSANDRA_KEYSPACE) -> None:
    """Create keyspace and all tables if they do not exist."""
    session.execute(KEYSPACE_CQL.format(keyspace=keyspace))
    session.set_keyspace(keyspace)
    for cql in ALL_TABLES:
        session.execute(cql)


def get_session(create_schema: bool = True) -> Session:
    """Return a connected session; optionally initialise schema."""
    cluster = create_cluster()
    session = cluster.connect()
    if create_schema:
        init_schema(session)
    else:
        session.set_keyspace(CASSANDRA_KEYSPACE)
    # Keep reference so GC does not close the cluster
    session.cluster_ref = cluster  # type: ignore[attr-defined]
    return session


def close_session(session: Session) -> None:
    cluster = getattr(session, "cluster_ref", None)
    if cluster is not None:
        cluster.shutdown()
