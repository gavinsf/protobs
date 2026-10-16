"""
Fixtures for the integration tests.

These need real services and are skipped (not failed) when they aren't
configured:

    TEST_DATABASE_URL     a Postgres DSN, e.g. postgresql://user:pass@localhost:5432/db
    TEST_KAFKA_BOOTSTRAP  a Kafka/Redpanda bootstrap address, e.g. localhost:19092

Isolation: nothing is created in the database's default schema. Every test
gets its own throwaway schema (selected through `search_path` on the DSN, so
the unqualified table names in src/sql resolve into it) which is dropped
afterwards. Every Kafka test gets its own topic(s) and consumer group. Both
are safe to point at a shared dev instance.
"""

import os
from collections.abc import Callable, Iterator

import psycopg
import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg.conninfo import make_conninfo
from src.config import Config
from src.sink import PostgresSink

from .helpers import unique


def pytest_collection_modifyitems(items):
    for item in items:
        if "integration" in item.nodeid.split("/"):
            item.add_marker(pytest.mark.integration)


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def base_dsn() -> str:
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not set")
    try:
        with psycopg.connect(dsn, connect_timeout=5):
            pass
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable: {exc}")
    return dsn


@pytest.fixture
def pg_dsn(base_dsn: str) -> Iterator[str]:
    """A DSN whose search_path is a fresh, empty, per-test schema."""
    schema = unique("t")
    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
        try:
            yield make_conninfo(base_dsn, options=f"-csearch_path={schema}")
        finally:
            admin.execute(f"DROP SCHEMA {schema} CASCADE")


@pytest.fixture
def db(pg_dsn: str) -> Iterator[psycopg.Connection]:
    """An independent connection for assertions (never the sink's own)."""
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        yield conn


@pytest.fixture
def sink(pg_dsn: str) -> Iterator[PostgresSink]:
    s = PostgresSink(pg_dsn, slow_after=60.0)
    try:
        yield s
    finally:
        s.close()


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def bootstrap() -> str:
    address = os.environ.get("TEST_KAFKA_BOOTSTRAP")
    if not address:
        pytest.skip("TEST_KAFKA_BOOTSTRAP not set")
    admin = AdminClient({"bootstrap.servers": address})
    try:
        admin.list_topics(timeout=5)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Kafka not reachable: {exc}")
    return address


@pytest.fixture
def make_topic(bootstrap: str) -> Iterator[Callable[..., str]]:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    created: list[str] = []

    def _make(partitions: int = 1) -> str:
        name = unique("capture_it")
        (future,) = admin.create_topics(
            [NewTopic(name, num_partitions=partitions, replication_factor=1)]
        ).values()
        future.result(timeout=15)
        created.append(name)
        return name

    yield _make

    if created:
        for future in admin.delete_topics(created).values():
            try:
                future.result(timeout=15)
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass


@pytest.fixture
def produce(bootstrap: str) -> Callable[..., None]:
    producer = Producer({"bootstrap.servers": bootstrap})

    def _produce(topic: str, value, *, partition: int = -1, headers=None, ts_ms=0):
        kwargs = {"partition": partition} if partition >= 0 else {}
        if ts_ms:
            kwargs["timestamp"] = ts_ms
        producer.produce(topic, value=value, headers=headers, **kwargs)
        producer.flush(15)

    return _produce


@pytest.fixture
def make_config(bootstrap: str, pg_dsn: str) -> Callable[..., Config]:
    def _make(topic: str, **overrides) -> Config:
        params = dict(
            brokers=bootstrap,
            topics=[topic],
            group_id=unique("group"),
            source="it",
            batch_size=100,
            max_batch_seconds=0.2,
            slow_write_seconds=60.0,
            pause_cooldown_seconds=0.5,
        )
        params.update(overrides)
        return Config(**params)

    return _make


def seed_group_at_start(bootstrap: str, group_id: str, topic: str, partitions: int):
    """Commit offset 0 for the group.

    The production consumer uses `auto.offset.reset=latest`; pre-committing 0
    lets a test produce *before* starting the tap and still read everything,
    without changing the consumer configuration under test.
    """
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group_id})
    try:
        c.commit(
            offsets=[TopicPartition(topic, p, 0) for p in range(partitions)],
            asynchronous=False,
        )
    finally:
        c.close()


def committed_offsets(
    bootstrap: str, group_id: str, topic: str, partitions: int = 1
) -> dict[int, int]:
    """The group's committed next-offset per partition (-1001 if none)."""
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group_id})
    try:
        tps = c.committed(
            [TopicPartition(topic, p) for p in range(partitions)], timeout=10
        )
        return {tp.partition: tp.offset for tp in tps}
    finally:
        c.close()
