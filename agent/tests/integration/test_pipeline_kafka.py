"""
End-to-end integration tests: Kafka -> Tap -> wire parsing -> PostgresSink.

A real broker and a real Postgres; nothing in the capture path is faked. The
tap runs on a background thread exactly as `run()` drives it in production,
using the production consumer configuration from `build_consumer`
(manual commits, read_committed).

Needs TEST_KAFKA_BOOTSTRAP and TEST_DATABASE_URL. Each test gets its own
topic, consumer group and database schema.
"""

import hashlib
import uuid

import psycopg
import pytest
from src.sink import PostgresSink
from src.tap import Tap

from .conftest import committed_offsets, seed_group_at_start
from .helpers import RunningTap, classic_frame, guid_header, wait_for

GUID = uuid.UUID("abcdef01-2345-6789-abcd-ef0123456789")
TS_MS = 1_700_000_000_000


class SpySink:
    """Delegates to a real PostgresSink; records batches, can fail on demand."""

    def __init__(self, inner: PostgresSink, fail_writes: int = 0) -> None:
        self.inner = inner
        self.fail_writes = fail_writes
        self.failures = 0
        self.batches: list[list[tuple]] = []

    def write(self, rows):
        if self.fail_writes > 0:
            self.fail_writes -= 1
            self.failures += 1
            raise psycopg.OperationalError("injected sink outage")
        self.batches.append(list(rows))
        self.inner.write(rows)

    def is_slow(self) -> bool:
        return self.inner.is_slow()

    def load_progress(self, source):
        return self.inner.load_progress(source)

    @property
    def written_offsets(self) -> list[tuple[str, int, int]]:
        return [(r[1], r[2], r[3]) for batch in self.batches for r in batch]


@pytest.fixture
def start(bootstrap, make_config):
    """Build a RunningTap whose group starts at offset 0 of `topic`."""

    def _start(sink, topic, *, group_id=None, partitions=1, **cfg_overrides):
        overrides = dict(cfg_overrides)
        if group_id:
            overrides["group_id"] = group_id
        cfg = make_config(topic, **overrides)
        seed_group_at_start(bootstrap, cfg.group_id, topic, partitions)
        return RunningTap(Tap(sink, cfg)), cfg

    return _start


def count(db, table="captured_messages") -> int:
    return db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def read_all(db, topic):
    cur = db.execute(
        'SELECT partition, "offset", payload FROM captured_messages '
        'WHERE topic = %s ORDER BY partition, "offset"',
        (topic,),
    )
    return [(p, o, bytes(v)) for p, o, v in cur.fetchall()]


# ---------------------------------------------------------------------------
# Capturing every wire format, byte for byte
# ---------------------------------------------------------------------------


class TestCapture:
    def test_classic_frame_is_captured_with_all_columns(
        self, sink, db, make_topic, produce, start
    ):
        topic = make_topic()
        value = classic_frame(77, b"order-created", indexes=(0,))
        produce(
            topic, value, ts_ms=TS_MS, headers=[("schema-version", b"v1"), ("n", None)]
        )
        runner, _ = start(sink, topic)

        with runner:
            wait_for(lambda: count(db) == 1)

        (row,) = db.execute(
            "SELECT source, topic, partition, \"offset\", broker_ts_ms, headers, "
            "payload, schema_id, schema_guid, message_index, payload_sha256 "
            "FROM captured_messages"
        ).fetchall()
        assert row == (
            "it",
            topic,
            0,
            0,
            TS_MS,
            {"schema-version": "djE=", "n": ""},
            value,
            77,
            None,
            [0],
            hashlib.sha256(value).digest(),
        )

    def test_guid_frame_unframed_and_tombstone_in_one_stream(
        self, sink, db, make_topic, produce, start
    ):
        topic = make_topic()
        produce(topic, b"guid-payload", headers=[guid_header(GUID, indexes=(2, 1))])
        produce(topic, b'{"just": "json"}')
        produce(topic, None)  # tombstone
        runner, _ = start(sink, topic)

        with runner:
            wait_for(lambda: count(db) == 3)

        rows = db.execute(
            "SELECT \"offset\", schema_id, schema_guid, message_index, payload "
            "FROM captured_messages ORDER BY \"offset\""
        ).fetchall()
        assert [(r[0], r[1], r[2], r[3], bytes(r[4])) for r in rows] == [
            (0, None, GUID, [2, 1], b"guid-payload"),
            (1, None, None, None, b'{"just": "json"}'),
            (2, None, None, None, b""),
        ]

    def test_partitions_are_captured_independently(
        self, sink, db, make_topic, produce, start
    ):
        topic = make_topic(partitions=3)
        expected = []
        for p in range(3):
            for i in range(4):
                value = classic_frame(p + 1, f"p{p}-m{i}".encode())
                produce(topic, value, partition=p)
                expected.append((p, i, value))
        runner, cfg = start(sink, topic, partitions=3)

        with runner:
            wait_for(lambda: count(db) == 12)

        assert read_all(db, topic) == sorted(expected)
        assert sink.load_progress("it") == {(topic, 0): 4, (topic, 1): 4, (topic, 2): 4}

    def test_ordering_within_a_partition_is_preserved(
        self, sink, db, make_topic, produce, start
    ):
        topic = make_topic()
        for i in range(50):
            produce(topic, classic_frame(1, f"{i:03d}".encode()))
        runner, _ = start(sink, topic, batch_size=7)  # several uneven batches

        with runner:
            wait_for(lambda: count(db) == 50)

        payloads = [v[-3:] for _, _, v in read_all(db, topic)]
        assert payloads == [f"{i:03d}".encode() for i in range(50)]


# ---------------------------------------------------------------------------
# Commit / progress semantics
# ---------------------------------------------------------------------------


class TestCommitsFollowDurability:
    def test_committed_offsets_match_sink_progress(
        self, bootstrap, sink, db, make_topic, produce, start
    ):
        topic = make_topic()
        for i in range(10):
            produce(topic, classic_frame(1, bytes([i])))
        runner, cfg = start(sink, topic, batch_size=4)

        with runner:
            wait_for(lambda: count(db) == 10)

        assert sink.load_progress("it") == {(topic, 0): 10}
        wait_for(
            lambda: committed_offsets(bootstrap, cfg.group_id, topic) == {0: 10},
            message="offsets were not committed on shutdown",
        )

    def test_stopping_flushes_a_partial_batch(
        self, bootstrap, sink, db, make_topic, produce, start
    ):
        topic = make_topic()
        for i in range(3):
            produce(topic, classic_frame(1, bytes([i])))
        # Neither the size nor the age threshold will trigger a flush.
        runner, cfg = start(sink, topic, batch_size=1_000, max_batch_seconds=3_600)

        with runner:
            wait_for(lambda: runner.tap.buf and len(runner.tap.buf) == 3)
            assert count(db) == 0  # still only buffered

        assert count(db) == 3  # the final flush on shutdown
        wait_for(lambda: committed_offsets(bootstrap, cfg.group_id, topic) == {0: 3})

    def test_a_restarted_tap_resumes_without_rereading(
        self, bootstrap, sink, db, make_topic, produce, start
    ):
        topic = make_topic()
        for i in range(5):
            produce(topic, classic_frame(1, bytes([i])))
        first_spy = SpySink(sink)
        runner, cfg = start(first_spy, topic)
        with runner:
            wait_for(lambda: count(db) == 5)
        wait_for(lambda: committed_offsets(bootstrap, cfg.group_id, topic) == {0: 5})

        for i in range(5, 8):
            produce(topic, classic_frame(1, bytes([i])))
        second_spy = SpySink(sink)
        # Same group, no re-seeding: it must pick up from the commit.
        second = RunningTap(Tap(second_spy, cfg))
        with second:
            wait_for(lambda: count(db) == 8)

        assert first_spy.written_offsets == [(topic, 0, i) for i in range(5)]
        assert second_spy.written_offsets == [(topic, 0, i) for i in (5, 6, 7)]


class TestAtLeastOnce:
    def test_nothing_is_committed_while_the_sink_is_down(
        self, bootstrap, sink, db, make_topic, produce, start
    ):
        topic = make_topic()
        for i in range(4):
            produce(topic, classic_frame(1, bytes([i])))
        dead_sink = SpySink(sink, fail_writes=10**6)
        runner, cfg = start(dead_sink, topic, batch_size=1)

        with runner:
            wait_for(lambda: dead_sink.failures >= 1)
        # Shutdown's final flush also failed; the consumer was still closed.

        assert count(db) == 0
        assert committed_offsets(bootstrap, cfg.group_id, topic) == {0: 0}

        # A replacement process with a healthy sink loses nothing.
        healthy = RunningTap(Tap(SpySink(sink), cfg))
        with healthy:
            wait_for(lambda: count(db) == 4)
        assert [o for _, o, _ in read_all(db, topic)] == [0, 1, 2, 3]

    def test_recovers_in_place_when_the_sink_comes_back(
        self, bootstrap, sink, db, make_topic, produce, start, monkeypatch
    ):
        monkeypatch.setattr(Tap, "RETRY_INITIAL_SECONDS", 0.2)
        topic = make_topic()
        for i in range(6):
            produce(topic, classic_frame(1, bytes([i])))
        flaky = SpySink(sink, fail_writes=2)
        runner, cfg = start(flaky, topic, batch_size=3)

        with runner:
            wait_for(lambda: count(db) == 6)

        assert flaky.failures == 2
        assert [o for _, o, _ in read_all(db, topic)] == list(range(6))
        wait_for(lambda: committed_offsets(bootstrap, cfg.group_id, topic) == {0: 6})

    def test_survives_the_database_dropping_the_session(
        self, sink, db, make_topic, produce, start, monkeypatch
    ):
        monkeypatch.setattr(Tap, "RETRY_INITIAL_SECONDS", 0.2)
        topic = make_topic()
        produce(topic, classic_frame(1, b"before"))
        runner, _ = start(sink, topic, batch_size=1)

        with runner:
            wait_for(lambda: count(db) == 1)
            pid = sink.conn.info.backend_pid
            db.execute("SELECT pg_terminate_backend(%s)", (pid,))
            for i in range(3):
                produce(topic, classic_frame(1, f"after-{i}".encode()))
            wait_for(lambda: count(db) == 4)

        assert [o for _, o, _ in read_all(db, topic)] == [0, 1, 2, 3]


# ---------------------------------------------------------------------------
# Poison messages
# ---------------------------------------------------------------------------


class TestPoisonMessages:
    def test_unstorable_message_is_dead_lettered_and_the_stream_continues(
        self, bootstrap, sink, db, make_topic, produce, start
    ):
        topic = make_topic()
        produce(topic, classic_frame(1, b"before"))
        produce(topic, classic_frame(2**31 + 5, b"poison"))  # id overflows int4
        produce(topic, classic_frame(3, b"after"))
        runner, cfg = start(sink, topic, batch_size=3)

        with runner:
            wait_for(lambda: sink.load_progress("it") == {(topic, 0): 3})

        assert [o for _, o, _ in read_all(db, topic)] == [0, 2]
        ((offset, payload),) = db.execute(
            'SELECT "offset", payload FROM capture_dead_letters'
        ).fetchall()
        assert (offset, bytes(payload)) == (1, classic_frame(2**31 + 5, b"poison"))
        wait_for(lambda: committed_offsets(bootstrap, cfg.group_id, topic) == {0: 3})

    def test_malformed_frame_is_captured_not_dropped(
        self, sink, db, make_topic, produce, start
    ):
        topic = make_topic()
        broken = b"\x00" + (5).to_bytes(4, "big") + b"\x80"  # truncated varint
        produce(topic, broken)
        runner, _ = start(sink, topic)

        with runner:
            wait_for(lambda: count(db) == 1)

        (row,) = db.execute(
            "SELECT payload, schema_id, message_index FROM captured_messages"
        ).fetchall()
        assert (bytes(row[0]), row[1], row[2]) == (broken, None, None)
        assert count(db, "capture_dead_letters") == 0
