"""
Integration tests: src/sink.py against a real Postgres.

Everything the unit tests can only simulate is checked here for real:
the schema, column types, ON CONFLICT idempotency, transaction rollback,
poison-row isolation and reconnecting after the server drops the session.

Needs TEST_DATABASE_URL; each test runs in its own throwaway schema.
"""

import time
import uuid

import psycopg
import pytest
from src.sink import PostgresSink

NOW_MS = 1_700_000_100_500
GUID = uuid.UUID("12345678-1234-5678-1234-567812345678")


def make_row(
    offset: int,
    *,
    source: str = "src",
    topic: str = "orders",
    partition: int = 0,
    headers: str | None = '{"trace": "YWJj"}',
    payload: bytes | None = b"payload",
    schema_id: int | None = 7,
    schema_guid: uuid.UUID | None = None,
    message_index: list[int] | None = (0,),
    broker_ts_ms: int | None = 1_700_000_000_000,
    captured_at_ms: int = NOW_MS,
) -> tuple:
    """A 12-column row in the layout produced by `tap.to_row`."""
    return (
        source,
        topic,
        partition,
        offset,
        broker_ts_ms,
        captured_at_ms,
        headers,
        payload,
        schema_id,
        schema_guid,
        None if message_index is None else list(message_index),
        b"\x11" * 32,
    )


def offsets(db, table="captured_messages", **where):
    clause = " AND ".join(f"{k} = %s" for k in where) or "true"
    rows = db.execute(
        f'SELECT "offset" FROM {table} WHERE {clause} ORDER BY "offset"',
        tuple(where.values()),
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class TestSchema:
    def test_creates_the_tables(self, sink, db):
        tables = {
            r[0]
            for r in db.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = current_schema()"
            )
        }

        assert tables == {
            "captured_messages",
            "capture_dead_letters",
            "capture_progress",
        }

    def test_starting_a_second_sink_is_a_noop(self, sink, pg_dsn, db):
        sink.write([make_row(0)])

        second = PostgresSink(pg_dsn, slow_after=1.0)  # re-runs CREATE ... IF NOT EXISTS
        second.close()

        assert offsets(db) == [0]


# ---------------------------------------------------------------------------
# Writing messages
# ---------------------------------------------------------------------------


class TestWrite:
    def test_round_trips_every_column_with_the_right_type(self, sink, db):
        row = make_row(
            12,
            partition=3,
            headers='{"trace": "YWJj"}',
            payload=b"\x00\x01binary\xff",
            schema_id=None,
            schema_guid=GUID,
            message_index=[1, 0, 2],
        )

        sink.write([row])

        (got,) = db.execute(
            "SELECT source, topic, partition, \"offset\", broker_ts_ms, "
            "captured_at_ms, headers, payload, schema_id, schema_guid, "
            "message_index, payload_sha256 FROM captured_messages"
        ).fetchall()
        assert got == (
            "src",
            "orders",
            3,
            12,
            1_700_000_000_000,
            NOW_MS,
            {"trace": "YWJj"},  # jsonb -> dict
            b"\x00\x01binary\xff",  # bytea -> bytes
            None,
            GUID,  # uuid -> UUID
            [1, 0, 2],  # int[] -> list
            b"\x11" * 32,
        )

    def test_optional_columns_may_be_null(self, sink, db):
        sink.write(
            [
                make_row(
                    0,
                    headers=None,
                    payload=None,
                    schema_id=None,
                    message_index=None,
                    broker_ts_ms=None,
                )
            ]
        )

        (got,) = db.execute(
            "SELECT headers, payload, schema_id, schema_guid, message_index, "
            "broker_ts_ms FROM captured_messages"
        ).fetchall()
        assert got == (None, None, None, None, None, None)

    def test_large_batch_is_stored_in_full(self, sink, db):
        sink.write([make_row(i, payload=b"x" * 64) for i in range(2_000)])

        assert db.execute("SELECT count(*) FROM captured_messages").fetchone() == (2_000,)

    def test_large_payload_is_stored_intact(self, sink, db):
        payload = bytes(range(256)) * 4096  # 1 MiB with every byte value

        sink.write([make_row(0, payload=payload)])

        assert db.execute("SELECT payload FROM captured_messages").fetchone() == (
            payload,
        )

    def test_empty_batch_is_a_noop(self, sink, db):
        sink.write([])

        assert offsets(db) == []
        assert db.execute("SELECT count(*) FROM capture_progress").fetchone() == (0,)

    def test_measures_real_write_time(self, pg_dsn):
        sink = PostgresSink(pg_dsn, slow_after=0.0)
        try:
            sink.write([make_row(0)])

            assert sink.last_write_seconds > 0
            assert sink.is_slow() is True
        finally:
            sink.close()


# ---------------------------------------------------------------------------
# Idempotency / at-least-once delivery
# ---------------------------------------------------------------------------


class TestRedelivery:
    def test_replaying_a_batch_does_not_duplicate_rows(self, sink, db):
        batch = [make_row(i) for i in range(5)]

        sink.write(batch)
        sink.write(batch)

        assert offsets(db) == [0, 1, 2, 3, 4]

    def test_first_capture_wins(self, sink, db):
        sink.write([make_row(0, payload=b"first", captured_at_ms=1)])

        sink.write([make_row(0, payload=b"second", captured_at_ms=2)])

        assert db.execute(
            "SELECT payload, captured_at_ms FROM captured_messages"
        ).fetchone() == (b"first", 1)

    def test_same_offset_in_different_partitions_topics_sources_is_distinct(
        self, sink, db
    ):
        sink.write(
            [
                make_row(0, partition=0),
                make_row(0, partition=1),
                make_row(0, topic="payments"),
                make_row(0, source="other"),
            ]
        )

        assert db.execute("SELECT count(*) FROM captured_messages").fetchone() == (4,)


# ---------------------------------------------------------------------------
# Progress
# ---------------------------------------------------------------------------


class TestProgress:
    def test_records_next_offset_per_partition(self, sink):
        sink.write(
            [
                make_row(0, partition=0),
                make_row(4, partition=0),
                make_row(9, partition=1),
                make_row(2, topic="payments", partition=0),
            ]
        )

        assert sink.load_progress("src") == {
            ("orders", 0): 5,
            ("orders", 1): 10,
            ("payments", 0): 3,
        }

    def test_accumulates_across_writes(self, sink):
        sink.write([make_row(i) for i in range(0, 5)])
        sink.write([make_row(i) for i in range(5, 12)])

        assert sink.load_progress("src") == {("orders", 0): 12}

    def test_never_moves_backwards(self, sink):
        sink.write([make_row(i) for i in range(10)])

        sink.write([make_row(i) for i in range(3)])  # a replayed, older batch

        assert sink.load_progress("src") == {("orders", 0): 10}

    def test_is_scoped_to_the_source(self, sink):
        sink.write([make_row(0, source="a"), make_row(7, source="b")])

        assert sink.load_progress("a") == {("orders", 0): 1}
        assert sink.load_progress("b") == {("orders", 0): 8}
        assert sink.load_progress("nobody") == {}

    def test_a_fresh_sink_resumes_from_stored_progress(self, sink, pg_dsn):
        sink.write([make_row(i) for i in range(6)])

        restarted = PostgresSink(pg_dsn, slow_after=1.0)
        try:
            assert restarted.load_progress("src") == {("orders", 0): 6}
        finally:
            restarted.close()


# ---------------------------------------------------------------------------
# Atomicity
# ---------------------------------------------------------------------------


class TestAtomicity:
    def test_unexpected_error_rolls_back_rows_and_progress(self, sink, db):
        malformed = make_row(1)[:-1]  # 11 columns: not a row error, a bug

        with pytest.raises(psycopg.Error):
            sink.write([make_row(0), malformed])

        assert offsets(db) == []
        assert sink.load_progress("src") == {}
        assert offsets(db, "capture_dead_letters") == []

    def test_sink_is_usable_after_a_failed_write(self, sink, db):
        with pytest.raises(psycopg.Error):
            sink.write([make_row(0)[:-1]])

        sink.write([make_row(0)])

        assert offsets(db) == [0]


# ---------------------------------------------------------------------------
# Poison rows
# ---------------------------------------------------------------------------


class TestPoisonRows:
    def test_check_violation_is_dead_lettered_and_neighbours_survive(self, sink, db):
        sink.write(
            [
                make_row(0),
                make_row(1, partition=-1, payload=b"poison", headers=None),
                make_row(2),
            ]
        )

        assert offsets(db) == [0, 2]
        (dead,) = db.execute(
            'SELECT source, topic, partition, "offset", broker_ts_ms, error, '
            "headers_text, payload FROM capture_dead_letters"
        ).fetchall()
        assert dead[:5] == ("src", "orders", -1, 1, 1_700_000_000_000)
        assert "captured_messages_partition_check" in dead[5]
        assert dead[6:] == (None, b"poison")

    def test_integer_overflow_is_dead_lettered(self, sink, db):
        # A 4-byte unsigned schema id above 2**31 doesn't fit the int column.
        sink.write([make_row(0), make_row(1, schema_id=4_000_000_000)])

        assert offsets(db) == [0]
        assert offsets(db, "capture_dead_letters") == [1]

    def test_dead_letter_keeps_the_headers_text(self, sink, db):
        sink.write([make_row(0, partition=-1, headers='{"k": "dg=="}')])

        assert db.execute(
            "SELECT headers_text FROM capture_dead_letters"
        ).fetchone() == ('{"k": "dg=="}',)

    def test_failed_at_is_when_it_was_dead_lettered(self, sink, db):
        before = int(time.time() * 1000)

        sink.write([make_row(0, partition=-1, captured_at_ms=1)])

        (failed_at,) = db.execute(
            "SELECT failed_at_ms FROM capture_dead_letters"
        ).fetchone()
        assert before <= failed_at <= int(time.time() * 1000)

    def test_progress_advances_past_the_poison_row(self, sink):
        sink.write([make_row(0), make_row(1, schema_id=4_000_000_000), make_row(2)])

        # Otherwise the tap would re-read offset 1 forever.
        assert sink.load_progress("src") == {("orders", 0): 3}

    def test_progress_advances_when_the_last_row_is_the_poison_one(self, sink):
        sink.write([make_row(0), make_row(1, schema_id=4_000_000_000)])

        assert sink.load_progress("src") == {("orders", 0): 2}

    def test_every_bad_row_is_dead_lettered(self, sink, db):
        sink.write(
            [
                make_row(0, schema_id=4_000_000_000),
                make_row(1),
                make_row(2, partition=-1),
                make_row(3, schema_id=-4_000_000_000),
            ]
        )

        assert offsets(db) == [1]
        assert offsets(db, "capture_dead_letters") == [0, 2, 3]

    def test_a_batch_that_is_entirely_poison(self, sink, db):
        sink.write([make_row(i, partition=-1) for i in range(3)])

        assert offsets(db) == []
        assert offsets(db, "capture_dead_letters") == [0, 1, 2]
        assert sink.load_progress("src") == {("orders", -1): 3}

    def test_replaying_a_poison_batch_is_idempotent(self, sink, db):
        batch = [make_row(0), make_row(1, schema_id=4_000_000_000)]

        sink.write(batch)
        sink.write(batch)

        assert offsets(db) == [0]
        assert offsets(db, "capture_dead_letters") == [1]
        assert sink.load_progress("src") == {("orders", 0): 2}

    def test_sink_keeps_working_after_a_poison_batch(self, sink, db):
        sink.write([make_row(0, schema_id=4_000_000_000)])

        sink.write([make_row(1), make_row(2)])

        assert offsets(db) == [1, 2]
        assert sink.load_progress("src") == {("orders", 0): 3}

    def test_clean_batches_do_not_touch_the_dead_letter_table(self, sink, db):
        sink.write([make_row(i) for i in range(3)])

        assert offsets(db, "capture_dead_letters") == []

    def test_dead_letter_is_logged(self, sink, caplog):
        with caplog.at_level("ERROR", logger="src.sink"):
            sink.write([make_row(9, topic="payments", partition=2, schema_id=2**40)])

        assert any("payments[2]@9" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Connection loss
# ---------------------------------------------------------------------------


def _kill_backend(db, pid: int) -> None:
    db.execute("SELECT pg_terminate_backend(%s)", (pid,))
    # The kill is asynchronous; wait until the backend is really gone.
    for _ in range(100):
        (alive,) = db.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE pid = %s", (pid,)
        ).fetchone()
        if not alive:
            return
        time.sleep(0.05)
    raise AssertionError("backend did not terminate")


class TestConnectionLoss:
    def test_write_fails_then_recovers_without_loss_or_duplication(self, sink, db):
        sink.write([make_row(0)])
        _kill_backend(db, sink.conn.info.backend_pid)
        batch = [make_row(1), make_row(2)]

        with pytest.raises(psycopg.OperationalError):
            sink.write(batch)  # the tap would back off and retry this batch
        sink.write(batch)

        assert offsets(db) == [0, 1, 2]
        assert sink.load_progress("src") == {("orders", 0): 3}

    def test_load_progress_after_reconnect(self, sink, db):
        sink.write([make_row(0)])
        _kill_backend(db, sink.conn.info.backend_pid)
        with pytest.raises(psycopg.OperationalError):
            sink.write([make_row(1)])
        sink.write([make_row(1)])  # reconnects

        assert sink.load_progress("src") == {("orders", 0): 2}
