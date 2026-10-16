"""
Unit tests for src/sink.py.

No database is touched. `psycopg.connect` is replaced by a `FakeConnection`
that records every statement together with the transaction depth it ran at,
so the tests can assert *what* was written, *in what order*, and *inside which
transaction* -- the properties that make the sink safe to retry.

* `FakeCursor` raises `InterfaceError` if it is used after its `with` block,
  like the real one (this is the failure mode of the original `_write_dead`).
* `FakeConnection.reject_rows` makes the INSERT fail for chosen rows, both as
  a batch (`executemany`) and one at a time (`execute`), the way Postgres
  rejects a constraint-violating row.
* `time` inside `src.sink` is swapped for `FakeClock`, so durations and
  dead-letter timestamps are exact and nothing sleeps.

Behaviour against a real Postgres is covered in tests/integration.
"""

import logging
from collections import namedtuple
from contextlib import contextmanager
from unittest.mock import patch

import psycopg
import pytest
from src import sink as sink_mod
from src.sink import (
    ADD_DEAD_LETTER,
    ADD_PROGRESS,
    INSERT,
    READ_PROGRESS,
    SCHEMA,
    PostgresSink,
)

DSN = "postgresql://capture@db.test/protobs"
NOW_S = 1_700_000_100.5
NOW_MS = 1_700_000_100_500

Call = namedtuple("Call", "kind sql params depth")


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class FakeClock:
    """Stands in for the `time` module as seen by src.sink."""

    def __init__(self) -> None:
        self.now = 1_000.0  # monotonic seconds
        self.wall = NOW_S  # epoch seconds

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.wall

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeCursor:
    def __init__(self, conn: "FakeConnection") -> None:
        self.conn = conn
        self.closed = False
        self._result: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.closed = True
        return False

    def _check_open(self) -> None:
        if self.closed:
            raise psycopg.InterfaceError("the cursor is closed")

    def execute(self, sql, params=None):
        self._check_open()
        self.conn.record("execute", sql, params)
        if sql == READ_PROGRESS:
            self._result = list(self.conn.progress_table)
        return self

    def executemany(self, sql, params_seq):
        self._check_open()
        self.conn.record("executemany", sql, list(params_seq))

    def fetchall(self):
        self._check_open()
        return self._result


class FakeConnection:
    def __init__(self, dsn: str, autocommit: bool) -> None:
        self.dsn = dsn
        self.autocommit = autocommit
        self.closed = False
        self.depth = 0
        self.calls: list[Call] = []
        self.progress_table: list[tuple] = []  # what READ_PROGRESS returns
        self.on_statement = None  # callable(Call), may raise
        self._rejects: list[tuple[list[tuple], Exception]] = []

    # -- configuration ------------------------------------------------------
    def reject_rows(self, rows, exc: Exception) -> None:
        self._rejects.append((list(rows), exc))

    # -- recording ----------------------------------------------------------
    def record(self, kind, sql=None, params=None) -> None:
        call = Call(kind, sql, params, self.depth)
        self.calls.append(call)
        if kind in ("execute", "executemany") and sql == INSERT:
            batch = params if kind == "executemany" else [params]
            for bad, exc in self._rejects:
                if any(row in bad for row in batch):
                    raise exc
        if self.on_statement and kind in ("execute", "executemany"):
            self.on_statement(call)

    def statements(self, sql: str) -> list[Call]:
        return [c for c in self.calls if c.sql == sql]

    # -- psycopg.Connection surface used by the sink ------------------------
    def execute(self, sql, params=None):
        self.record("execute", sql, params)

    def cursor(self):
        return FakeCursor(self)

    @contextmanager
    def transaction(self):
        self.depth += 1
        self.record("begin")
        try:
            yield
        except BaseException:
            self.record("rollback")
            raise
        else:
            self.record("commit")
        finally:
            self.depth -= 1

    def close(self) -> None:
        self.closed = True


class ConnectionFactory:
    """Replacement for `psycopg.connect`; remembers every connection made."""

    def __init__(self) -> None:
        self.connections: list[FakeConnection] = []

    def __call__(self, dsn, autocommit=False, **_):
        conn = FakeConnection(dsn, autocommit)
        self.connections.append(conn)
        return conn

    @property
    def last(self) -> FakeConnection:
        return self.connections[-1]


# ---------------------------------------------------------------------------
# Fixtures / builders
# ---------------------------------------------------------------------------


@pytest.fixture
def clock():
    clk = FakeClock()
    with patch.object(sink_mod, "time", clk):
        yield clk


@pytest.fixture
def connect():
    factory = ConnectionFactory()
    with patch.object(sink_mod.psycopg, "connect", factory):
        yield factory


@pytest.fixture
def sink(connect, clock):
    return PostgresSink(DSN, slow_after=1.5)


@pytest.fixture
def conn(sink, connect) -> FakeConnection:
    """The sink's live connection, with the schema-creation call cleared."""
    connect.last.calls.clear()
    return connect.last


def make_row(
    offset: int,
    *,
    source: str = "src",
    topic: str = "orders",
    partition: int = 0,
    headers: str | None = '{"k": "dg=="}',
    payload: bytes = b"payload",
    broker_ts_ms: int = 1_700_000_000_000,
) -> tuple:
    """A 12-column row in the layout produced by `tap.to_row`."""
    return (
        source,
        topic,
        partition,
        offset,
        broker_ts_ms,
        NOW_MS,
        headers,
        payload,
        7,  # schema_id
        None,  # schema_guid
        [0],  # message_index
        b"\x00" * 32,  # payload_sha256
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestInit:
    def test_connects_with_autocommit(self, connect, clock):
        PostgresSink(DSN, slow_after=1.5)

        assert [c.dsn for c in connect.connections] == [DSN]
        # Transactions are opened explicitly; autocommit keeps reads from
        # leaving an idle-in-transaction session behind.
        assert connect.last.autocommit is True

    def test_creates_schema_on_startup(self, connect, clock):
        PostgresSink(DSN, slow_after=1.5)

        assert [(c.kind, c.sql) for c in connect.last.calls] == [("execute", SCHEMA)]

    def test_starts_not_slow(self, sink):
        assert sink.last_write_seconds == 0.0
        assert sink.is_slow() is False

    def test_connection_failure_propagates(self, clock):
        with patch.object(
            sink_mod.psycopg, "connect", side_effect=psycopg.OperationalError("down")
        ):
            with pytest.raises(psycopg.OperationalError):
                PostgresSink(DSN, slow_after=1.5)


# ---------------------------------------------------------------------------
# write(): the happy path
# ---------------------------------------------------------------------------


class TestWrite:
    def test_inserts_rows_then_progress_in_one_transaction(self, sink, conn):
        rows = [make_row(0), make_row(1)]

        sink.write(rows)

        assert [(c.kind, c.sql, c.depth) for c in conn.calls] == [
            ("begin", None, 1),
            ("executemany", INSERT, 1),
            ("executemany", ADD_PROGRESS, 1),
            ("commit", None, 1),
        ]
        assert conn.statements(INSERT)[0].params == rows

    def test_progress_is_highest_offset_plus_one(self, sink, conn):
        # Out-of-order on purpose: the max wins, not the last seen.
        sink.write([make_row(5), make_row(9), make_row(7)])

        assert conn.statements(ADD_PROGRESS)[0].params == [("src", "orders", 0, 10)]

    def test_progress_is_tracked_per_source_topic_partition(self, sink, conn):
        sink.write(
            [
                make_row(0, partition=0),
                make_row(4, partition=0),
                make_row(2, partition=1),
                make_row(8, topic="payments", partition=0),
                make_row(3, source="other", partition=0),
            ]
        )

        progress = conn.statements(ADD_PROGRESS)[0].params
        assert sorted(progress) == [
            ("other", "orders", 0, 4),
            ("src", "orders", 0, 5),
            ("src", "orders", 1, 3),
            ("src", "payments", 0, 9),
        ]

    def test_empty_batch_records_no_progress(self, sink, conn):
        sink.write([])

        assert conn.statements(ADD_PROGRESS)[0].params == []
        assert conn.statements(INSERT)[0].params == []

    def test_records_how_long_the_write_took(self, sink, conn, clock):
        conn.on_statement = lambda call: clock.advance(0.4)

        sink.write([make_row(0)])

        # Two statements (INSERT, ADD_PROGRESS) at 0.4s each.
        assert sink.last_write_seconds == pytest.approx(0.8)

    def test_each_write_replaces_the_previous_duration(self, sink, conn, clock):
        conn.on_statement = lambda call: clock.advance(2.0)
        sink.write([make_row(0)])
        conn.on_statement = None

        sink.write([make_row(1)])

        assert sink.last_write_seconds == 0.0


# ---------------------------------------------------------------------------
# write(): connection handling
# ---------------------------------------------------------------------------


class TestWriteConnection:
    def test_reuses_an_open_connection(self, sink, connect):
        sink.write([make_row(0)])
        sink.write([make_row(1)])

        assert len(connect.connections) == 1

    def test_reconnects_when_connection_was_closed(self, sink, connect):
        connect.last.closed = True

        sink.write([make_row(0)])

        assert len(connect.connections) == 2
        assert sink.conn is connect.last
        assert connect.connections[1].statements(INSERT)  # wrote on the new one
        assert not connect.connections[0].statements(INSERT)

    def test_non_row_error_propagates_and_rolls_back(self, sink, conn):
        conn.reject_rows([make_row(0)], psycopg.OperationalError("connection lost"))

        with pytest.raises(psycopg.OperationalError):
            sink.write([make_row(0)])

        assert [c.kind for c in conn.calls if c.kind in ("commit", "rollback")] == [
            "rollback"
        ]
        # No per-row isolation or dead-lettering for infrastructure errors:
        # the tap must see the failure and retry the whole batch.
        assert not conn.statements(ADD_DEAD_LETTER)
        assert not conn.statements(ADD_PROGRESS)

    def test_failed_write_is_not_swallowed_before_close(self, sink, conn):
        conn.reject_rows([make_row(0)], psycopg.OperationalError("boom"))

        with pytest.raises(psycopg.OperationalError):
            sink.write([make_row(0)])
        sink.close()

        assert conn.closed


# ---------------------------------------------------------------------------
# write(): bad rows are isolated and dead-lettered
# ---------------------------------------------------------------------------


class TestDeadLetters:
    @pytest.fixture(autouse=True)
    def _freeze_clock(self, clock):
        clock.wall = NOW_S

    def test_batch_failure_falls_back_to_row_by_row(self, sink, conn):
        good_a, bad, good_b = make_row(0), make_row(1), make_row(2)
        conn.reject_rows([bad], psycopg.errors.CheckViolation("partition < 0"))

        sink.write([good_a, bad, good_b])

        # The first (batch) attempt is rolled back...
        assert [c.kind for c in conn.calls[:3]] == ["begin", "executemany", "rollback"]
        # ...then each row is inserted on its own.
        singles = [c.params for c in conn.calls if c.kind == "execute"]
        assert [r for r in singles if r in (good_a, bad, good_b)] == [
            good_a,
            bad,
            good_b,
        ]

    def test_only_the_bad_row_is_dead_lettered(self, sink, conn):
        good, bad = make_row(0), make_row(1, headers=None, payload=b"\xff\xfe")
        conn.reject_rows([bad], psycopg.errors.CheckViolation("nope"))

        sink.write([good, bad])

        (dead,) = conn.statements(ADD_DEAD_LETTER)
        assert dead.params == (
            "src",  # source
            "orders",  # topic
            0,  # partition
            1,  # offset
            1_700_000_000_000,  # broker_ts_ms
            NOW_MS,  # failed_at_ms: when it failed, not when it was captured
            "nope",  # error text
            None,  # headers_text
            b"\xff\xfe",  # payload
        )

    def test_dead_letter_error_text_is_the_exception_message(self, sink, conn):
        bad = make_row(0)
        conn.reject_rows([bad], psycopg.errors.NumericValueOutOfRange("int overflow"))

        sink.write([bad])

        assert conn.statements(ADD_DEAD_LETTER)[0].params[6] == "int overflow"

    @pytest.mark.parametrize(
        "exc",
        [
            psycopg.errors.CheckViolation("x"),
            psycopg.errors.UniqueViolation("x"),
            psycopg.errors.NumericValueOutOfRange("x"),
            psycopg.DataError("x"),
        ],
        ids=["check", "unique", "numeric-range", "data-error"],
    )
    def test_integrity_and_data_errors_both_dead_letter(self, sink, conn, exc):
        bad = make_row(0)
        conn.reject_rows([bad], exc)

        sink.write([bad])

        assert [c.params[3] for c in conn.statements(ADD_DEAD_LETTER)] == [0]

    def test_every_dead_row_is_recorded(self, sink, conn):
        rows = [make_row(i) for i in range(4)]
        conn.reject_rows([rows[0], rows[2]], psycopg.errors.CheckViolation("bad"))

        sink.write(rows)

        assert [c.params[3] for c in conn.statements(ADD_DEAD_LETTER)] == [0, 2]

    def test_progress_still_covers_the_whole_batch(self, sink, conn):
        # The bad row is parked in the dead-letter table, so the offset must
        # advance past it -- otherwise the tap would re-read it forever.
        rows = [make_row(0), make_row(1), make_row(2)]
        conn.reject_rows([rows[2]], psycopg.errors.CheckViolation("bad"))

        sink.write(rows)

        assert conn.statements(ADD_PROGRESS)[-1].params == [("src", "orders", 0, 3)]

    def test_inserts_dead_letters_and_progress_atomically(self, sink, conn):
        rows = [make_row(0), make_row(1)]
        conn.reject_rows([rows[1]], psycopg.errors.CheckViolation("bad"))

        sink.write(rows)

        # Attempt 1 (the batch) is rolled back. The retry then runs inside ONE
        # outer transaction, with a savepoint per row insert; the dead letter
        # and the progress update share that outer transaction and it commits
        # last.
        shape = [(c.kind, c.sql, c.depth) for c in conn.calls]
        assert shape == [
            ("begin", None, 1),
            ("executemany", INSERT, 1),
            ("rollback", None, 1),
            ("begin", None, 1),
            ("begin", None, 2),
            ("execute", INSERT, 2),
            ("commit", None, 2),
            ("begin", None, 2),
            ("execute", INSERT, 2),
            ("rollback", None, 2),
            ("execute", ADD_DEAD_LETTER, 1),
            ("executemany", ADD_PROGRESS, 1),
            ("commit", None, 1),
        ]

    def test_logs_a_warning_for_the_batch_and_an_error_per_dead_row(
        self, sink, conn, caplog
    ):
        bad = make_row(41, partition=3, topic="payments")
        conn.reject_rows([bad], psycopg.errors.CheckViolation("nope"))

        with caplog.at_level(logging.WARNING, logger=sink_mod.log.name):
            sink.write([bad])

        by_level = {r.levelno: r.getMessage() for r in caplog.records}
        assert "batch rejected" in by_level[logging.WARNING]
        assert "payments[3]@41" in by_level[logging.ERROR]
        assert "nope" in by_level[logging.ERROR]

    def test_non_row_error_during_isolation_propagates(self, sink, conn):
        # First attempt: row error -> isolation. During isolation the
        # connection dies: that must reach the tap, not be dead-lettered.
        bad = make_row(0)
        errors = [psycopg.errors.CheckViolation("bad"), psycopg.OperationalError("lost")]
        original_record = conn.record

        def flaky_record(kind, sql=None, params=None):
            if kind in ("execute", "executemany") and sql == INSERT:
                raise errors.pop(0)
            return original_record(kind, sql, params)

        conn.record = flaky_record

        with pytest.raises(psycopg.OperationalError):
            sink.write([bad])

        assert not conn.statements(ADD_DEAD_LETTER)

    def test_duration_is_recorded_on_the_dead_letter_path_too(
        self, sink, conn, clock
    ):
        bad = make_row(0)
        conn.reject_rows([bad], psycopg.errors.CheckViolation("bad"))
        conn.on_statement = lambda call: clock.advance(1.0)

        sink.write([bad])

        assert sink.last_write_seconds > 0
        assert sink.is_slow()  # >1.5s of statements


# ---------------------------------------------------------------------------
# is_slow / load_progress / close
# ---------------------------------------------------------------------------


class TestIsSlow:
    @pytest.mark.parametrize(
        "seconds, expected",
        [(0.0, False), (1.49, False), (1.5, False), (1.51, True), (30.0, True)],
    )
    def test_slow_only_when_strictly_over_threshold(self, sink, seconds, expected):
        sink.last_write_seconds = seconds

        assert sink.is_slow() is expected


class TestLoadProgress:
    def test_maps_topic_partition_to_next_offset(self, sink, conn):
        conn.progress_table = [("orders", 0, 10), ("orders", 1, 4), ("payments", 0, 99)]

        assert sink.load_progress("src") == {
            ("orders", 0): 10,
            ("orders", 1): 4,
            ("payments", 0): 99,
        }

    def test_queries_for_the_given_source_only(self, sink, conn):
        sink.load_progress("redpanda-prod")

        (call,) = conn.statements(READ_PROGRESS)
        assert call.params == ("redpanda-prod",)

    def test_unknown_source_has_no_progress(self, sink, conn):
        assert sink.load_progress("never-seen") == {}


class TestClose:
    def test_closes_the_connection(self, sink, conn):
        sink.close()

        assert conn.closed is True
