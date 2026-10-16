"""
Integration tests: wire.py -> tap.py -> sink.py -> Postgres, with no broker.

Messages are stubbed at the Kafka boundary only. Frame bytes go through the
real wire parser, the real `to_row` / `Tap`, and the real sink into a real
database, so these catch contract drift *between* the modules: a column the
parser produces that the table can't hold, a frame the tap mishandles, an
offset the sink and the tap disagree about.

Needs TEST_DATABASE_URL; the Kafka-backed variants live in
test_pipeline_kafka.py.
"""

import hashlib
import logging
import uuid
from unittest.mock import create_autospec

import pytest
from confluent_kafka import Consumer, TopicPartition
from src import tap as tap_mod
from src.config import Config
from src.tap import Tap, to_row
from src.wire import VALUE_SCHEMA_ID_HEADER

from .helpers import StubMessage, classic_frame, guid_header, zigzag_varint

GUID = uuid.UUID("12345678-1234-5678-1234-567812345678")
TS_MS = 1_700_000_000_123


@pytest.fixture
def cfg() -> Config:
    return Config(source="it", batch_size=3, max_batch_seconds=60.0)


def capture(sink, cfg, *messages: StubMessage) -> None:
    """to_row every message and write them as one batch."""
    sink.write([to_row(m, cfg) for m in messages])


def stored(db):
    """The single captured row as a dict."""
    cur = db.execute("SELECT * FROM captured_messages")
    cols = [d.name for d in cur.description]
    (row,) = cur.fetchall()
    return dict(zip(cols, row))


# ---------------------------------------------------------------------------
# Every frame variant survives parse -> row -> insert
# ---------------------------------------------------------------------------


class TestFramesLandInPostgres:
    def test_classic_frame(self, sink, db, cfg):
        value = classic_frame(42, b"proto-bytes", indexes=(0,))

        capture(sink, cfg, StubMessage(offset_=5, value_=value, timestamp_ms=TS_MS))

        row = stored(db)
        assert row["schema_id"] == 42
        assert row["schema_guid"] is None
        assert row["message_index"] == [0]
        assert bytes(row["payload"]) == value  # framing prefix is kept
        assert (row["source"], row["topic"], row["partition"], row["offset"]) == (
            "it",
            "orders",
            0,
            5,
        )
        assert row["broker_ts_ms"] == TS_MS

    def test_classic_frame_with_nested_message_indexes(self, sink, db, cfg):
        value = classic_frame(7, b"x", indexes=(2, 0, 1))

        capture(sink, cfg, StubMessage(value_=value))

        assert stored(db)["message_index"] == [2, 0, 1]

    def test_guid_header_frame(self, sink, db, cfg):
        message = StubMessage(
            value_=b"raw-protobuf", headers_=[guid_header(GUID, indexes=(1, 3))]
        )

        capture(sink, cfg, message)

        row = stored(db)
        assert row["schema_guid"] == GUID
        assert row["schema_id"] is None
        assert row["message_index"] == [1, 3]
        assert bytes(row["payload"]) == b"raw-protobuf"

    def test_unframed_value(self, sink, db, cfg):
        capture(sink, cfg, StubMessage(value_=b'{"plain": "json"}'))

        row = stored(db)
        assert row["schema_id"] is None
        assert row["schema_guid"] is None
        assert row["message_index"] is None
        assert bytes(row["payload"]) == b'{"plain": "json"}'

    @pytest.mark.parametrize("value", [None, b""], ids=["tombstone", "empty"])
    def test_empty_values_are_stored_as_empty_payloads(self, sink, db, cfg, value):
        capture(sink, cfg, StubMessage(value_=value))

        row = stored(db)
        assert bytes(row["payload"]) == b""
        assert row["schema_id"] is None

    def test_short_value_starting_with_a_zero_byte_is_unframed(self, sink, db, cfg):
        capture(sink, cfg, StubMessage(value_=b"\x00\x01\x02"))

        assert stored(db)["schema_id"] is None

    def test_max_int32_schema_id_fits_the_column(self, sink, db, cfg):
        capture(sink, cfg, StubMessage(value_=classic_frame(2**31 - 1, b"p")))

        assert stored(db)["schema_id"] == 2**31 - 1

    def test_sha256_matches_what_postgres_computes(self, sink, db, cfg):
        value = classic_frame(1, bytes(range(256)))
        capture(sink, cfg, StubMessage(value_=value))

        (match,) = db.execute(
            "SELECT payload_sha256 = sha256(payload) FROM captured_messages"
        ).fetchone()
        assert match is True
        assert bytes(stored(db)["payload_sha256"]) == hashlib.sha256(value).digest()


# ---------------------------------------------------------------------------
# Frames the parser rejects must still be captured
# ---------------------------------------------------------------------------


class TestUnparseableFramesAreStillCaptured:
    @pytest.mark.parametrize(
        "value, headers",
        [
            # classic magic byte, but the message-index varint is cut off
            (b"\x00" + (9).to_bytes(4, "big") + b"\x80", None),
            # classic header says "3 indexes" but the buffer ends
            (b"\x00" + (9).to_bytes(4, "big") + zigzag_varint(3) + b"\x02", None),
            # GUID header is too short to hold a GUID
            (b"payload", [(VALUE_SCHEMA_ID_HEADER, b"\x01\x02\x03")]),
            # GUID header has an unknown version byte
            (b"payload", [(VALUE_SCHEMA_ID_HEADER, b"\x02" + GUID.bytes + b"\x00")]),
        ],
        ids=["truncated-varint", "truncated-indexes", "short-guid", "bad-version"],
    )
    def test_stored_verbatim_as_unframed(self, sink, db, cfg, caplog, value, headers):
        with caplog.at_level(logging.WARNING, logger=tap_mod.log.name):
            capture(sink, cfg, StubMessage(value_=value, headers_=headers))

        row = stored(db)
        assert bytes(row["payload"]) == value
        assert (row["schema_id"], row["schema_guid"], row["message_index"]) == (
            None,
            None,
            None,
        )
        assert any("frame parse failed" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------


class TestHeaders:
    def test_headers_are_stored_as_base64_in_jsonb(self, sink, db, cfg):
        capture(
            sink,
            cfg,
            StubMessage(
                value_=b"x",
                headers_=[("trace-id", b"abc"), ("blob", b"\xff\x00\xfe"), ("nil", None)],
            ),
        )

        assert stored(db)["headers"] == {
            "trace-id": "YWJj",
            "blob": "/wD+",
            "nil": "",
        }

    def test_duplicate_header_keys_keep_the_last_value(self, sink, db, cfg):
        capture(
            sink,
            cfg,
            StubMessage(value_=b"x", headers_=[("k", b"old"), ("k", b"new")]),
        )

        assert stored(db)["headers"] == {"k": "bmV3"}

    def test_no_headers_is_sql_null_not_json_null(self, sink, db, cfg):
        capture(sink, cfg, StubMessage(value_=b"x", headers_=None))

        (is_sql_null,) = db.execute(
            "SELECT headers IS NULL FROM captured_messages"
        ).fetchone()
        assert is_sql_null is True

    def test_unicode_header_names_round_trip(self, sink, db, cfg):
        capture(sink, cfg, StubMessage(value_=b"x", headers_=[("ключ-🔑", b"v")]))

        assert stored(db)["headers"] == {"ключ-🔑": "dg=="}


# ---------------------------------------------------------------------------
# Wire <-> column-type contract
# ---------------------------------------------------------------------------


class TestWireMeetsSchema:
    def test_schema_id_beyond_int32_is_dead_lettered_not_lost(self, sink, db, cfg):
        # Classic ids are 4 *unsigned* bytes; the column is a signed int4.
        # The row must be parked, not crash the batch or vanish.
        ok = StubMessage(offset_=0, value_=classic_frame(1, b"a"))
        too_big = StubMessage(offset_=1, value_=classic_frame(2**31, b"b"))
        ok_too = StubMessage(offset_=2, value_=classic_frame(2, b"c"))

        capture(sink, cfg, ok, too_big, ok_too)

        assert [
            r[0] for r in db.execute('SELECT "offset" FROM captured_messages ORDER BY 1')
        ] == [0, 2]
        ((offset, payload),) = db.execute(
            'SELECT "offset", payload FROM capture_dead_letters'
        ).fetchall()
        assert offset == 1
        assert bytes(payload) == classic_frame(2**31, b"b")
        assert sink.load_progress("it") == {("orders", 0): 3}


# ---------------------------------------------------------------------------
# Tap driving a real sink (consumer scripted, everything else real)
# ---------------------------------------------------------------------------


@pytest.fixture
def consumer():
    c = create_autospec(Consumer, instance=True)
    c.assignment.return_value = [TopicPartition("orders", 0)]
    return c


def deliver(consumer, *messages):
    """Make successive polls return `messages`, then None forever."""
    pending = list(messages)
    consumer.poll.side_effect = lambda timeout=None: pending.pop(0) if pending else None


class TestTapDrivesSink:
    def test_flushes_full_batch_to_postgres_then_commits(
        self, sink, db, cfg, consumer
    ):
        events = []

        def on_commit(**kwargs):
            (rows,) = db.execute("SELECT count(*) FROM captured_messages").fetchone()
            events.append((kwargs["offsets"], rows))

        consumer.commit.side_effect = on_commit
        deliver(
            consumer,
            *[StubMessage(offset_=i, value_=classic_frame(5, b"m")) for i in range(3)],
        )
        tap = Tap(sink, cfg, consumer=consumer)  # batch_size == 3

        for _ in range(3):
            tap.poll_once()

        # At commit time the rows were already durable in Postgres.
        ((offsets, rows_at_commit),) = events
        assert rows_at_commit == 3
        assert [(o.topic, o.partition, o.offset) for o in offsets] == [("orders", 0, 3)]
        assert sink.load_progress("it") == {("orders", 0): 3}

    def test_close_flushes_a_partial_batch(self, sink, db, cfg, consumer):
        deliver(
            consumer,
            StubMessage(offset_=0, value_=b"a"),
            StubMessage(offset_=1, value_=b"b"),
        )
        tap = Tap(sink, cfg, consumer=consumer)
        tap.poll_once()
        tap.poll_once()

        tap.close()

        assert db.execute("SELECT count(*) FROM captured_messages").fetchone() == (2,)
        consumer.close.assert_called_once()

    def test_poison_message_does_not_block_the_commit(self, sink, db, cfg, consumer):
        deliver(
            consumer,
            StubMessage(offset_=0, value_=classic_frame(1, b"ok")),
            StubMessage(offset_=1, value_=classic_frame(2**31, b"poison")),
            StubMessage(offset_=2, value_=classic_frame(3, b"ok")),
        )
        tap = Tap(sink, cfg, consumer=consumer)

        for _ in range(3):
            tap.poll_once()

        (kwargs,) = [c.kwargs for c in consumer.commit.call_args_list]
        assert [o.offset for o in kwargs["offsets"]] == [3]  # past the poison row
        assert tap.buf == []
        assert db.execute("SELECT count(*) FROM capture_dead_letters").fetchone() == (1,)

    def test_sink_outage_keeps_the_batch_and_retries_without_loss(
        self, sink, db, cfg, consumer, monkeypatch
    ):
        monkeypatch.setattr(Tap, "RETRY_INITIAL_SECONDS", 0.0)
        deliver(
            consumer,
            *[StubMessage(offset_=i, value_=classic_frame(5, b"m")) for i in range(3)],
        )
        tap = Tap(sink, cfg, consumer=consumer)
        sink.conn.close()  # the database "goes away"; psycopg reports closed
        # ...and comes back on the next write attempt (the sink reconnects).
        original_connect = sink._connect
        attempts = []

        def flaky_connect():
            attempts.append(1)
            if len(attempts) == 1:
                raise ConnectionError("database still down")
            return original_connect()

        monkeypatch.setattr(sink, "_connect", flaky_connect)

        tap.poll_once()
        tap.poll_once()
        tap.poll_once()  # batch full -> flush fails -> backoff

        assert len(tap.buf) == 3  # nothing lost, nothing committed
        consumer.commit.assert_not_called()
        consumer.pause.assert_called_once()

        tap.poll_once()  # retry_at has passed -> flush succeeds

        assert tap.buf == []
        assert db.execute("SELECT count(*) FROM captured_messages").fetchone() == (3,)
        consumer.resume.assert_called()
        assert [o.offset for o in consumer.commit.call_args.kwargs["offsets"]] == [3]
