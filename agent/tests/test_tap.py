"""
Unit tests for src/tap.py.

Nothing here touches a broker, a database, or the wall clock:

* `Consumer` is an autospec'd mock, so calls are checked against the real API.
* `PostgresSink` is replaced by `FakeSink`, which records what it was given.
* `time` inside `src.tap` is swapped for `FakeClock`, so batch-age and
  pause-cooldown logic is deterministic and no test sleeps.
* `Config` is the real settings model (built from explicit kwargs), so a
  renamed or removed field breaks these tests instead of being papered over.
* Buffering/flush/poll tests stub `to_row` (they care about batching, not row
  layout). `TestToRow` exercises the real `to_row` against the real `wire`
  parser, using hand-built Confluent frames.

`build_consumer` lives in `src.kafka` and is tested in `test_kafka.py`.
"""

import base64
import hashlib
import json
import logging
import threading
from collections import namedtuple
from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec, patch
from uuid import UUID

import pytest
from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from src import tap as tap_mod
from src.config import Config
from src.tap import (
    Tap,
    append_buf,
    check_error,
    headers_to_dict,
    offsets_from,
    should_flush_buf,
    to_row,
)
from src.wire import VALUE_SCHEMA_ID_HEADER

TS_MS = 1_700_000_000_123
INGESTED_AT_MS = 1_700_000_100_500  # FakeClock.wall, in ms
GUID = UUID("12345678-1234-5678-1234-567812345678")

# Column order of the tuples produced by `to_row` / consumed by the sink.
Row = namedtuple(
    "Row",
    "source topic partition offset kafka_ts ingested_at headers value "
    "schema_id schema_guid message_indexes value_sha256",
)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class FakeClock:
    """Stands in for the `time` module as seen by src.tap."""

    def __init__(self) -> None:
        self.now = 1_000.0  # monotonic seconds
        self.wall = 1_700_000_100.5  # epoch seconds

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.wall

    def advance(self, seconds: float) -> None:
        self.now += seconds
        self.wall += seconds


class FakeMessage:
    """Minimal confluent_kafka.Message look-alike (the real one can't be built)."""

    def __init__(
        self,
        topic="orders",
        partition=0,
        offset=0,
        key=b"key",
        value=b"payload",
        headers=None,
        timestamp=(TIMESTAMP_CREATE_TIME, TS_MS),
        error=None,
    ):
        self._topic, self._partition, self._offset = topic, partition, offset
        self._key, self._value, self._headers = key, value, headers
        self._timestamp, self._error = timestamp, error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def timestamp(self):
        return self._timestamp

    def error(self):
        return self._error


class FakeSink:
    """Records every batch it receives and the order of its side effects."""

    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.batches: list[list[tuple]] = []
        self.slow = False
        self.write_error: Exception | None = None

    def write(self, rows) -> None:
        if self.write_error:
            raise self.write_error
        self._events.append("write")
        # Tap clears its buffer right after write(), so keep a copy.
        self.batches.append(list(rows))

    def is_slow(self) -> bool:
        return self.slow


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clock(monkeypatch) -> FakeClock:
    fake = FakeClock()
    monkeypatch.setattr(tap_mod, "time", fake)
    return fake


@pytest.fixture
def cfg() -> Config:
    return Config(
        source="redpanda-prod",
        brokers="localhost:9092",
        group_id="protobs-tap",
        topics=["orders", "payments"],
        batch_size=3,
        max_batch_seconds=5.0,
        pause_cooldown_seconds=30.0,
    )


@pytest.fixture
def stub_to_row(monkeypatch) -> MagicMock:
    """Replace `to_row` with a signature-checked stub returning a simple Row.

    Keeps buffering/flush/poll tests independent of frame parsing. The stub
    carries the message's topic/partition/offset into the row, which is all
    that batching and commit logic reads. Set `.side_effect` to simulate a
    message that can't be converted.
    """
    fake = create_autospec(
        tap_mod.to_row,
        side_effect=lambda message, cfg: make_row(
            message.topic(), message.partition(), message.offset()
        ),
    )
    monkeypatch.setattr(tap_mod, "to_row", fake)
    return fake


@pytest.fixture
def events() -> list[str]:
    """Ordered log of write / commit / pause / resume / close side effects."""
    return []


@pytest.fixture
def sink(events) -> FakeSink:
    return FakeSink(events)


@pytest.fixture
def consumer(events) -> MagicMock:
    c = create_autospec(Consumer, instance=True)
    c.poll.return_value = None
    c.assignment.return_value = []
    for name in ("commit", "pause", "resume", "close"):
        getattr(c, name).side_effect = lambda *a, _n=name, **k: events.append(_n)
    return c


@pytest.fixture
def tap(sink, cfg, consumer, stub_to_row) -> Tap:
    return Tap(sink, cfg, consumer)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_row(topic="orders", partition=0, offset=0) -> Row:
    """A buffered row, built directly so it doesn't depend on `to_row`."""
    value = b"payload"
    return Row(
        source="redpanda-prod",
        topic=topic,
        partition=partition,
        offset=offset,
        kafka_ts=TS_MS,
        ingested_at=INGESTED_AT_MS,
        headers=None,
        value=value,
        schema_id=42,
        schema_guid=None,
        message_indexes=[0, 1],
        value_sha256=hashlib.sha256(value).digest(),
    )


def classic_frame(schema_id: int, payload: bytes, indexes=(0, 1)) -> bytes:
    """Confluent classic framing: magic 0, 4-byte BE id, zigzag-varint indexes.

    Only supports indexes < 64 (single-byte varints), which is all tests need.
    """
    index_bytes = bytes([len(indexes) << 1]) + bytes(i << 1 for i in indexes)
    return b"\x00" + schema_id.to_bytes(4, "big") + index_bytes + payload


def prefill(tap: Tap, clock: FakeClock, n: int) -> None:
    """Put n rows (offsets 0..n-1) in the buffer as if a batch were in progress."""
    tap.buf.extend(make_row(offset=i) for i in range(n))
    tap.batch_started = clock.now


def as_tuples(tps) -> list[tuple]:
    # TopicPartition.__eq__ ignores offset, so compare the fields explicitly.
    return [(tp.topic, tp.partition, tp.offset) for tp in tps]


def fatal_error() -> KafkaError:
    return KafkaError(KafkaError._FATAL, "fatal", fatal=True)


# ---------------------------------------------------------------------------
# headers_to_dict
# ---------------------------------------------------------------------------


class TestHeadersToDict:
    @pytest.mark.parametrize("headers", [None, []], ids=["none", "empty"])
    def test_missing_headers_return_none(self, headers):
        assert headers_to_dict(headers) is None

    def test_values_are_base64_encoded(self):
        result = headers_to_dict([("trace-id", b"\x00\xffabc")])

        assert result == {"trace-id": base64.b64encode(b"\x00\xffabc").decode()}

    def test_none_value_becomes_empty_string(self):
        assert headers_to_dict([("flag", None)]) == {"flag": ""}

    def test_duplicate_keys_keep_last_value(self):
        result = headers_to_dict([("k", b"first"), ("k", b"second")])

        assert result == {"k": base64.b64encode(b"second").decode()}


# ---------------------------------------------------------------------------
# to_row
# ---------------------------------------------------------------------------


class TestToRow:
    """Real `to_row` + real `wire` parser; no stubs beyond the fake clock."""

    def test_classic_frame_builds_every_column_in_order(self, cfg):
        value = classic_frame(42, b"payload", indexes=(0, 1))
        msg = FakeMessage(
            topic="orders",
            partition=2,
            offset=17,
            value=value,
            headers=[("trace-id", b"abc")],
        )

        row = Row(*to_row(msg, cfg))

        assert row == Row(
            source="redpanda-prod",
            topic="orders",
            partition=2,
            offset=17,
            kafka_ts=TS_MS,
            ingested_at=INGESTED_AT_MS,
            headers=json.dumps({"trace-id": base64.b64encode(b"abc").decode()}),
            value=value,  # stored whole, framing prefix included
            schema_id=42,
            schema_guid=None,
            message_indexes=[0, 1],
            value_sha256=hashlib.sha256(value).digest(),
        )

    def test_guid_header_frame_populates_guid_not_schema_id(self, cfg):
        header = b"\x01" + GUID.bytes + b"\x00"  # version 1, guid, indexes [0]
        msg = FakeMessage(value=b"payload", headers=[(VALUE_SCHEMA_ID_HEADER, header)])

        row = Row(*to_row(msg, cfg))

        assert row.schema_guid == GUID
        assert row.schema_id is None
        assert row.message_indexes == [0]
        assert row.value == b"payload"

    def test_unframed_value_has_no_schema_columns(self, cfg):
        row = Row(*to_row(FakeMessage(value=b"\x08\x96\x01"), cfg))

        assert row.schema_id is None
        assert row.schema_guid is None
        assert row.message_indexes is None
        assert row.value == b"\x08\x96\x01"

    def test_tombstone_value_is_treated_as_empty_bytes(self, cfg):
        row = Row(*to_row(FakeMessage(value=None), cfg))

        assert row.value == b""
        assert row.value_sha256 == hashlib.sha256(b"").digest()

    def test_missing_headers_are_stored_as_null(self, cfg):
        row = Row(*to_row(FakeMessage(headers=None), cfg))

        assert row.headers is None

    def test_malformed_frame_is_stored_unframed_and_logged(self, cfg, caplog):
        # A frame the parser rejects must not lose the message: it is kept
        # verbatim with no schema columns.
        msg = FakeMessage(
            value=b"payload",
            headers=[(VALUE_SCHEMA_ID_HEADER, b"\x09" + b"\x00" * 20)],
        )

        with caplog.at_level(logging.WARNING, logger=tap_mod.log.name):
            row = Row(*to_row(msg, cfg))

        assert row.value == b"payload"
        assert row.schema_id is None
        assert row.schema_guid is None
        assert row.message_indexes is None
        assert "frame parse failed" in caplog.text


# ---------------------------------------------------------------------------
# offsets_from
# ---------------------------------------------------------------------------


class TestOffsetsFrom:
    def test_empty_buffer_commits_nothing(self):
        assert offsets_from([]) == []

    def test_commits_next_offset_not_last_consumed(self, cfg):
        result = offsets_from([make_row(offset=4)])

        assert as_tuples(result) == [("orders", 0, 5)]

    def test_takes_highest_offset_per_partition_regardless_of_order(self, cfg):
        rows = [make_row(offset=o) for o in (7, 3, 9, 1)]

        assert as_tuples(offsets_from(rows)) == [("orders", 0, 10)]

    def test_tracks_each_topic_partition_independently(self, cfg):
        rows = [
            make_row("orders", 0, 10),
            make_row("orders", 1, 20),
            make_row("payments", 0, 30),
            make_row("orders", 0, 11),
        ]

        assert sorted(as_tuples(offsets_from(rows))) == [
            ("orders", 0, 12),
            ("orders", 1, 21),
            ("payments", 0, 31),
        ]


# ---------------------------------------------------------------------------
# should_flush_buf
# ---------------------------------------------------------------------------


class TestShouldFlushBuf:
    @pytest.mark.parametrize(
        ("rows", "age", "expected"),
        [
            (0, 999.0, False),  # nothing to flush, however old
            (1, 0.0, False),  # below size, fresh
            (2, 5.0, False),  # age must strictly exceed the max
            (3, 0.0, True),  # batch_size reached
            (4, 0.0, True),  # batch_size exceeded
            (1, 5.1, True),  # stale
        ],
    )
    def test_decision(self, cfg, rows, age, expected):
        buf = [make_row(offset=i) for i in range(rows)]

        assert should_flush_buf(buf, age, cfg) is expected


# ---------------------------------------------------------------------------
# append_buf
# ---------------------------------------------------------------------------


class TestAppendBuf:
    def test_none_message_leaves_buffer_and_batch_start_alone(self, cfg):
        buf: list[tuple] = []

        assert append_buf(None, buf, 12.0, cfg) == 12.0
        assert buf == []

    def test_errored_message_is_not_buffered(self, cfg):
        msg = FakeMessage(error=KafkaError(KafkaError._TRANSPORT, "down"))
        buf: list[tuple] = []

        assert append_buf(msg, buf, 12.0, cfg) == 12.0
        assert buf == []

    def test_first_message_starts_the_batch_clock(self, cfg, clock, stub_to_row):
        buf: list[tuple] = []

        started = append_buf(FakeMessage(offset=5), buf, 0.0, cfg)

        assert started == clock.now
        assert [r.offset for r in buf] == [5]
        stub_to_row.assert_called_once()

    def test_later_messages_keep_the_original_batch_start(
        self, cfg, clock, stub_to_row
    ):
        buf = [make_row(offset=0)]
        clock.advance(2)

        started = append_buf(FakeMessage(offset=1), buf, 1_000.0, cfg)

        assert started == 1_000.0
        assert [r.offset for r in buf] == [0, 1]

    def test_bad_message_is_logged_and_skipped(self, cfg, stub_to_row, caplog):
        stub_to_row.side_effect = ValueError("bad frame")
        msg = FakeMessage(topic="orders", partition=3, offset=9)
        buf: list[tuple] = []

        with caplog.at_level(logging.ERROR, logger="src.tap"):
            started = append_buf(msg, buf, 12.0, cfg)

        assert started == 12.0
        assert buf == []
        assert "skipping bad message orders[3]@9" in caplog.text
        assert caplog.records[0].exc_info is not None  # traceback was kept

    def test_valid_framed_message_is_buffered_by_the_real_to_row(self, cfg):
        # End-to-end through the real to_row: a well-formed message must be
        # buffered, not logged-and-dropped.
        msg = FakeMessage(value=classic_frame(42, b"payload"))
        buf: list[tuple] = []

        append_buf(msg, buf, 0.0, cfg)

        assert len(buf) == 1


# ---------------------------------------------------------------------------
# check_error
# ---------------------------------------------------------------------------


class TestCheckError:
    def test_none_message_is_fine(self):
        check_error(None)

    def test_message_without_error_is_fine(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="src.tap"):
            check_error(FakeMessage())

        assert caplog.records == []

    def test_partition_eof_is_ignored(self, caplog):
        msg = FakeMessage(error=KafkaError(KafkaError._PARTITION_EOF))

        with caplog.at_level(logging.DEBUG, logger="src.tap"):
            check_error(msg)

        assert caplog.records == []

    def test_fatal_error_raises(self):
        with pytest.raises(KafkaException):
            check_error(FakeMessage(error=fatal_error()))

    def test_non_fatal_error_is_logged_not_raised(self, caplog):
        msg = FakeMessage(error=KafkaError(KafkaError._TRANSPORT, "broker down"))

        with caplog.at_level(logging.WARNING, logger="src.tap"):
            check_error(msg)

        assert "kafka error" in caplog.text
        assert "broker down" in caplog.text


# ---------------------------------------------------------------------------
# Tap: construction
# ---------------------------------------------------------------------------


class TestTapInit:
    def test_uses_injected_consumer(self, sink, cfg, consumer):
        with patch.object(tap_mod, "build_consumer") as build:
            t = Tap(sink, cfg, consumer)

        assert t.c is consumer
        build.assert_not_called()

    def test_builds_consumer_from_config_when_none_injected(self, sink, cfg):
        with patch.object(tap_mod, "build_consumer") as build:
            t = Tap(sink, cfg)

        build.assert_called_once_with(cfg)
        assert t.c is build.return_value

    def test_starts_idle(self, tap):
        assert tap.buf == []
        assert tap.resume_at is None


# ---------------------------------------------------------------------------
# Tap.flush / Tap.close
# ---------------------------------------------------------------------------


class TestTapFlush:
    def test_noop_when_buffer_empty(self, tap, sink, consumer):
        tap.flush()

        assert sink.batches == []
        consumer.commit.assert_not_called()

    def test_writes_batch_commits_next_offsets_and_clears_buffer(
        self, tap, sink, consumer, cfg
    ):
        rows = [make_row(offset=4), make_row(offset=7)]
        tap.buf.extend(rows)

        tap.flush()

        assert sink.batches == [rows]
        kwargs = consumer.commit.call_args.kwargs
        assert kwargs["asynchronous"] is True
        assert as_tuples(kwargs["offsets"]) == [("orders", 0, 8)]
        assert tap.buf == []

    def test_commit_happens_only_after_sink_write(self, tap, cfg, events):
        tap.buf.append(make_row())

        tap.flush()

        assert events == ["write", "commit"]

    def test_write_failure_skips_commit_and_keeps_buffer(
        self, tap, sink, consumer, cfg
    ):
        sink.write_error = RuntimeError("db down")
        tap.buf.append(make_row())

        with pytest.raises(RuntimeError):
            tap.flush()

        consumer.commit.assert_not_called()
        assert len(tap.buf) == 1

    def test_commit_failure_is_logged_and_does_not_undo_the_write(
        self, tap, sink, consumer, caplog
    ):
        # The rows are already durable; a failed commit only means they may be
        # re-read (and de-duplicated by the sink) after a restart.
        consumer.commit.side_effect = KafkaException(fatal_error())
        rows = [make_row()]
        tap.buf.extend(rows)

        with caplog.at_level(logging.WARNING, logger=tap_mod.log.name):
            tap.flush()  # must not raise

        assert sink.batches == [rows]
        assert tap.buf == []
        assert "offset commit failed" in caplog.text


class TestTapClose:
    def test_flushes_then_closes_consumer(self, tap, cfg, events):
        tap.buf.append(make_row())

        tap.close()

        assert events == ["write", "commit", "close"]

    def test_closes_consumer_even_if_flush_fails(
        self, tap, sink, consumer, cfg, caplog
    ):
        sink.write_error = RuntimeError("db down")
        tap.buf.append(make_row())

        with caplog.at_level(logging.ERROR, logger=tap_mod.log.name):
            tap.close()  # shutdown must not raise

        consumer.close.assert_called_once()
        consumer.commit.assert_not_called()  # nothing was durable
        assert len(tap.buf) == 1
        assert "final flush failed, 1 buffered will be re-read" in caplog.text


# ---------------------------------------------------------------------------
# Tap.run
# ---------------------------------------------------------------------------


class TestTapRun:
    def test_subscribes_polls_until_stopped_then_closes(self, tap, consumer, cfg):
        stop = threading.Event()
        polls = []

        def poll(timeout):
            polls.append(timeout)
            if len(polls) == 3:
                stop.set()
            return None

        consumer.poll.side_effect = poll

        tap.run(stop)

        consumer.subscribe.assert_called_once_with(cfg.topics, on_revoke=tap._on_revoke)
        assert len(polls) == 3
        consumer.close.assert_called_once()

    def test_does_not_poll_if_already_stopped(self, tap, consumer):
        stop = threading.Event()
        stop.set()

        tap.run(stop)

        consumer.poll.assert_not_called()
        consumer.close.assert_called_once()

    def test_closes_consumer_when_poll_raises(self, tap, consumer):
        consumer.poll.return_value = FakeMessage(error=fatal_error())

        with pytest.raises(KafkaException):
            tap.run(threading.Event())

        consumer.close.assert_called_once()


# ---------------------------------------------------------------------------
# Tap.poll_once: buffering, flushing, backpressure
# ---------------------------------------------------------------------------


class TestTapPollOnce:
    def test_buffers_message_without_flushing_below_threshold(
        self, tap, sink, consumer, clock
    ):
        consumer.poll.return_value = FakeMessage(offset=5)

        tap.poll_once()

        consumer.poll.assert_called_once_with(1.0)
        assert [r.offset for r in tap.buf] == [5]
        assert tap.batch_started == clock.now
        assert sink.batches == []
        consumer.commit.assert_not_called()

    def test_flushes_when_batch_size_reached(self, tap, sink, consumer, cfg, clock):
        prefill(tap, clock, n=cfg.batch_size - 1)
        consumer.poll.return_value = FakeMessage(offset=cfg.batch_size - 1)

        tap.poll_once()

        assert [[r.offset for r in b] for b in sink.batches] == [[0, 1, 2]]
        assert as_tuples(consumer.commit.call_args.kwargs["offsets"]) == [
            ("orders", 0, 3)
        ]
        assert tap.buf == []

    def test_flushes_stale_batch_even_when_poll_returns_nothing(
        self, tap, sink, cfg, clock
    ):
        prefill(tap, clock, n=1)
        clock.advance(cfg.max_batch_seconds + 0.1)

        tap.poll_once()

        assert len(sink.batches) == 1
        assert tap.buf == []

    def test_keeps_fresh_partial_batch_when_poll_returns_nothing(
        self, tap, sink, cfg, clock
    ):
        prefill(tap, clock, n=1)
        clock.advance(cfg.max_batch_seconds - 0.1)

        tap.poll_once()

        assert sink.batches == []
        assert len(tap.buf) == 1

    def test_fatal_error_raises_before_anything_is_buffered(self, tap, consumer):
        consumer.poll.return_value = FakeMessage(error=fatal_error())

        with pytest.raises(KafkaException):
            tap.poll_once()

        assert tap.buf == []

    def test_non_fatal_error_message_is_skipped(self, tap, consumer):
        consumer.poll.return_value = FakeMessage(
            error=KafkaError(KafkaError._TRANSPORT, "down")
        )

        tap.poll_once()

        assert tap.buf == []


class TestTapBackpressure:
    @pytest.fixture
    def full_batch(self, tap, consumer, cfg, clock):
        """Arrange the next poll_once() to hit the batch_size flush threshold."""
        prefill(tap, clock, n=cfg.batch_size - 1)
        consumer.poll.return_value = FakeMessage(offset=cfg.batch_size - 1)

    @pytest.fixture
    def assigned(self, consumer):
        partitions = [TopicPartition("orders", 0), TopicPartition("orders", 1)]
        consumer.assignment.return_value = partitions
        return partitions

    def test_pauses_assignment_after_flush_when_sink_is_slow(
        self, tap, sink, consumer, cfg, clock, full_batch, assigned, events
    ):
        sink.slow = True

        tap.poll_once()

        consumer.pause.assert_called_once_with(assigned)
        assert tap.resume_at == clock.now + cfg.pause_cooldown_seconds
        assert events == ["write", "commit", "pause"]

    def test_does_not_pause_when_sink_is_healthy(
        self, tap, consumer, full_batch, assigned
    ):
        tap.poll_once()

        consumer.pause.assert_not_called()
        assert tap.resume_at is None

    def test_does_not_pause_without_an_assignment(
        self, tap, sink, consumer, full_batch
    ):
        sink.slow = True  # consumer.assignment() is [] by default

        tap.poll_once()

        consumer.pause.assert_not_called()
        assert tap.resume_at is None

    def test_does_not_pause_again_while_already_paused(
        self, tap, sink, consumer, clock, full_batch, assigned
    ):
        sink.slow = True
        tap.resume_at = clock.now + 10

        tap.poll_once()

        consumer.pause.assert_not_called()
        assert tap.resume_at == clock.now + 10

    def test_does_not_pause_when_nothing_was_flushed(
        self, tap, sink, consumer, assigned
    ):
        sink.slow = True

        tap.poll_once()  # empty buffer: nothing to flush

        consumer.pause.assert_not_called()

    def test_stays_paused_until_cooldown_elapses(self, tap, consumer, clock, assigned):
        tap.resume_at = clock.now + 10

        tap.poll_once()

        consumer.resume.assert_not_called()
        assert tap.resume_at is not None

    def test_resumes_once_cooldown_has_elapsed(self, tap, consumer, clock, assigned):
        tap.resume_at = clock.now  # boundary: now >= resume_at

        tap.poll_once()

        consumer.resume.assert_called_once_with(assigned)
        assert tap.resume_at is None

    def test_does_not_resume_when_never_paused(self, tap, consumer, assigned):
        tap.poll_once()

        consumer.resume.assert_not_called()


# ---------------------------------------------------------------------------
# Tap._on_revoke (rebalance callback registered by run())
# ---------------------------------------------------------------------------


class TestTapOnRevoke:
    @pytest.fixture
    def partitions(self):
        return SimpleNamespace(
            kept=TopicPartition("orders", 0), revoked=TopicPartition("orders", 1)
        )

    def test_flushes_buffer_before_partitions_are_taken_away(
        self, tap, sink, consumer, cfg, partitions
    ):
        tap.buf.append(make_row(partition=1, offset=3))

        tap._on_revoke(consumer, [partitions.revoked])

        assert len(sink.batches) == 1
        assert as_tuples(consumer.commit.call_args.kwargs["offsets"]) == [
            ("orders", 1, 4)
        ]
        assert tap.buf == []

    def test_resumes_only_kept_partitions_when_paused(
        self, tap, consumer, clock, partitions
    ):
        consumer.assignment.return_value = [partitions.kept, partitions.revoked]
        tap.resume_at = clock.now + 10

        tap._on_revoke(consumer, [partitions.revoked])

        resumed = consumer.resume.call_args.args[0]
        assert as_tuples(resumed) == [("orders", 0, -1001)]
        assert tap.resume_at is None

    def test_does_not_resume_when_not_paused(self, tap, consumer, partitions):
        consumer.assignment.return_value = [partitions.kept, partitions.revoked]

        tap._on_revoke(consumer, [partitions.revoked])

        consumer.resume.assert_not_called()

    def test_flush_happens_before_resume(
        self, tap, consumer, cfg, clock, partitions, events
    ):
        consumer.assignment.return_value = [partitions.kept, partitions.revoked]
        tap.resume_at = clock.now + 10
        tap.buf.append(make_row())

        tap._on_revoke(consumer, [partitions.revoked])

        assert events == ["write", "commit", "resume"]
