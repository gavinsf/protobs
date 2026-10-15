"""
Polls the redpanda pipeline, not modifying the source.
Polling speed doesn't change.

Message docs: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-message
"""

import base64
import hashlib
import json
import logging
import threading
import time

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)

from .config import Config
from .kafka import build_consumer
from .sink import PostgresSink
from .wire import (
    KEY_SCHEMA_ID_HEADER,
    VALUE_SCHEMA_ID_HEADER,
    ClassicFrame,
    ConfluentFrameParser,
    GuidFrame,
    UnframedFrame,
)

log = logging.getLogger(__name__)


class Tap:
    RETRY_INITIAL_SECONDS = 1.0
    RETRY_MAX_SECONDS = 30.0

    def __init__(
        self, sink: PostgresSink, cfg: Config, consumer: Consumer | None = None
    ):
        self.sink = sink
        self.cfg = cfg
        self.c = consumer or build_consumer(cfg)
        self.buf: list[tuple] = []
        self.batch_started = 0.0
        self.resume_at: float | None = None
        self.retry_at: float | None = None
        self.backoff = self.RETRY_INITIAL_SECONDS

    def run(self, stop: threading.Event) -> None:
        self.c.subscribe(self.cfg.topics, on_revoke=self._on_revoke)
        try:
            while not stop.is_set():
                self.poll_once()
        finally:
            self.close()

    def poll_once(self) -> None:
        message = self.c.poll(1.0)
        check_error(message)
        self.batch_started = append_buf(message, self.buf, self.batch_started, self.cfg)
        age = time.monotonic() - self.batch_started

        if self.retry_at is not None:
            if time.monotonic() >= self.retry_at:
                self._try_flush()
        elif should_flush_buf(self.buf, age, self.cfg) and self._try_flush():
            self._maybe_pause()
        self._maybe_resume()

    def close(self) -> None:
        try:
            self.flush()
        except Exception:
            log.exception(
                "final flush failed, %d buffered will be re-read", len(self.buf)
            )
        finally:
            self.c.close()

    def flush(self) -> None:
        if not self.buf:
            return
        self.sink.write(self.buf)
        offsets = offsets_from(self.buf)
        self.buf.clear()
        try:
            self.c.commit(offsets=offsets, asynchronous=True)
        except KafkaException:
            log.warning("offset commit failed", exc_info=True)

    # ---------------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------------
    def _try_flush(self) -> bool:
        try:
            self.flush()
        except Exception:
            self._enter_backoff()
            return False
        self._leave_backoff()
        return True

    def _enter_backoff(self):
        log.exception("Flush failed. Retrying in %.0fs", self.backoff)
        assignment = self.c.assignment()
        if assignment:
            self.c.pause(assignment)
        self.retry_at = time.monotonic() + self.backoff
        self.backoff = min(self.backoff * 3, self.RETRY_MAX_SECONDS)

    def _leave_backoff(self):
        if self.retry_at is not None:
            log.info("sink recovered, resuming")
            self.c.resume(self.c.assignment())
            self.retry_at = None
            self.resume_at = None
        self.backoff = self.RETRY_INITIAL_SECONDS

    def _maybe_pause(self) -> None:
        if self.resume_at is None and self.sink.is_slow():
            assignment = self.c.assignment()
            if assignment:
                self.c.pause(assignment)
                self.resume_at = time.monotonic() + self.cfg.pause_cooldown_seconds

    def _maybe_resume(self) -> None:
        if self.retry_at is not None:
            return
        if self.resume_at is not None and time.monotonic() >= self.resume_at:
            self.c.resume(self.c.assignment())
            self.resume_at = None

    # If partitions were paused, on revoke, unpause partitions being kept
    def _on_revoke(self, consumer: Consumer, partitions) -> None:
        try:
            self.flush()
        except Exception:
            log.exception("flush on revoke failed, dropping %d buffered", len(self.buf))
            self.buf.clear()
        if self.resume_at is not None:
            kept = [tp for tp in consumer.assignment() if tp not in partitions]
            consumer.resume(kept)
            self.resume_at = None


def headers_to_dict(headers) -> dict | None:
    dic = {}
    if not headers:
        return None
    for key, val in headers:
        dic[key] = base64.b64encode(val or b"").decode()
    return dic


def parse_value_frame(value, headers):
    try:
        return ConfluentFrameParser(value, headers, VALUE_SCHEMA_ID_HEADER).parse()
    except Exception:
        log.warning("frame parse failed, storing as unframed", exc_info=True)
        return UnframedFrame()


def to_row(message: Message, cfg: Config) -> tuple:
    value = message.value() or b""
    # key = message.key()
    headers = message.headers()

    value_frame = parse_value_frame(value, headers)
    # Only the columns that apply to the frame variant are set; the rest are NULL.
    schema_id = value_frame.schema_id if isinstance(value_frame, ClassicFrame) else None
    schema_guid = (
        value_frame.schema_guid if isinstance(value_frame, GuidFrame) else None
    )
    message_indexes = (
        None
        if isinstance(value_frame, UnframedFrame)
        else list(value_frame.message_indexes)
    )

    headers_dict = headers_to_dict(headers)
    headers_json = json.dumps(headers_dict) if headers_dict is not None else None
    """ TODO Allow key frame handling
    key_frame = (
        ConfluentFrameParser(key, headers, KEY_SCHEMA_ID_HEADER) if key else None
    )
    """

    return (
        cfg.source,
        message.topic(),
        message.partition(),
        message.offset(),
        message.timestamp()[1],
        int(time.time() * 1000),
        headers_json,
        value,
        schema_id,
        schema_guid,
        message_indexes,
        # key,
        # key_frame.schema_id if key_frame else None,
        # key_frame.schema_guid if key_frame else None,
        # key_frame.message_indexes if key_frame else None,
        hashlib.sha256(value).digest(),
    )


def should_flush_buf(buf, age, cfg) -> bool:
    return bool(buf) and (len(buf) >= cfg.batch_size or age > cfg.max_batch_seconds)


def offsets_from(buf):
    latest = {}
    for row in buf:
        key = (row[1], row[2])
        latest[key] = max(latest.get(key, -1), row[3])
    return [TopicPartition(t, p, o + 1) for (t, p), o in latest.items()]


def append_buf(
    message: Message, buf: list[tuple], batch_started: float, cfg: Config
) -> float:
    if message is None or message.error():
        return batch_started
    try:
        row = to_row(message, cfg)
    except Exception:
        log.exception(
            "skipping bad message %s[%d]@%d",
            message.topic(),
            message.partition(),
            message.offset(),
        )
        return batch_started
    if not buf:
        batch_started = time.monotonic()
    buf.append(row)
    return batch_started


def check_error(message: Message | None) -> None:
    if message is None:
        return
    err = message.error()
    if err is None or err.code() == KafkaError._PARTITION_EOF:
        return
    if err.fatal():
        raise KafkaException(err)
    log.warning("kafka error: %s", err)
