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
from .sink import PostgresSink
from .wire import parse_frame

log = logging.getLogger(__name__)


class Tap:
    def __init__(
        self, sink: PostgresSink, cfg: Config, consumer: Consumer | None = None
    ):
        self.sink = sink
        self.cfg = cfg
        self.c = consumer or build_consumer(cfg)
        self.buf: list[tuple] = []
        self.batch_started = 0.0
        self.resume_at: float | None = None

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

        if should_flush_buf(self.buf, age, self.cfg):
            self.flush()
            self._maybe_pause()
        self._maybe_resume()

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self.c.close()

    def flush(self) -> None:
        if not self.buf:
            return
        self.sink.write(self.buf)
        self.c.commit(offsets=offsets_from(self.buf), asynchronous=False)
        self.buf.clear()

    # ---------------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------------

    def _maybe_pause(self) -> None:
        if self.resume_at is None and self.sink.is_slow():
            assignment = self.c.assignment()
            if assignment:
                self.c.pause(assignment)
                self.resume_at = time.monotonic() + self.cfg.pause_cooldown_seconds

    def _maybe_resume(self) -> None:
        if self.resume_at is not None and time.monotonic() >= self.resume_at:
            self.c.resume(self.c.assignment())
            self.resume_at = None

    # If partitions were paused, on revoke, unpause partitions being kept
    def _on_revoke(self, consumer: Consumer, partitions) -> None:
        self.flush()
        if self.resume_at is not None:
            kept = [tp for tp in consumer.assignment() if tp not in partitions]
            consumer.resume(kept)
            self.resume_at = None


def build_consumer(cfg: Config) -> Consumer:
    conf = {
        "bootstrap.servers": cfg.brokers,
        "group.id": cfg.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",
        "isolation.level": "read_committed",
    }
    # TODO Add SASL_SSL if cfg.sasl_username statement
    return Consumer(conf)


def headers_to_dict(headers) -> dict | None:
    dic = {}
    if not headers:
        return None
    for key, val in headers:
        dic[key] = base64.b64encode(val or b"").decode()
    return dic


def to_row(message: Message, cfg: Config) -> tuple:
    value = message.value() or b""
    schema_id, indexes = parse_frame(value)
    headers_dict = headers_to_dict(message.headers())
    return (
        cfg.source,
        message.topic(),
        message.partition(),
        message.offset(),
        message.timestamp()[1],
        int(time.time() * 1000),
        message.key(),
        json.dumps(headers_dict),
        value,
        schema_id,
        indexes,
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
