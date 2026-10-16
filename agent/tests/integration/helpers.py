"""
Helpers shared by the integration tests.

The frame builders are written from the Confluent wire-format spec and do not
import anything from `src.wire`, so a parser bug can't be mirrored here.
"""

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable
from uuid import UUID

from src.tap import Tap
from src.wire import VALUE_SCHEMA_ID_HEADER

WAIT_SECONDS = 30.0


# ---------------------------------------------------------------------------
# Wire-format builders
# ---------------------------------------------------------------------------


def zigzag_varint(n: int) -> bytes:
    zz = (n << 1) ^ (n >> 63)
    out = bytearray()
    while True:
        low, zz = zz & 0x7F, zz >> 7
        if zz:
            out.append(low | 0x80)
        else:
            out.append(low)
            return bytes(out)


def encode_indexes(indexes: tuple[int, ...]) -> bytes:
    # [0] is the common case and is written as a single 0 byte (count == 0).
    if indexes == (0,):
        return zigzag_varint(0)
    return zigzag_varint(len(indexes)) + b"".join(zigzag_varint(i) for i in indexes)


def classic_frame(schema_id: int, payload: bytes, indexes=(0,)) -> bytes:
    """magic byte 0 | 4-byte big-endian schema id | message indexes | payload"""
    return (
        b"\x00" + schema_id.to_bytes(4, "big") + encode_indexes(tuple(indexes)) + payload
    )


def guid_header(guid: UUID, indexes=(0,)) -> tuple[str, bytes]:
    """The (name, value) header pair that carries a GUID-style schema id."""
    return (
        VALUE_SCHEMA_ID_HEADER,
        b"\x01" + guid.bytes + encode_indexes(tuple(indexes)),
    )


# ---------------------------------------------------------------------------
# A stand-in for confluent_kafka.Message (used where no broker is needed)
# ---------------------------------------------------------------------------


@dataclass
class StubMessage:
    topic_: str = "orders"
    partition_: int = 0
    offset_: int = 0
    value_: bytes | None = b""
    headers_: list[tuple[str, bytes]] | None = None
    timestamp_ms: int = 1_700_000_000_000
    key_: bytes | None = None

    def topic(self):
        return self.topic_

    def partition(self):
        return self.partition_

    def offset(self):
        return self.offset_

    def value(self):
        return self.value_

    def key(self):
        return self.key_

    def headers(self):
        return self.headers_

    def timestamp(self):
        return (1, self.timestamp_ms)  # (TIMESTAMP_CREATE_TIME, ms)

    def error(self):
        return None


# ---------------------------------------------------------------------------
# Running a Tap
# ---------------------------------------------------------------------------


@dataclass
class RunningTap:
    """Runs `tap.run` on a thread; `stop()` (or leaving the block) ends it.

    Errors raised inside the thread are re-raised on the test's thread so a
    crashed tap fails the test instead of silently timing it out.
    """

    tap: Tap
    stop_event: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None
    _thread: threading.Thread | None = None

    def _target(self) -> None:
        try:
            self.tap.run(self.stop_event)
        except BaseException as exc:  # noqa: BLE001 - re-raised in stop()
            self.error = exc

    def __enter__(self) -> "RunningTap":
        self._thread = threading.Thread(target=self._target, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self.stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=WAIT_SECONDS)
            assert not self._thread.is_alive(), "tap did not stop"
            self._thread = None
        if self.error is not None:
            raise self.error

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            self.stop_event.set()
            if self._thread is not None:
                self._thread.join(timeout=WAIT_SECONDS)
            return
        self.stop()


def wait_for(
    predicate: Callable[[], object],
    *,
    timeout: float = WAIT_SECONDS,
    interval: float = 0.1,
    message: str = "condition not met in time",
):
    """Poll until `predicate()` is truthy and return its value."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"{message} (last value: {last!r})")


def unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"
