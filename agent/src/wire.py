"""
"The message indexes are encoded as int using variable-length zigzag encoding,
prefixed by the length of the array,
which is also a variable length, zigzag encoded."
- https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format-schema-id-in-the-payload-prefix

There are 2 possible formats: Classic and GUID.
Classic (prefix):
- Byte 0: 0
- Bytes 1-4: Schema ID
- Bytes 5-x: Message indexes

GUID: (in header)
- Byte 0: Version Byte
- Byte 1-16: GUID
- Byte 17-x: Message indexes
"""

from dataclasses import dataclass
from enum import Enum, auto
from uuid import UUID

KEY_SCHEMA_ID_HEADER = "__key_schema_id"
VALUE_SCHEMA_ID_HEADER = "__value_schema_id"

_ID_SIZE = 4  # Classic ID size
_GUID_SIZE = 16  # GUID size
_MAX_VARINT_BYTES = 10  # 64-bit


@dataclass(frozen=True, slots=True)
class ClassicFrame:
    schema_id: int
    message_indexes: tuple[int, ...] = ()
    payload_offset: int = 5


@dataclass(frozen=True, slots=True)
class GuidFrame:
    schema_guid: UUID
    message_indexes: tuple[int, ...] = ()
    payload_offset: int = 0


@dataclass(frozen=True, slots=True)
class UnframedFrame:
    payload_offset: int = 0


Frame = ClassicFrame | GuidFrame | UnframedFrame


class FrameFormat(Enum):
    UNFRAMED = auto()
    CLASSIC = auto()
    GUID = auto()


class ConfluentFrameParser:
    def __init__(
        self, value: bytes, headers: list[tuple[str, bytes]], header_name: str
    ):
        self.value = value
        self.headers = headers
        self.header_name = header_name

    def parse(self) -> Frame:
        match self._find_format():
            case FrameFormat.CLASSIC:
                return self._parse_classic()
            case FrameFormat.GUID:
                return self._parse_guid()
            case FrameFormat.UNFRAMED:
                return UnframedFrame()
            case _:
                raise ValueError("Unknown frame")

    def _find_format(self) -> FrameFormat:
        for key, val in self.headers or []:
            if self.header_name == key and val:
                return FrameFormat.GUID
        if self.value and len(self.value) >= 5 and self.value[0] == 0:
            return FrameFormat.CLASSIC
        return FrameFormat.UNFRAMED

    def _parse_classic(self) -> ClassicFrame:
        schema_id = int.from_bytes(self.value[1 : 1 + _ID_SIZE], "big")
        pos = 1 + _ID_SIZE
        indexes, pos = _read_message_indexes(self.value, pos)
        return ClassicFrame(schema_id, indexes, pos)

    def _find_header_value(self) -> bytes | None:
        for key, val in reversed(
            self.headers or []
        ):  # They store newest headers at the end
            if key == self.header_name:
                return val
        return None

    def _parse_guid(self) -> GuidFrame:
        raw = self._find_header_value()
        if raw is None or len(raw) < (1 + _GUID_SIZE) or raw[0] != 1:
            raise ValueError("malformed guid header")
        guid = UUID(bytes=raw[1 : (1 + _GUID_SIZE)])
        indexes, _ = _read_message_indexes(raw, 1 + _GUID_SIZE)
        return GuidFrame(guid, indexes, payload_offset=0)


def _read_message_indexes(buf: bytes, pos: int) -> tuple[tuple[int, ...], int]:
    count, pos = _read_zigzag_varint(buf, pos)
    if count == 0:
        return ((0,), pos)
    if count < 0:
        raise ValueError("negative index count")
    indexes = []
    for _ in range(count):
        index, pos = _read_zigzag_varint(buf, pos)
        indexes.append(index)
    return (tuple(indexes), pos)


def _read_zigzag_varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    for i in range(_MAX_VARINT_BYTES):
        if pos + i >= len(buf):
            raise ValueError("truncated varint")
        b = buf[pos + i]
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            dropped_sign_bit = result >> 1  # Zigzag stores sign in the lowest bit
            mask = -(result & 1)  # If the lowest bit was 1, mask = 11111...
            new_result = (
                dropped_sign_bit ^ mask
            )  # If the mask is non-zero, turn the result negative
            return new_result, pos + i + 1
        shift += 7
    raise ValueError("varint longer than 10 bytes")
