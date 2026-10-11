"""
Unit tests for src/wire.py (Confluent wire-format frame parsing).

Pure functions and frozen dataclasses only: no I/O, no mocks beyond one
`monkeypatch` to reach an otherwise-unreachable defensive branch.

Layout mirrors the module, bottom-up:

    _read_zigzag_varint  ->  _read_message_indexes  ->  ConfluentFrameParser

Test inputs are built with the helpers below (an independent encoder written
from the Confluent/protobuf spec), and the encoder itself is pinned against
hand-computed byte vectors, so a bug in `wire.py` can't be mirrored by a
matching bug in the tests.
"""

import dataclasses
import random
from uuid import UUID

import pytest
from src import wire
from src.wire import (
    KEY_SCHEMA_ID_HEADER,
    VALUE_SCHEMA_ID_HEADER,
    ClassicFrame,
    ConfluentFrameParser,
    Frame,
    FrameFormat,
    GuidFrame,
    UnframedFrame,
    _read_message_indexes,
    _read_zigzag_varint,
)

INT64_MAX = 2**63 - 1
INT64_MIN = -(2**63)

GUID = UUID("12345678-1234-5678-1234-567812345678")
OTHER_GUID = UUID("ffffffff-0000-4000-8000-0123456789ab")
HEADER = VALUE_SCHEMA_ID_HEADER
PAYLOAD = b"\xde\xad\xbe\xef"


# ---------------------------------------------------------------------------
# Builders (independent of src.wire)
# ---------------------------------------------------------------------------


def zigzag_varint(n: int) -> bytes:
    """Encode a signed 64-bit int as a zigzag + base-128 varint."""
    zz = (n << 1) ^ (n >> 63)
    out = bytearray()
    while True:
        low, zz = zz & 0x7F, zz >> 7
        if zz:
            out.append(low | 0x80)
        else:
            out.append(low)
            return bytes(out)


def message_indexes(*indexes: int) -> bytes:
    """Encode message indexes: [count, idx...], with the `[0]` shortcut.

    The spec encodes the common first-message case `[0]` as a single 0 byte.
    """
    if indexes == (0,):
        return zigzag_varint(0)
    return zigzag_varint(len(indexes)) + b"".join(zigzag_varint(i) for i in indexes)


def classic_value(schema_id: int, indexes: tuple[int, ...] = (0,), payload=PAYLOAD):
    return b"\x00" + schema_id.to_bytes(4, "big") + message_indexes(*indexes) + payload


def guid_header(guid: UUID, indexes: tuple[int, ...] = (0,), version: int = 1):
    return bytes([version]) + guid.bytes + message_indexes(*indexes)


def parse(value, headers=None, header_name=HEADER) -> Frame:
    return ConfluentFrameParser(value, headers, header_name).parse()


# ---------------------------------------------------------------------------
# Test-helper sanity: the reference encoder against known byte vectors
# ---------------------------------------------------------------------------

# (encoded bytes, decoded value), computed by hand from the zigzag definition.
VARINT_VECTORS = [
    pytest.param(b"\x00", 0, id="0"),
    pytest.param(b"\x01", -1, id="-1"),
    pytest.param(b"\x02", 1, id="1"),
    pytest.param(b"\x03", -2, id="-2"),
    pytest.param(b"\x04", 2, id="2"),
    pytest.param(b"\x7e", 63, id="63-largest-1-byte-positive"),
    pytest.param(b"\x7f", -64, id="-64-largest-1-byte-negative"),
    pytest.param(b"\x80\x01", 64, id="64-first-2-byte"),
    pytest.param(b"\x81\x01", -65, id="-65-first-2-byte-negative"),
    pytest.param(b"\xfe\xff\xff\xff\x0f", 2**31 - 1, id="int32-max"),
    pytest.param(b"\xff\xff\xff\xff\x0f", -(2**31), id="int32-min"),
    pytest.param(b"\xfe" + b"\xff" * 8 + b"\x01", INT64_MAX, id="int64-max"),
    pytest.param(b"\xff" * 9 + b"\x01", INT64_MIN, id="int64-min"),
]


@pytest.mark.parametrize(("encoded", "value"), VARINT_VECTORS)
def test_reference_encoder_matches_known_vectors(encoded, value):
    assert zigzag_varint(value) == encoded


# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_header_names(self):
        assert KEY_SCHEMA_ID_HEADER == "__key_schema_id"
        assert VALUE_SCHEMA_ID_HEADER == "__value_schema_id"

    def test_key_and_value_headers_differ(self):
        assert KEY_SCHEMA_ID_HEADER != VALUE_SCHEMA_ID_HEADER

    def test_frame_format_members_are_distinct(self):
        assert {f.name for f in FrameFormat} == {"UNFRAMED", "CLASSIC", "GUID"}
        assert len({f.value for f in FrameFormat}) == 3


# ---------------------------------------------------------------------------
# Frame dataclasses
# ---------------------------------------------------------------------------


class TestFrameDataclasses:
    def test_classic_defaults(self):
        frame = ClassicFrame(schema_id=7)
        assert frame.message_indexes == ()
        assert frame.payload_offset == 5

    def test_guid_defaults(self):
        frame = GuidFrame(schema_guid=GUID)
        assert frame.message_indexes == ()
        assert frame.payload_offset == 0

    def test_unframed_defaults(self):
        assert UnframedFrame().payload_offset == 0

    @pytest.mark.parametrize(
        ("frame", "attr", "new"),
        [
            pytest.param(ClassicFrame(1), "schema_id", 2, id="classic"),
            pytest.param(GuidFrame(GUID), "schema_guid", OTHER_GUID, id="guid"),
            pytest.param(UnframedFrame(), "payload_offset", 9, id="unframed"),
        ],
    )
    def test_frames_are_immutable(self, frame, attr, new):
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(frame, attr, new)

    @pytest.mark.parametrize(
        "frame",
        [ClassicFrame(1, (0,), 6), GuidFrame(GUID, (0,)), UnframedFrame()],
        ids=["classic", "guid", "unframed"],
    )
    def test_frames_are_hashable_and_value_equal(self, frame):
        clone = dataclasses.replace(frame)
        assert clone == frame
        assert hash(clone) == hash(frame)
        assert len({frame, clone}) == 1

    def test_frames_use_slots(self):
        # slots=True: no per-instance __dict__ (cheap on the hot path).
        for frame in (ClassicFrame(1), GuidFrame(GUID), UnframedFrame()):
            assert not hasattr(frame, "__dict__")

    def test_different_frame_types_never_compare_equal(self):
        assert ClassicFrame(1) != GuidFrame(GUID)
        assert UnframedFrame() != ClassicFrame(1, (), 0)

    @pytest.mark.parametrize(
        "frame", [ClassicFrame(1), GuidFrame(GUID), UnframedFrame()]
    )
    def test_frame_union_accepts_every_variant(self, frame):
        assert isinstance(frame, Frame)

    def test_frame_union_rejects_other_types(self):
        assert not isinstance(object(), Frame)


# ---------------------------------------------------------------------------
# _read_zigzag_varint
# ---------------------------------------------------------------------------


class TestReadZigzagVarint:
    @pytest.mark.parametrize(("encoded", "value"), VARINT_VECTORS)
    def test_decodes_known_vectors(self, encoded, value):
        assert _read_zigzag_varint(encoded, 0) == (value, len(encoded))

    def test_roundtrips_random_int64_values(self):
        rng = random.Random(0xC0FFEE)  # seeded: deterministic across runs
        values = [rng.randint(INT64_MIN, INT64_MAX) for _ in range(500)]
        # Also exercise every encoded length (1..10 bytes) around power-of-two edges.
        for bits in range(0, 63, 7):
            values += [(1 << bits) - 1, 1 << bits, -(1 << bits), -(1 << bits) - 1]
        for value in values:
            encoded = zigzag_varint(value)
            assert _read_zigzag_varint(encoded, 0) == (value, len(encoded))

    def test_honours_start_position(self):
        buf = b"\xaa\xbb" + zigzag_varint(300) + b"\xcc"
        value, pos = _read_zigzag_varint(buf, 2)
        assert value == 300
        assert pos == 2 + len(zigzag_varint(300))

    def test_stops_at_first_terminating_byte(self):
        # Trailing bytes (even continuation bytes) belong to the next value.
        buf = b"\x02" + b"\xff\xff"
        assert _read_zigzag_varint(buf, 0) == (1, 1)

    def test_consecutive_reads_walk_the_buffer(self):
        buf = zigzag_varint(5) + zigzag_varint(-300) + zigzag_varint(0)
        pos, got = 0, []
        while pos < len(buf):
            value, pos = _read_zigzag_varint(buf, pos)
            got.append(value)
        assert got == [5, -300, 0]
        assert pos == len(buf)

    def test_accepts_exactly_ten_bytes(self):
        encoded = zigzag_varint(INT64_MIN)
        assert len(encoded) == 10
        assert _read_zigzag_varint(encoded, 0) == (INT64_MIN, 10)

    @pytest.mark.parametrize(
        ("buf", "pos"),
        [
            pytest.param(b"", 0, id="empty-buffer"),
            pytest.param(b"\x02", 1, id="pos-at-end"),
            pytest.param(b"\x02", 99, id="pos-past-end"),
            pytest.param(b"\x80", 0, id="lone-continuation-byte"),
            pytest.param(b"\xff\xff\xff", 0, id="continuation-bytes-only"),
            pytest.param(b"\x00\x80", 1, id="truncated-after-offset"),
        ],
    )
    def test_truncated_input_raises(self, buf, pos):
        with pytest.raises(ValueError, match="truncated varint"):
            _read_zigzag_varint(buf, pos)

    def test_ten_continuation_bytes_is_too_long(self):
        with pytest.raises(ValueError, match="longer than 10 bytes"):
            _read_zigzag_varint(b"\x80" * 10, 0)

    def test_eleven_byte_varint_is_too_long(self):
        buf = b"\x80" * 10 + b"\x01"
        with pytest.raises(ValueError, match="longer than 10 bytes"):
            _read_zigzag_varint(buf, 0)

    def test_too_long_is_reported_even_with_a_terminator_available(self):
        # The cap is on varint width, not buffer length.
        buf = b"\xff" * 10 + b"\x00" * 5
        with pytest.raises(ValueError, match="longer than 10 bytes"):
            _read_zigzag_varint(buf, 0)

    def test_input_is_not_modified_and_accepts_bytearray_and_memoryview(self):
        raw = zigzag_varint(-12345)
        for buf in (raw, bytearray(raw), memoryview(raw)):
            assert _read_zigzag_varint(buf, 0) == (-12345, len(raw))


# ---------------------------------------------------------------------------
# _read_message_indexes
# ---------------------------------------------------------------------------


class TestReadMessageIndexes:
    def test_zero_count_is_the_first_message_shortcut(self):
        # Single 0x00 byte means [0]; it consumes exactly one byte.
        assert _read_message_indexes(b"\x00", 0) == ((0,), 1)

    def test_single_index(self):
        buf = message_indexes(3)
        assert _read_message_indexes(buf, 0) == ((3,), len(buf))

    def test_explicit_zero_index_is_not_the_shortcut(self):
        # [0] spelled out as count=1, idx=0 decodes to the same thing.
        buf = zigzag_varint(1) + zigzag_varint(0)
        assert _read_message_indexes(buf, 0) == ((0,), 2)

    def test_multiple_indexes_preserve_order(self):
        buf = message_indexes(2, 0, 5, 1)
        assert _read_message_indexes(buf, 0) == ((2, 0, 5, 1), len(buf))

    def test_returns_tuple(self):
        indexes, _ = _read_message_indexes(message_indexes(1, 2), 0)
        assert isinstance(indexes, tuple)

    def test_multi_byte_indexes(self):
        buf = message_indexes(64, 10_000, 2**31 - 1)
        assert _read_message_indexes(buf, 0) == ((64, 10_000, 2**31 - 1), len(buf))

    def test_large_count(self):
        indexes = tuple(range(200))  # count itself needs a 2-byte varint
        buf = message_indexes(*indexes)
        assert _read_message_indexes(buf, 0) == (indexes, len(buf))

    def test_honours_start_position_and_leaves_payload_alone(self):
        prefix, tail = b"\x00\x00\x00\x00\x07", b"payload"
        buf = prefix + message_indexes(1, 4) + tail
        indexes, pos = _read_message_indexes(buf, len(prefix))
        assert indexes == (1, 4)
        assert buf[pos:] == tail

    def test_negative_count_raises(self):
        with pytest.raises(ValueError, match="negative index count"):
            _read_message_indexes(zigzag_varint(-1), 0)

    def test_empty_buffer_raises(self):
        with pytest.raises(ValueError, match="truncated varint"):
            _read_message_indexes(b"", 0)

    def test_missing_indexes_raise(self):
        buf = zigzag_varint(3) + zigzag_varint(1)  # promises 3, delivers 1
        with pytest.raises(ValueError, match="truncated varint"):
            _read_message_indexes(buf, 0)

    def test_absurd_count_fails_fast_instead_of_allocating(self):
        buf = zigzag_varint(2**40) + b"\x02"
        with pytest.raises(ValueError, match="truncated varint"):
            _read_message_indexes(buf, 0)

    def test_malformed_index_varint_raises(self):
        buf = zigzag_varint(1) + b"\x80" * 10
        with pytest.raises(ValueError, match="longer than 10 bytes"):
            _read_message_indexes(buf, 0)


# ---------------------------------------------------------------------------
# ConfluentFrameParser: classic (in-payload prefix)
# ---------------------------------------------------------------------------


class TestParseClassic:
    def test_basic_frame_with_first_message_shortcut(self):
        value = classic_value(42)
        frame = parse(value)
        assert frame == ClassicFrame(
            schema_id=42, message_indexes=(0,), payload_offset=6
        )
        assert value[frame.payload_offset :] == PAYLOAD

    def test_returns_classic_frame_type(self):
        assert isinstance(parse(classic_value(1)), ClassicFrame)

    @pytest.mark.parametrize(
        "schema_id",
        [0, 1, 255, 256, 0x01020304, 2**31 - 1, 2**32 - 1],
        ids=lambda n: hex(n),
    )
    def test_schema_id_is_big_endian_uint32(self, schema_id):
        assert parse(classic_value(schema_id)).schema_id == schema_id

    def test_schema_id_byte_order(self):
        value = b"\x00\x00\x00\x01\x00" + b"\x00" + PAYLOAD
        assert parse(value).schema_id == 256  # big-endian, not 0x00010000

    @pytest.mark.parametrize(
        "indexes",
        [(1,), (2,), (0, 1), (1, 0), (3, 1, 4, 1, 5), (64,), (0, 0, 0)],
        ids=str,
    )
    def test_message_indexes_and_payload_offset(self, indexes):
        value = classic_value(9, indexes)
        frame = parse(value)
        assert frame.message_indexes == indexes
        assert frame.payload_offset == 5 + len(message_indexes(*indexes))
        assert value[frame.payload_offset :] == PAYLOAD

    def test_empty_payload(self):
        value = classic_value(5, (0,), payload=b"")
        frame = parse(value)
        assert frame.payload_offset == len(value)
        assert value[frame.payload_offset :] == b""

    def test_payload_beginning_with_zero_byte_is_not_reinterpreted(self):
        payload = b"\x00\x00\x00\x00\x00"
        value = classic_value(5, (0,), payload=payload)
        frame = parse(value)
        assert frame.payload_offset == 6
        assert value[frame.payload_offset :] == payload

    def test_accepts_none_or_empty_headers(self):
        value = classic_value(11)
        assert parse(value, headers=None) == parse(value, headers=[])

    def test_unrelated_headers_are_ignored(self):
        headers = [("trace-id", b"abc"), (KEY_SCHEMA_ID_HEADER, guid_header(GUID))]
        # Parser is looking for the *value* header, so the key header is noise.
        assert parse(classic_value(8), headers, HEADER).schema_id == 8

    def test_empty_target_header_falls_back_to_classic(self):
        assert parse(classic_value(8), [(HEADER, b"")]).schema_id == 8

    def test_magic_byte_with_nothing_after_schema_id_is_truncated(self):
        # Exactly 5 bytes: magic + id, but no message-index varint.
        with pytest.raises(ValueError, match="truncated varint"):
            parse(b"\x00\x00\x00\x00\x2a")

    def test_truncated_message_indexes(self):
        value = b"\x00\x00\x00\x00\x2a" + zigzag_varint(3) + zigzag_varint(1)
        with pytest.raises(ValueError, match="truncated varint"):
            parse(value)

    def test_negative_index_count(self):
        value = b"\x00\x00\x00\x00\x2a" + zigzag_varint(-2)
        with pytest.raises(ValueError, match="negative index count"):
            parse(value)


# ---------------------------------------------------------------------------
# ConfluentFrameParser: GUID (in-header)
# ---------------------------------------------------------------------------


class TestParseGuid:
    def test_basic_frame(self):
        frame = parse(PAYLOAD, [(HEADER, guid_header(GUID))])
        assert frame == GuidFrame(
            schema_guid=GUID, message_indexes=(0,), payload_offset=0
        )

    def test_returns_guid_frame_type(self):
        assert isinstance(parse(PAYLOAD, [(HEADER, guid_header(GUID))]), GuidFrame)

    def test_payload_is_the_whole_value(self):
        frame = parse(PAYLOAD, [(HEADER, guid_header(GUID))])
        assert PAYLOAD[frame.payload_offset :] == PAYLOAD

    @pytest.mark.parametrize(
        "indexes", [(0,), (1,), (0, 1), (2, 0, 7), (300,)], ids=str
    )
    def test_message_indexes(self, indexes):
        frame = parse(PAYLOAD, [(HEADER, guid_header(GUID, indexes))])
        assert frame.message_indexes == indexes

    @pytest.mark.parametrize(
        "guid", [GUID, OTHER_GUID, UUID(int=0), UUID(int=2**128 - 1)]
    )
    def test_guid_roundtrip(self, guid):
        assert parse(PAYLOAD, [(HEADER, guid_header(guid))]).schema_guid == guid

    def test_guid_uses_rfc4122_byte_order(self):
        raw = b"\x01" + bytes(range(16)) + b"\x00"
        guid = parse(PAYLOAD, [(HEADER, raw)]).schema_guid
        assert guid.bytes == bytes(range(16))

    def test_header_wins_over_classic_looking_value(self):
        # A value that starts with the classic magic byte is still raw payload
        # when the GUID header is present.
        value = classic_value(42)
        frame = parse(value, [(HEADER, guid_header(GUID))])
        assert isinstance(frame, GuidFrame)
        assert frame.payload_offset == 0

    def test_empty_value_with_guid_header(self):
        assert isinstance(parse(b"", [(HEADER, guid_header(GUID))]), GuidFrame)
        assert isinstance(parse(None, [(HEADER, guid_header(GUID))]), GuidFrame)

    def test_header_lookup_is_by_exact_name(self):
        headers = [("__value_schema_id_x", guid_header(GUID))]
        assert parse(PAYLOAD, headers) == UnframedFrame()

    def test_key_header_name_is_honoured_when_configured(self):
        headers = [
            (VALUE_SCHEMA_ID_HEADER, guid_header(OTHER_GUID)),
            (KEY_SCHEMA_ID_HEADER, guid_header(GUID)),
        ]
        key_frame = parse(b"k", headers, KEY_SCHEMA_ID_HEADER)
        value_frame = parse(b"v", headers, VALUE_SCHEMA_ID_HEADER)
        assert key_frame.schema_guid == GUID
        assert value_frame.schema_guid == OTHER_GUID

    def test_newest_duplicate_header_wins(self):
        # Kafka appends headers, so the last occurrence is the newest.
        headers = [
            (HEADER, guid_header(OTHER_GUID)),
            ("other", b"x"),
            (HEADER, guid_header(GUID)),
        ]
        assert parse(PAYLOAD, headers).schema_guid == GUID

    def test_extra_trailing_bytes_in_header_are_ignored(self):
        raw = guid_header(GUID, (1,)) + b"junk"
        frame = parse(PAYLOAD, [(HEADER, raw)])
        assert frame.message_indexes == (1,)
        assert frame.payload_offset == 0

    @pytest.mark.parametrize("version", [0, 2, 255])
    def test_unsupported_version_byte(self, version):
        raw = guid_header(GUID, version=version)
        with pytest.raises(ValueError, match="malformed guid header"):
            parse(PAYLOAD, [(HEADER, raw)])

    @pytest.mark.parametrize(
        "raw",
        [
            pytest.param(b"\x01", id="version-only"),
            pytest.param(b"\x01" + GUID.bytes[:-1], id="guid-one-byte-short"),
        ],
    )
    def test_short_header_is_malformed(self, raw):
        with pytest.raises(ValueError, match="malformed guid header"):
            parse(PAYLOAD, [(HEADER, raw)])

    def test_header_without_message_indexes_is_truncated(self):
        raw = b"\x01" + GUID.bytes  # exactly 17 bytes, indexes missing
        with pytest.raises(ValueError, match="truncated varint"):
            parse(PAYLOAD, [(HEADER, raw)])

    def test_negative_index_count(self):
        raw = b"\x01" + GUID.bytes + zigzag_varint(-1)
        with pytest.raises(ValueError, match="negative index count"):
            parse(PAYLOAD, [(HEADER, raw)])

    def test_find_header_value_without_headers_returns_none(self):
        for headers in (None, []):
            parser = ConfluentFrameParser(PAYLOAD, headers, HEADER)
            assert parser._find_header_value() is None

    def test_parse_guid_with_missing_header_is_malformed(self):
        # Defensive: parse() never routes here without a header, but a direct
        # call must fail cleanly rather than with a TypeError on None.
        parser = ConfluentFrameParser(PAYLOAD, [("other", b"x")], HEADER)
        with pytest.raises(ValueError, match="malformed guid header"):
            parser._parse_guid()


# ---------------------------------------------------------------------------
# ConfluentFrameParser: unframed + format detection
# ---------------------------------------------------------------------------


class TestParseUnframed:
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(b"", id="empty-bytes"),
            pytest.param(None, id="none"),
            pytest.param(b"\x08\x96\x01", id="raw-protobuf"),
            pytest.param(b"\x01\x00\x00\x00\x2a\x00", id="nonzero-first-byte"),
            pytest.param(b"\x00", id="magic-byte-only"),
            pytest.param(b"\x00\x00\x00\x00", id="magic-plus-3-bytes-too-short"),
        ],
    )
    def test_value_without_a_frame(self, value):
        assert parse(value) == UnframedFrame(payload_offset=0)

    def test_returns_unframed_frame_type(self):
        assert isinstance(parse(b"\x08\x01"), UnframedFrame)

    def test_four_bytes_is_below_the_classic_minimum(self):
        assert parse(b"\x00" * 4) == UnframedFrame()

    def test_five_zero_bytes_is_classic_with_shortcut_index_missing(self):
        # Boundary: len == 5 crosses into the classic path (and then needs an
        # index varint that isn't there).
        with pytest.raises(ValueError, match="truncated varint"):
            parse(b"\x00" * 5)

    def test_six_zero_bytes_is_classic_schema_id_zero(self):
        assert parse(b"\x00" * 6) == ClassicFrame(0, (0,), 6)

    @pytest.mark.parametrize("headers", [None, [], [("a", b"1")], [(HEADER, b"")]])
    def test_irrelevant_or_empty_headers_do_not_trigger_guid(self, headers):
        assert parse(b"\x08\x01", headers) == UnframedFrame()

    def test_unframed_payload_is_the_whole_value(self):
        value = b"\x08\x96\x01"
        assert value[parse(value).payload_offset :] == value


class TestFindFormat:
    @pytest.mark.parametrize(
        ("value", "headers", "expected"),
        [
            pytest.param(b"", None, FrameFormat.UNFRAMED, id="nothing"),
            pytest.param(
                b"\x00\x00\x00\x00\x01", None, FrameFormat.CLASSIC, id="classic"
            ),
            pytest.param(
                b"\x01\x00\x00\x00\x01", None, FrameFormat.UNFRAMED, id="bad-magic"
            ),
            pytest.param(b"x", [(HEADER, b"\x01")], FrameFormat.GUID, id="guid"),
            pytest.param(
                b"\x00\x00\x00\x00\x01",
                [(HEADER, b"\x01")],
                FrameFormat.GUID,
                id="guid-beats-classic",
            ),
            pytest.param(
                b"\x00\x00\x00\x00\x01",
                [(HEADER, b"")],
                FrameFormat.CLASSIC,
                id="empty-header-ignored",
            ),
        ],
    )
    def test_detects_format(self, value, headers, expected):
        parser = ConfluentFrameParser(value, headers, HEADER)
        assert parser._find_format() is expected

    def test_unknown_format_raises(self, monkeypatch):
        # `parse` has a defensive catch-all for future FrameFormat members.
        parser = ConfluentFrameParser(b"", [], HEADER)
        monkeypatch.setattr(parser, "_find_format", lambda: "not-a-format")
        with pytest.raises(ValueError, match="Unknown frame"):
            parser.parse()


class TestParserState:
    def test_constructor_stores_arguments(self):
        headers = [(HEADER, b"x")]
        parser = ConfluentFrameParser(b"v", headers, HEADER)
        assert (parser.value, parser.headers, parser.header_name) == (
            b"v",
            headers,
            HEADER,
        )

    def test_parse_is_repeatable(self):
        parser = ConfluentFrameParser(classic_value(3, (1, 2)), [], HEADER)
        assert parser.parse() == parser.parse()

    def test_parse_does_not_mutate_headers(self):
        headers = [(HEADER, guid_header(GUID)), ("a", b"1")]
        snapshot = list(headers)
        parse(PAYLOAD, headers)
        assert headers == snapshot


def test_module_exposes_no_unexpected_public_frame_types():
    # Guards against a new Frame variant being added without parser/test support.
    variants = set(Frame.__args__)
    assert variants == {ClassicFrame, GuidFrame, UnframedFrame}
    assert all(dataclasses.is_dataclass(v) for v in variants)
    assert wire.Frame is Frame
