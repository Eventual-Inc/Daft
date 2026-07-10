from __future__ import annotations


def _varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("test fixture varints must be non-negative")

    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _length_delimited(field_number: int, value: bytes) -> bytes:
    return _varint((field_number << 3) | 2) + _varint(len(value)) + value


def foxglove_compressed_video_payload(
    *,
    timestamp_seconds: int,
    timestamp_nanos: int,
    frame_id: str,
    data: bytes,
    format: str,
) -> bytes:
    """Encode the fields used by ``foxglove.CompressedVideo`` without generated protos."""
    if not 0 <= timestamp_nanos < 1_000_000_000:
        raise ValueError("timestamp_nanos must be in [0, 1_000_000_000)")

    timestamp = _varint(1 << 3) + _varint(timestamp_seconds)
    timestamp += _varint(2 << 3) + _varint(timestamp_nanos)

    return b"".join(
        [
            _length_delimited(1, timestamp),
            _length_delimited(2, frame_id.encode()),
            _length_delimited(3, data),
            _length_delimited(4, format.encode()),
        ]
    )
