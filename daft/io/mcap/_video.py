"""Stateful Foxglove ``CompressedVideo`` decoding for the MCAP source."""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from typing import TYPE_CHECKING, Any

from daft.daft import ImageMode
from daft.datatype import DataType
from daft.dependencies import av
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch
from daft.series import Series

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass(frozen=True)
class _CompressedVideo:
    timestamp_ns: int
    frame_id: str
    data: bytes
    format: str


@dataclass(frozen=True)
class _VideoMessage:
    source_path: str
    topic: str
    log_time: int
    publish_time: int
    sequence: int
    timestamp_ns: int
    frame_id: str
    format: str
    data: bytes


@dataclass(frozen=True)
class _DecodedVideoFrame:
    message: _VideoMessage
    frame_index: int
    is_key_frame: bool
    data: Any | None
    size_bytes: int


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(data) and shift < 70:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, offset
        shift += 7
    raise ValueError("Malformed Foxglove CompressedVideo protobuf varint")


def _read_length_delimited(data: bytes, offset: int) -> tuple[bytes, int]:
    length, offset = _read_varint(data, offset)
    end = offset + length
    if end > len(data):
        raise ValueError("Truncated Foxglove CompressedVideo protobuf field")
    return data[offset:end], end


def _skip_protobuf_field(data: bytes, offset: int, wire_type: int) -> int:
    if wire_type == 0:
        _, offset = _read_varint(data, offset)
        return offset
    if wire_type == 1:
        end = offset + 8
    elif wire_type == 2:
        _, end = _read_length_delimited(data, offset)
        return end
    elif wire_type == 5:
        end = offset + 4
    else:
        raise ValueError(f"Unsupported protobuf wire type {wire_type} in Foxglove CompressedVideo")
    if end > len(data):
        raise ValueError("Truncated Foxglove CompressedVideo protobuf field")
    return end


def _parse_timestamp(data: bytes) -> int:
    seconds = 0
    nanos = 0
    offset = 0
    while offset < len(data):
        tag, offset = _read_varint(data, offset)
        field_number, wire_type = tag >> 3, tag & 0x07
        if field_number == 1 and wire_type == 0:
            seconds, offset = _read_varint(data, offset)
            if seconds >= 1 << 63:
                seconds -= 1 << 64
        elif field_number == 2 and wire_type == 0:
            nanos, offset = _read_varint(data, offset)
            if nanos >= 1 << 31:
                nanos -= 1 << 32
        else:
            offset = _skip_protobuf_field(data, offset, wire_type)
    if not 0 <= nanos < 1_000_000_000:
        raise ValueError(f"Invalid Foxglove CompressedVideo timestamp nanos: {nanos}")
    timestamp_ns = seconds * 1_000_000_000 + nanos
    if not -(1 << 63) <= timestamp_ns < 1 << 63:
        raise ValueError("Foxglove CompressedVideo timestamp does not fit in int64 nanoseconds")
    return timestamp_ns


def _parse_foxglove_compressed_video(data: bytes) -> _CompressedVideo:
    """Parse the stable four-field Foxglove ``CompressedVideo`` protobuf schema.

    Keeping this adapter schema-specific avoids importing generated protobuf
    modules or materializing a dynamic message for every video frame.
    Unknown protobuf fields are skipped for forwards compatibility.
    """
    timestamp_ns = 0
    frame_id = ""
    encoded = b""
    format = ""
    offset = 0
    while offset < len(data):
        tag, offset = _read_varint(data, offset)
        field_number, wire_type = tag >> 3, tag & 0x07
        if tag == 0:
            raise ValueError("Invalid zero protobuf tag in Foxglove CompressedVideo")
        if field_number in {1, 2, 3, 4} and wire_type != 2:
            raise ValueError(f"Invalid wire type for Foxglove CompressedVideo field {field_number}")
        if field_number == 1:
            value, offset = _read_length_delimited(data, offset)
            timestamp_ns = _parse_timestamp(value)
        elif field_number == 2:
            value, offset = _read_length_delimited(data, offset)
            try:
                frame_id = value.decode("utf-8")
            except UnicodeDecodeError as error:
                raise ValueError("Foxglove CompressedVideo frame_id is not UTF-8") from error
        elif field_number == 3:
            encoded, offset = _read_length_delimited(data, offset)
        elif field_number == 4:
            value, offset = _read_length_delimited(data, offset)
            try:
                format = value.decode("utf-8")
            except UnicodeDecodeError as error:
                raise ValueError("Foxglove CompressedVideo format is not UTF-8") from error
        else:
            offset = _skip_protobuf_field(data, offset, wire_type)
    if not encoded:
        raise ValueError("Foxglove CompressedVideo has no encoded frame data")
    if not format:
        raise ValueError("Foxglove CompressedVideo has no format")
    return _CompressedVideo(timestamp_ns=timestamp_ns, frame_id=frame_id, data=encoded, format=format)


def _av_codec_name(format: str) -> str:
    normalized = format.strip().lower()
    if normalized == "h264":
        return "h264"
    if normalized == "h265":
        return "hevc"
    raise ValueError(f"Unsupported Foxglove CompressedVideo format {format!r}; expected one of: 'h264', 'h265'")


def _annex_b_nal_types(data: bytes, format: str) -> Iterator[int]:
    """Yield H.264/H.265 NAL unit types from an Annex-B byte stream."""
    offset = 0
    while offset + 3 < len(data):
        if data[offset : offset + 4] == b"\x00\x00\x00\x01":
            header = offset + 4
        elif data[offset : offset + 3] == b"\x00\x00\x01":
            header = offset + 3
        else:
            offset += 1
            continue
        if header < len(data):
            if format == "h264":
                yield data[header] & 0x1F
            elif format == "h265":
                yield (data[header] >> 1) & 0x3F
        offset = header + 1


def _is_video_keyframe(video: _CompressedVideo) -> bool:
    format = video.format.strip().lower()
    nal_types = set(_annex_b_nal_types(video.data, format))
    if format == "h264":
        return {5, 7, 8}.issubset(nal_types)
    if format == "h265":
        return {32, 33, 34}.issubset(nal_types) and any(16 <= nal_type <= 23 for nal_type in nal_types)
    _av_codec_name(video.format)
    raise AssertionError("unreachable")


def _find_previous_video_keyframe(
    file: Any,
    topic: str,
    requested_start_time: int,
) -> int | None:
    """Use indexed reverse reads to locate the nearest decodable keyframe."""
    from daft.daft import PyMcapReader

    end_time = None if requested_start_time == (1 << 64) - 1 else requested_start_time + 1
    reader = PyMcapReader(
        file,
        ["log_time", "data"],
        # Only the nearest message must cross the Python boundary. MCAP chunk
        # decompression remains the physical lower bound.
        batch_size=1,
        end_time=end_time,
        topics=[topic],
        limit=None,
        reverse=True,
    )
    if not reader.indexed:
        return None
    successor_prefix = b""
    format: str | None = None
    found_random_access = False
    parameter_sets: set[int] = set()
    while (batch := reader.next_batch()) is not None:
        values = RecordBatch._from_pyrecordbatch(batch).to_pydict()
        for log_time, payload in zip(values["log_time"], values["data"]):
            video = _parse_foxglove_compressed_video(bytes(payload))
            if format is None:
                format = video.format
            elif _av_codec_name(video.format) != _av_codec_name(format):
                raise ValueError(f"Foxglove CompressedVideo format changed while scanning topic {topic!r}")
            if _is_video_keyframe(video):
                return log_time
            normalized = video.format.strip().lower()
            # At most a start code and NAL header can straddle this boundary.
            # Keeping a tiny chronological-successor prefix makes this scan
            # linear rather than repeatedly copying/rescanning a whole GOP.
            nal_types = set(_annex_b_nal_types(video.data + successor_prefix, normalized))
            if not found_random_access:
                found_random_access = (
                    5 in nal_types if normalized == "h264" else any(16 <= value <= 23 for value in nal_types)
                )
                if found_random_access:
                    parameter_sets = nal_types & ({7, 8} if normalized == "h264" else {32, 33, 34})
            else:
                parameter_sets.update(nal_types & ({7, 8} if normalized == "h264" else {32, 33, 34}))

            required = {7, 8} if normalized == "h264" else {32, 33, 34}
            if found_random_access and required.issubset(parameter_sets):
                return log_time
            successor_prefix = (video.data + successor_prefix)[:8]
    return None


def mcap_video_schema(image_height: int | None, image_width: int | None) -> Schema:
    return Schema.from_field_name_and_types(
        [
            ("source_path", DataType.string()),
            ("topic", DataType.string()),
            ("log_time", DataType.uint64()),
            ("publish_time", DataType.uint64()),
            ("sequence", DataType.uint32()),
            ("frame_id", DataType.string()),
            ("format", DataType.string()),
            ("frame_index", DataType.int64()),
            ("frame_time", DataType.float64()),
            ("timestamp_ns", DataType.int64()),
            ("is_key_frame", DataType.bool()),
            (
                "data",
                DataType.image(mode=ImageMode.RGB, height=image_height, width=image_width),
            ),
        ]
    )


class _TopicVideoDecoder:
    """A persistent codec context for one ordered MCAP topic stream."""

    def __init__(self, format: str, include_data: bool, image_height: int | None, image_width: int | None):
        self.format = format
        self.codec_name = _av_codec_name(format)
        self.context: Any = av.CodecContext.create(self.codec_name, "r")
        self.include_data = include_data
        self.image_height = image_height
        self.image_width = image_width
        self.next_packet_id = 0
        self.next_frame_index = 0
        self.pending: dict[int, _VideoMessage] = {}
        self.buffered_source: _VideoMessage | None = None
        self.last_message: _VideoMessage | None = None

    def _finish_frames(self, frames: list[Any]) -> Iterator[_DecodedVideoFrame]:
        for frame in frames:
            packet_id = frame.pts
            message = self.pending.pop(packet_id, None) if packet_id is not None else None
            if message is None and self.pending:
                # A codec is allowed to omit/rewrite PTS. Foxglove forbids B-frames,
                # so the oldest pending source message is the deterministic fallback.
                oldest = min(self.pending)
                message = self.pending.pop(oldest)
            if message is None:
                raise ValueError(f"{self.format} decoder emitted a frame without an MCAP source message")

            pixels = None
            if self.include_data:
                output = frame
                if self.image_height is not None and self.image_width is not None:
                    output = frame.reformat(width=self.image_width, height=self.image_height)
                pixels = output.to_ndarray(format="rgb24")

            yield _DecodedVideoFrame(
                message=message,
                frame_index=self.next_frame_index,
                is_key_frame=bool(frame.key_frame),
                data=pixels,
                size_bytes=256 if pixels is None else int(pixels.nbytes) + 256,
            )
            self.next_frame_index += 1

    def _decode_packet(self, packet: Any, message: _VideoMessage) -> Iterator[_DecodedVideoFrame]:
        packet_id = self.next_packet_id
        self.next_packet_id += 1
        packet.pts = packet_id
        packet.dts = packet_id
        packet.time_base = Fraction(1, 1_000_000_000)
        self.pending[packet_id] = message
        yield from self._finish_frames(self.context.decode(packet))

    def decode(self, message: _VideoMessage) -> Iterator[_DecodedVideoFrame]:
        if _av_codec_name(message.format) != self.codec_name:
            raise ValueError(
                f"Foxglove CompressedVideo format changed on topic {message.topic!r}: "
                f"{self.format!r} to {message.format!r}"
            )
        self.last_message = message
        if self.buffered_source is None:
            self.buffered_source = message
        for packet in self.context.parse(message.data):
            # PyAV's parser may retain a suffix of the current MCAP chunk for the
            # next access unit. Attribute this packet to the earliest chunk that
            # has been buffered for it, then make the current chunk the next
            # candidate. This is exact for conforming one-image messages and a
            # deterministic best effort for ABC's fragmented byte streams.
            source = self.buffered_source
            self.buffered_source = message
            yield from self._decode_packet(packet, source)

    def flush(self) -> Iterator[_DecodedVideoFrame]:
        for packet in self.context.parse(b""):
            source = self.buffered_source or self.last_message
            if source is None:
                raise ValueError(f"{self.format} parser emitted a packet without an MCAP source message")
            yield from self._decode_packet(packet, source)
        yield from self._finish_frames(self.context.decode(None))
        self.pending.clear()
        self.buffered_source = None


class McapVideoDecoder:
    """Decode native MCAP message batches while retaining per-topic codec state."""

    def __init__(
        self,
        output_schema: Schema,
        columns: list[str],
        emit_start_time: int | None,
        image_height: int | None,
        image_width: int | None,
    ) -> None:
        if not getattr(av, "module_available", lambda: True)():
            raise ImportError("MCAP video decoding requires PyAV. Install with `pip install daft[video]`.")
        self.output_schema = output_schema
        self.columns = columns
        self.emit_start_time = emit_start_time
        self.image_height = image_height
        self.image_width = image_width
        self.include_data = "data" in columns
        self.decoders: dict[str, _TopicVideoDecoder] = {}

    @staticmethod
    def _messages(batch: RecordBatch) -> Iterator[_VideoMessage]:
        values = batch.to_pydict()
        for source_path, topic, log_time, publish_time, sequence, payload in zip(
            values["source_path"],
            values["topic"],
            values["log_time"],
            values["publish_time"],
            values["sequence"],
            values["data"],
        ):
            video = _parse_foxglove_compressed_video(bytes(payload))
            yield _VideoMessage(
                source_path=source_path,
                topic=topic,
                log_time=log_time,
                publish_time=publish_time,
                sequence=sequence,
                timestamp_ns=video.timestamp_ns,
                frame_id=video.frame_id,
                format=video.format,
                data=video.data,
            )

    def decode_batch(self, batch: RecordBatch) -> Iterator[_DecodedVideoFrame]:
        for message in self._messages(batch):
            decoder = self.decoders.get(message.topic)
            if decoder is None:
                decoder = _TopicVideoDecoder(
                    message.format,
                    self.include_data,
                    self.image_height,
                    self.image_width,
                )
                self.decoders[message.topic] = decoder
            for frame in decoder.decode(message):
                if self.emit_start_time is None or frame.message.log_time >= self.emit_start_time:
                    yield frame

    def flush(self) -> Iterator[_DecodedVideoFrame]:
        for decoder in self.decoders.values():
            for frame in decoder.flush():
                if self.emit_start_time is None or frame.message.log_time >= self.emit_start_time:
                    yield frame

    def to_record_batch(self, frames: list[_DecodedVideoFrame]) -> RecordBatch:
        values: dict[str, list[Any]] = {column: [] for column in self.columns}
        for frame in frames:
            message = frame.message
            row = {
                "source_path": message.source_path,
                "topic": message.topic,
                "log_time": message.log_time,
                "publish_time": message.publish_time,
                "sequence": message.sequence,
                "frame_id": message.frame_id,
                "format": message.format,
                "frame_index": frame.frame_index,
                "frame_time": message.timestamp_ns / 1_000_000_000,
                "timestamp_ns": message.timestamp_ns,
                "is_key_frame": frame.is_key_frame,
                "data": frame.data,
            }
            for column in self.columns:
                values[column].append(row[column])

        series = [
            Series.from_pylist(values[column], name=column, dtype=self.output_schema[column].dtype)
            for column in self.columns
        ]
        return RecordBatch._from_series(series, num_rows=len(frames))
