from __future__ import annotations

from fractions import Fraction

import pytest

import daft
from daft.io.mcap._video import (
    _av_codec_name,
    _find_previous_video_keyframe,
    _parse_foxglove_compressed_video,
)
from tests.io.mcap._foxglove_video_fixtures import foxglove_compressed_video_payload


@pytest.mark.parametrize(
    ("format", "data"),
    [
        pytest.param("h264", b"\x00\x00\x00\x01\x65", id="h264"),
        pytest.param("h265", b"\x00\x00\x00\x01\x26\x01", id="h265-hevc"),
    ],
)
def test_parse_foxglove_compressed_video(format: str, data: bytes) -> None:
    payload = foxglove_compressed_video_payload(
        timestamp_seconds=1_725_000_000,
        timestamp_nanos=123_456_789,
        frame_id="top-camera",
        data=data,
        format=format,
    )

    video = _parse_foxglove_compressed_video(payload)

    assert video.timestamp_ns == 1_725_000_000_123_456_789
    assert video.frame_id == "top-camera"
    assert video.data == data
    assert video.format == format


def test_foxglove_compressed_video_wire_field_numbers() -> None:
    payload = foxglove_compressed_video_payload(
        timestamp_seconds=1,
        timestamp_nanos=2,
        frame_id="cam",
        data=b"\x00\x00\x00\x01\x65",
        format="h264",
    )

    # timestamp=1, frame_id=2, data=3, format=4. The nested Timestamp uses
    # seconds=1 and nanos=2.
    assert payload.hex() == "0a0408011002120363616d1a050000000165220468323634"


@pytest.mark.parametrize(
    ("format", "expected"),
    [
        pytest.param("h264", "h264", id="h264"),
        pytest.param("h265", "hevc", id="h265-hevc"),
    ],
)
def test_av_codec_name(format: str, expected: str) -> None:
    assert _av_codec_name(format) == expected


def test_av_codec_name_rejects_unsupported_format() -> None:
    with pytest.raises(ValueError, match="Unsupported Foxglove CompressedVideo format"):
        _av_codec_name("vp9")


@pytest.fixture
def foxglove_video_mcap(tmp_path):
    av = pytest.importorskip("av")
    np = pytest.importorskip("numpy")
    mcap_writer = pytest.importorskip("mcap.writer")

    encoder = av.CodecContext.create("libx264", "w")
    encoder.width = 16
    encoder.height = 16
    encoder.pix_fmt = "yuv420p"
    encoder.time_base = Fraction(1, 30)
    encoder.framerate = Fraction(30, 1)
    encoder.options = {
        "preset": "ultrafast",
        "tune": "zerolatency",
        # Foxglove requires no B-frames and parameter sets on every keyframe.
        "x264-params": "keyint=4:min-keyint=4:scenecut=0:repeat-headers=1:aud=1:bframes=0",
    }

    packets = []
    for index in range(8):
        pixels = np.full((16, 16, 3), index * 25, dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(pixels, format="rgb24")
        frame.pts = index
        packets.extend(encoder.encode(frame))
    packets.extend(encoder.encode(None))
    assert len(packets) == 8

    path = tmp_path / "foxglove-video.mcap"
    with path.open("wb") as output:
        writer = mcap_writer.Writer(output, chunk_size=1024)
        writer.start()
        schema_id = writer.register_schema(name="foxglove.CompressedVideo", encoding="protobuf", data=b"")
        channel_id = writer.register_channel(topic="/camera", message_encoding="protobuf", schema_id=schema_id)
        other_channel_id = writer.register_channel(topic="/imu", message_encoding="protobuf", schema_id=schema_id)
        for index, packet in enumerate(packets):
            payload = foxglove_compressed_video_payload(
                timestamp_seconds=1_725_000_000,
                timestamp_nanos=index * 1_000_000,
                frame_id="top-camera",
                data=bytes(packet),
                format="h264",
            )
            writer.add_message(
                channel_id=channel_id,
                log_time=index * 100,
                publish_time=index * 100 + 1,
                sequence=index,
                data=payload,
            )
            writer.add_message(
                channel_id=other_channel_id,
                log_time=index * 100 + 50,
                publish_time=index * 100 + 51,
                sequence=index,
                data=b"not-video",
            )
        writer.finish()
    return path


def test_read_mcap_decodes_stateful_foxglove_video(foxglove_video_mcap) -> None:
    np = pytest.importorskip("numpy")

    result = daft.read_mcap(
        foxglove_video_mcap,
        topics=["/camera"],
        decode_video=True,
        batch_size=2,
    ).to_pydict()

    assert result["source_path"] == [str(foxglove_video_mcap)] * 8
    assert result["topic"] == ["/camera"] * 8
    assert result["log_time"] == [index * 100 for index in range(8)]
    assert result["publish_time"] == [index * 100 + 1 for index in range(8)]
    assert result["sequence"] == list(range(8))
    assert result["frame_id"] == ["top-camera"] * 8
    assert result["format"] == ["h264"] * 8
    assert result["frame_index"] == list(range(8))
    assert result["timestamp_ns"] == [1_725_000_000_000_000_000 + index * 1_000_000 for index in range(8)]
    assert result["is_key_frame"] == [True, False, False, False, True, False, False, False]
    assert [round(np.asarray(image).mean()) for image in result["data"]] == pytest.approx(
        [0, 23, 48, 73, 98, 123, 148, 173], abs=2
    )


def test_read_mcap_decodes_fragmented_abc_style_video(foxglove_video_mcap, tmp_path) -> None:
    np = pytest.importorskip("numpy")
    mcap_writer = pytest.importorskip("mcap.writer")

    envelopes = daft.read_mcap(foxglove_video_mcap, topics=["/camera"]).select("data").to_pydict()["data"]
    stream = b"".join(_parse_foxglove_compressed_video(envelope).data for envelope in envelopes)
    chunks = [stream[offset : offset + 73] for offset in range(0, len(stream), 73)]
    assert len(chunks) > len(envelopes)

    path = tmp_path / "fragmented-video.mcap"
    with path.open("wb") as output:
        writer = mcap_writer.Writer(output, chunk_size=512)
        writer.start()
        schema_id = writer.register_schema(name="foxglove.CompressedVideo", encoding="protobuf", data=b"")
        channel_id = writer.register_channel(topic="/camera", message_encoding="protobuf", schema_id=schema_id)
        for index, chunk in enumerate(chunks):
            writer.add_message(
                channel_id=channel_id,
                log_time=index * 100,
                publish_time=index * 100 + 1,
                sequence=index,
                data=foxglove_compressed_video_payload(
                    timestamp_seconds=1_725_000_000,
                    timestamp_nanos=index * 1_000_000,
                    frame_id="top-camera",
                    data=chunk,
                    format="h264",
                ),
            )
        writer.finish()

    result = daft.read_mcap(path, topics=["/camera"], decode_video=True, batch_size=2).to_pydict()

    assert result["source_path"] == [str(path)] * 8
    assert result["frame_index"] == list(range(8))
    assert [round(np.asarray(image).mean()) for image in result["data"]] == pytest.approx(
        [0, 23, 48, 73, 98, 123, 148, 173], abs=2
    )


def test_mcap_video_planner_pushdown_projection_and_keyframe_warmup(foxglove_video_mcap) -> None:
    resolver_calls: list[tuple[str, str, int]] = []

    def preceding_keyframe(path: str, topic: str, requested_start: int) -> int:
        resolver_calls.append((path, topic, requested_start))
        return 400

    result = (
        daft.read_mcap(
            foxglove_video_mcap,
            topics=["/camera"],
            decode_video=True,
            batch_size=2,
            video_start_time_resolver=preceding_keyframe,
        )
        .where((daft.col("topic") == "/camera") & (daft.col("log_time") >= 550))  # type: ignore[operator]
        .select("source_path", "log_time", "frame_id")
        .to_pydict()
    )

    assert resolver_calls == [(str(foxglove_video_mcap), "/camera", 550)]
    assert result == {
        "source_path": [str(foxglove_video_mcap)] * 2,
        "log_time": [600, 700],
        "frame_id": ["top-camera", "top-camera"],
    }


def test_mcap_video_time_filter_backscans_safely_without_resolver(foxglove_video_mcap) -> None:
    file = daft.McapFile(str(foxglove_video_mcap))
    assert _find_previous_video_keyframe(file._inner, "/camera", requested_start_time=550) == 400

    result = (
        daft.read_mcap(
            foxglove_video_mcap,
            topics=["/camera"],
            decode_video=True,
            start_time=250,
            batch_size=2,
        )
        .select("log_time")
        .to_pydict()
    )

    assert result == {"log_time": [300, 400, 500, 600, 700]}


def test_mcap_video_time_filter_unindexed_falls_back_to_stream_start(foxglove_video_mcap, tmp_path) -> None:
    mcap_writer = pytest.importorskip("mcap.writer")
    envelopes = daft.read_mcap(foxglove_video_mcap, topics=["/camera"]).select("data").to_pydict()["data"]

    path = tmp_path / "unindexed-video.mcap"
    with path.open("wb") as output:
        writer = mcap_writer.Writer(output, index_types=mcap_writer.IndexType.NONE)
        writer.start()
        schema_id = writer.register_schema(name="foxglove.CompressedVideo", encoding="protobuf", data=b"")
        channel_id = writer.register_channel(topic="/camera", message_encoding="protobuf", schema_id=schema_id)
        for index, envelope in enumerate(envelopes):
            writer.add_message(
                channel_id=channel_id,
                log_time=index * 100,
                publish_time=index * 100 + 1,
                sequence=index,
                data=envelope,
            )
        writer.finish()

    result = (
        daft.read_mcap(path, topics=["/camera"], decode_video=True, start_time=250, batch_size=2)
        .select("log_time")
        .to_pydict()
    )

    assert result == {"log_time": [300, 400, 500, 600, 700]}


def test_mcap_video_resizes_to_fixed_image_type(foxglove_video_mcap) -> None:
    dataframe = daft.read_mcap(
        foxglove_video_mcap,
        topics=["/camera"],
        decode_video=True,
        image_height=8,
        image_width=10,
    ).select("data")

    assert dataframe.schema()["data"].dtype == daft.DataType.image("RGB", height=8, width=10)
    assert dataframe.limit(1).to_pydict()["data"][0].shape == (8, 10, 3)


def test_mcap_video_limit_stops_after_output_frames(foxglove_video_mcap, monkeypatch) -> None:
    from daft.io.mcap import _video

    calls = 0
    original = _video._TopicVideoDecoder.decode

    def tracked_decode(self, message):
        nonlocal calls
        calls += 1
        yield from original(self, message)

    monkeypatch.setattr(_video._TopicVideoDecoder, "decode", tracked_decode)
    assert (
        daft.read_mcap(foxglove_video_mcap, topics=["/camera"], decode_video=True)
        .select("log_time")
        .limit(1)
        .count_rows()
        == 1
    )
    # The parser needs one lookahead message to delimit the first access unit,
    # but the task must not consume the remaining GOP after satisfying the limit.
    assert calls == 2


def test_mcap_video_output_batches_have_a_byte_budget(foxglove_video_mcap, monkeypatch) -> None:
    from daft.io.mcap import _mcap, _video

    batch_lengths: list[int] = []
    original = _video.McapVideoDecoder.to_record_batch

    def tracked_batch(self, frames):
        batch_lengths.append(len(frames))
        return original(self, frames)

    monkeypatch.setattr(_mcap, "MCAP_VIDEO_OUTPUT_BATCH_BYTES", 1)
    monkeypatch.setattr(_video.McapVideoDecoder, "to_record_batch", tracked_batch)

    assert daft.read_mcap(foxglove_video_mcap, topics=["/camera"], decode_video=True).count_rows() == 8
    assert batch_lengths == [1] * 8


def test_mcap_video_validates_options(foxglove_video_mcap) -> None:
    with pytest.raises(ValueError, match="requires at least one explicit topic"):
        daft.read_mcap(foxglove_video_mcap, decode_video=True)
    with pytest.raises(ValueError, match="must be supplied together"):
        daft.read_mcap(foxglove_video_mcap, topics=["/camera"], decode_video=True, image_height=16)
    with pytest.raises(ValueError, match="at or before requested_start_time"):
        daft.read_mcap(
            foxglove_video_mcap,
            topics=["/camera"],
            decode_video=True,
            start_time=100,
            video_start_time_resolver=lambda _path, _topic, _start: 200,
        ).collect()
