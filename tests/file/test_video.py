from __future__ import annotations

import pytest

pytest.importorskip("av")

import daft
from daft import dependencies
from daft.schema import Field


@pytest.fixture(scope="module")
def sample_video_path():
    return "tests/assets/sample_video.mp4"


def test_video_file_standalone(sample_video_path):
    file = daft.VideoFile(sample_video_path)
    keyframes = list(file.keyframes())
    assert len(keyframes) == 13


def test_video_file_dtype(sample_video_path):
    df = daft.from_pydict({"path": [sample_video_path]})
    df = df.select(daft.functions.video_file(df["path"]).alias("video"))

    field = next(df.schema().__iter__())

    assert field.dtype == daft.DataType.file(daft.MediaType.video())


def test_video_file_verify():
    df = daft.from_pydict({"path": ["tests/assets/sampled-tpch.jsonl"]})

    with pytest.raises(ValueError):
        df = df.select(daft.functions.video_file(df["path"], verify=True).alias("video"))
        df.collect()


def test_video_file_verify_ok(sample_video_path):
    df = daft.from_pydict({"path": [sample_video_path]})

    df = df.select(daft.functions.video_file(df["path"], verify=True).alias("video"))
    df.collect()


def test_get_metadata(sample_video_path):
    df = daft.from_pydict({"path": [sample_video_path]})
    df = df.select(daft.functions.video_file(df["path"], verify=True).alias("video"))
    df = df.select(daft.functions.video_metadata(df["video"]))

    expected = {
        "video": [{"width": 192, "height": 144, "fps": 30.0, "frame_count": 290, "time_base": 1.1111111111111112e-05}]
    }

    assert df.to_pydict() == expected


def test_keyframes(sample_video_path):
    df = daft.from_pydict({"path": [sample_video_path]})
    df = df.select(daft.functions.video_file(df["path"], verify=True).alias("video"))
    df = df.select(daft.functions.video_keyframes(df["video"]))
    expected_schema = daft.Schema._from_fields([Field.create("video", daft.DataType.list(daft.DataType.image()))])

    actual_schema = df.schema()
    assert actual_schema == expected_schema

    values = df.to_pydict()["video"][0]
    assert len(values) == 13


def test_keyframes_start_time_beyond_duration_returns_empty(sample_video_path):
    file = daft.VideoFile(sample_video_path)
    metadata = file.metadata()
    duration = metadata["duration"]
    assert duration is not None

    start_time = duration + 1.0
    keyframes = list(file.keyframes(start_time=start_time))

    assert len(keyframes) == 0


def test_keyframes_start_time_skips_early_frames(sample_video_path):
    import av
    import numpy as np

    file = daft.VideoFile(sample_video_path)
    metadata = file.metadata()
    duration = metadata["duration"]
    assert duration is not None

    start_time = duration / 2.0

    later_keyframes = list(file.keyframes(start_time=start_time))

    # Verify using lower-level av API to check timestamps and content
    with av.open(sample_video_path) as container:
        stream = container.streams.video[0]
        stream.codec_context.skip_frame = "NONKEY"
        expected_frames = [f for f in container.decode(stream) if f.time >= start_time]

    assert len(later_keyframes) == len(expected_frames)
    assert len(later_keyframes) > 0

    np.testing.assert_array_equal(np.array(later_keyframes[0]), np.array(expected_frames[0].to_image()))


def test_video_keyframes_start_time_beyond_duration_returns_empty(sample_video_path):
    file = daft.VideoFile(sample_video_path)
    metadata = file.metadata()
    duration = metadata["duration"]
    assert duration is not None

    start_time = duration + 1.0

    df = daft.from_pydict({"path": [sample_video_path]})
    df = df.select(daft.functions.video_file(df["path"], verify=True).alias("video"))
    df = df.select(daft.functions.video_keyframes(df["video"], start_time=start_time).alias("frames"))

    frames = df.to_pydict()["frames"][0]
    assert frames == []


def test_keyframes_raises_informative_error_when_pillow_missing(sample_video_path, monkeypatch):
    """Regression test for issue #6064: ensure informative error when pillow is missing."""
    monkeypatch.setattr(dependencies.pil_image, "module_available", lambda: False)

    file = daft.VideoFile(sample_video_path)

    with pytest.raises(ImportError, match="pillow.*required.*pip install daft\\[video\\]"):
        list(file.keyframes())


# --- video_frames tests ---


def test_frames_standalone(sample_video_path):
    """VideoFile.frames() returns all 290 frames with metadata."""
    file = daft.VideoFile(sample_video_path)
    frames = list(file.frames())
    assert len(frames) == 290

    # Check first frame metadata
    first = frames[0]
    assert first["frame_index"] == 0
    assert first["is_key_frame"] is True
    assert first["frame_time"] is not None
    assert first["data"].size == (192, 144)


def test_frames_with_resize(sample_video_path):
    """VideoFile.frames() resizes frames when width/height are given."""
    file = daft.VideoFile(sample_video_path)
    frames = list(file.frames(width=64, height=48))
    assert len(frames) == 290
    assert frames[0]["data"].size == (64, 48)


def test_frames_with_partial_resize_raises(sample_video_path):
    file = daft.VideoFile(sample_video_path)

    with pytest.raises(ValueError, match="Both width and height must be specified together"):
        list(file.frames(width=64))


def test_frames_time_range(sample_video_path):
    """VideoFile.frames() filters by start_time and end_time."""
    file = daft.VideoFile(sample_video_path)
    all_frames = list(file.frames())
    subset = list(file.frames(start_time=5.0, end_time=6.0))

    assert len(subset) > 0
    assert len(subset) < len(all_frames)
    for f in subset:
        assert f["frame_time"] is not None
        assert f["frame_time"] >= 5.0
        assert f["frame_time"] <= 6.0

    expected_subset = [f for f in all_frames if f["frame_time"] is not None and 5.0 <= f["frame_time"] <= 6.0]
    assert [f["frame_index"] for f in subset] == [f["frame_index"] for f in expected_subset]


def test_frames_start_time_beyond_duration_returns_empty(sample_video_path):
    file = daft.VideoFile(sample_video_path)
    metadata = file.metadata()
    duration = metadata["duration"]
    assert duration is not None

    frames = list(file.frames(start_time=duration + 1.0))
    assert len(frames) == 0


def test_frames_includes_keyframe_and_non_keyframe(sample_video_path):
    """video_frames decodes all frames, not just keyframes."""
    file = daft.VideoFile(sample_video_path)
    frames = list(file.frames())

    key_count = sum(1 for f in frames if f["is_key_frame"])
    non_key_count = sum(1 for f in frames if not f["is_key_frame"])

    # The sample video has 13 keyframes and 277 non-keyframes
    assert key_count == 13
    assert non_key_count == 277


def test_frames_can_filter_keyframes(sample_video_path):
    file = daft.VideoFile(sample_video_path)

    keyframes = list(file.frames(is_key_frame=True))

    assert len(keyframes) == 13
    assert all(frame["is_key_frame"] for frame in keyframes)


def test_frames_can_filter_non_keyframes(sample_video_path):
    file = daft.VideoFile(sample_video_path)

    frames = list(file.frames(is_key_frame=False))

    assert len(frames) == 277
    assert all(not frame["is_key_frame"] for frame in frames)


def test_video_frames_expression(sample_video_path):
    """video_frames() expression function returns correct schema and data."""
    df = daft.from_pydict({"path": [sample_video_path]})
    df = df.select(daft.functions.video_file(df["path"], verify=True).alias("video"))
    df = df.select(daft.functions.video_frames(df["video"]).alias("frames"))

    result = df.to_pydict()["frames"][0]
    assert len(result) == 290

    # Each element should be a struct with expected keys
    first = result[0]
    assert "frame_index" in first
    assert "frame_time" in first
    assert "is_key_frame" in first
    assert "data" in first


def test_video_frames_expression_with_time_range(sample_video_path):
    """video_frames() expression function respects start_time/end_time."""
    df = daft.from_pydict({"path": [sample_video_path]})
    df = df.select(daft.functions.video_file(df["path"], verify=True).alias("video"))
    df = df.select(daft.functions.video_frames(df["video"], start_time=5.0, end_time=6.0).alias("frames"))

    result = df.to_pydict()["frames"][0]
    assert len(result) > 0
    assert len(result) < 290


def test_video_frames_expression_can_filter_keyframes(sample_video_path):
    df = daft.from_pydict({"path": [sample_video_path]})
    df = df.select(daft.functions.video_file(df["path"], verify=True).alias("video"))
    df = df.select(daft.functions.video_frames(df["video"], is_key_frame=True).alias("frames"))

    result = df.to_pydict()["frames"][0]
    assert len(result) == 13
    assert all(frame["is_key_frame"] for frame in result)


def test_video_frames_expression_with_resize(sample_video_path):
    """video_frames() expression function respects width/height."""
    df = daft.from_pydict({"path": [sample_video_path]})
    df = df.select(daft.functions.video_file(df["path"], verify=True).alias("video"))
    df = df.select(daft.functions.video_frames(df["video"], width=64, height=48).alias("frames"))

    result = df.to_pydict()["frames"][0]
    assert len(result) == 290
    assert result[0]["data"].shape == (48, 64, 3)


def test_frames_raises_informative_error_when_pillow_missing(sample_video_path, monkeypatch):
    monkeypatch.setattr(dependencies.pil_image, "module_available", lambda: False)

    file = daft.VideoFile(sample_video_path)

    with pytest.raises(ImportError, match="pillow.*required.*pip install daft\\[video\\]"):
        list(file.frames())


# --- sample_interval_seconds tests ---


def test_frames_sample_interval_seconds(sample_video_path):
    """VideoFile.frames(sample_interval_seconds=1.0) emits one frame per second."""
    file = daft.VideoFile(sample_video_path)
    sampled = list(file.frames(sample_interval_seconds=1.0))

    # Sample video is 290 frames at 30 fps ~= 9.67s, so sampling at 1.0s yields t=0,1,...,9 → 10 frames
    assert len(sampled) == 10

    times = [f["frame_time"] for f in sampled]
    # First emitted frame is at t=0 (first frame whose time >= 0)
    assert times[0] == pytest.approx(0.0, abs=0.05)
    # Each subsequent emitted time is monotonically >= previous + ~1.0
    for prev, curr in zip(times, times[1:]):
        assert curr - prev >= 0.95


def test_frames_sample_interval_zero_raises(sample_video_path):
    file = daft.VideoFile(sample_video_path)
    with pytest.raises(ValueError, match="sample_interval_seconds must be positive"):
        list(file.frames(sample_interval_seconds=0.0))


def test_frames_sample_interval_negative_raises(sample_video_path):
    file = daft.VideoFile(sample_video_path)
    with pytest.raises(ValueError, match="sample_interval_seconds must be positive"):
        list(file.frames(sample_interval_seconds=-1.0))


def test_frames_sample_interval_combined_with_keyframe(sample_video_path):
    """sample_interval combined with is_key_frame=True samples among keyframes only."""
    file = daft.VideoFile(sample_video_path)
    keyframes_only = list(file.frames(is_key_frame=True))
    sampled_keyframes = list(file.frames(is_key_frame=True, sample_interval_seconds=1.0))

    # Sampling should yield no more frames than the unsampled keyframe set
    assert len(sampled_keyframes) <= len(keyframes_only)
    assert all(f["is_key_frame"] for f in sampled_keyframes)


def test_video_frames_expression_sample_interval(sample_video_path):
    """video_frames() expression accepts sample_interval_seconds and applies it."""
    df = daft.from_pydict({"path": [sample_video_path]})
    df = df.select(daft.functions.video_file(df["path"], verify=True).alias("video"))
    df = df.select(daft.functions.video_frames(df["video"], sample_interval_seconds=1.0).alias("frames"))

    result = df.to_pydict()["frames"][0]
    assert len(result) == 10


# --- video_frames_from_bytes tests ---


def test_video_frames_from_bytes_standalone(sample_video_path):
    """video_frames_from_bytes() reads bytes column and produces same frames as video_frames()."""
    from pathlib import Path

    video_bytes = Path(sample_video_path).read_bytes()
    df = daft.from_pydict({"video_bytes": [video_bytes]})
    df = df.select(daft.functions.video_frames_from_bytes(df["video_bytes"]).alias("frames"))

    result = df.to_pydict()["frames"][0]
    assert len(result) == 290
    first = result[0]
    assert "frame_index" in first
    assert "frame_time" in first
    assert "is_key_frame" in first
    assert "data" in first


def test_video_frames_from_bytes_sample_interval(sample_video_path):
    """video_frames_from_bytes() respects sample_interval_seconds."""
    from pathlib import Path

    video_bytes = Path(sample_video_path).read_bytes()
    df = daft.from_pydict({"video_bytes": [video_bytes]})
    df = df.select(
        daft.functions.video_frames_from_bytes(df["video_bytes"], sample_interval_seconds=1.0).alias("frames")
    )

    result = df.to_pydict()["frames"][0]
    assert len(result) == 10


def test_video_frames_from_bytes_resize(sample_video_path):
    """video_frames_from_bytes() respects width/height."""
    from pathlib import Path

    video_bytes = Path(sample_video_path).read_bytes()
    df = daft.from_pydict({"video_bytes": [video_bytes]})
    df = df.select(daft.functions.video_frames_from_bytes(df["video_bytes"], width=64, height=48).alias("frames"))

    result = df.to_pydict()["frames"][0]
    assert len(result) == 290
    assert result[0]["data"].shape == (48, 64, 3)


def test_video_frames_from_bytes_keyframe_filter(sample_video_path):
    """video_frames_from_bytes() respects is_key_frame=True."""
    from pathlib import Path

    video_bytes = Path(sample_video_path).read_bytes()
    df = daft.from_pydict({"video_bytes": [video_bytes]})
    df = df.select(daft.functions.video_frames_from_bytes(df["video_bytes"], is_key_frame=True).alias("frames"))

    result = df.to_pydict()["frames"][0]
    assert len(result) == 13
    assert all(frame["is_key_frame"] for frame in result)


def test_video_frames_from_bytes_null_input_returns_empty_list(sample_video_path):
    """video_frames_from_bytes() returns [] for null rows instead of crashing.

    Lets upstream UDFs signal failure with None (e.g. a download UDF whose
    blobstore client errored) without aborting the whole batch — the
    surrounding pipeline can branch on the same null column to populate an
    ``extract_error`` column.
    """
    from pathlib import Path

    video_bytes = Path(sample_video_path).read_bytes()
    df = daft.from_pydict({"video_bytes": [video_bytes, None]})
    df = df.select(daft.functions.video_frames_from_bytes(df["video_bytes"]).alias("frames"))

    out = df.to_pydict()["frames"]
    assert len(out) == 2
    assert len(out[0]) == 290  # ok row keeps the full frame list
    assert out[1] == []  # null row yields empty list, no exception
