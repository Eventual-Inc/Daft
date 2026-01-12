from __future__ import annotations

import pytest

pytest.importorskip("av")

import daft
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
