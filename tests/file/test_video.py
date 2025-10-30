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
