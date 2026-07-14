from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Iterator
from fractions import Fraction
from unittest.mock import MagicMock, patch

import av
import numpy as np
import pytest

import daft
from daft.io.av._read_video_frames import _VideoFramesSourceTask


def _make_mock_container(frame_times: list[float], key_frames: list[bool] | None = None):
    mock_container = MagicMock()
    mock_stream = MagicMock()
    mock_stream.type = "video"
    mock_stream.codec_context = MagicMock()
    mock_container.streams = [mock_stream]

    frames: list[MagicMock] = []
    for idx, t in enumerate(frame_times):
        frame = MagicMock()
        frame.time = t
        frame.time_base = Fraction(1, 1)
        frame.pts = idx
        frame.dts = idx
        frame.duration = 1
        frame.key_frame = key_frames[idx] if key_frames is not None else False
        frame.reformat.return_value = frame
        frame.to_ndarray.return_value = np.zeros((2, 2, 3), dtype="uint8")
        frames.append(frame)

    def decode_side_effect(*args, **kwargs):  # type: ignore[override]
        if decode_side_effect.frames:
            frame = decode_side_effect.frames.pop(0)
            return iter([frame])
        raise StopIteration

    decode_side_effect.frames = frames.copy()  # type: ignore[attr-defined]
    mock_container.decode.side_effect = decode_side_effect

    return mock_container


def test_read_video_eof():
    """Test that _list_frames handles av.EOFError gracefully."""
    # Create mock container and stream
    mock_container = MagicMock()
    mock_stream = MagicMock()
    mock_stream.type = "video"
    mock_stream.codec_context = MagicMock()
    mock_container.streams = [mock_stream]
    mock_container.decode.side_effect = av.EOFError(0, "mock message", "mock.mp4")

    # Create single task because we only want to test `_list_frames`.
    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
    )

    # Mock av.open to return our mock container
    with patch("av.open", return_value=mock_container):
        # Call _list_frames and collect results
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    # Verify no frames were yielded (EOF raised immediately) AND no raising
    assert len(frames) == 0

    # Verify container was closed
    mock_container.close.assert_called_once()


def test_video_reads_do_not_block_shared_event_loop():
    barrier = threading.Barrier(2)
    producer_threads: list[int] = []

    def read_batches() -> Iterator[MagicMock]:
        producer_threads.append(threading.get_ident())
        barrier.wait(timeout=1)
        yield MagicMock()

    tasks = [
        _VideoFramesSourceTask(
            path=f"test-{index}.mp4",
            image_height=480,
            image_width=640,
            is_key_frame=None,
            io_config=None,
        )
        for index in range(2)
    ]
    for task in tasks:
        task._read_record_batches = read_batches  # type: ignore[method-assign]

    async def collect(task: _VideoFramesSourceTask) -> list[MagicMock]:
        return [batch async for batch in task.read()]

    async def collect_all() -> list[list[MagicMock]]:
        return await asyncio.gather(*(collect(task) for task in tasks))

    results = asyncio.run(collect_all())

    assert [len(result) for result in results] == [1, 1]
    assert len(set(producer_threads)) == 2
    assert threading.get_ident() not in producer_threads


def test_video_read_applies_backpressure():
    produced_batches = 0
    producer_done = threading.Event()

    def read_batches() -> Iterator[MagicMock]:
        nonlocal produced_batches
        try:
            for _ in range(100):
                produced_batches += 1
                yield MagicMock()
        finally:
            producer_done.set()

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
    )
    task._read_record_batches = read_batches  # type: ignore[method-assign]

    async def read_one() -> None:
        reader = task.read()
        await anext(reader)
        while produced_batches < 3:
            await asyncio.sleep(0.01)
        assert produced_batches == 3
        await reader.aclose()
        await asyncio.to_thread(producer_done.wait, 1)

    asyncio.run(read_one())
    assert producer_done.is_set()


def test_video_read_close_does_not_wait_for_decode():
    decode_started = threading.Event()
    release_decode = threading.Event()
    producer_done = threading.Event()

    def read_batches() -> Iterator[MagicMock]:
        try:
            yield MagicMock()
            decode_started.set()
            release_decode.wait(timeout=1)
            yield MagicMock()
        finally:
            producer_done.set()

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
    )
    task._read_record_batches = read_batches  # type: ignore[method-assign]

    async def close_while_decoding() -> float:
        reader = task.read()
        await anext(reader)
        await asyncio.to_thread(decode_started.wait, 1)
        start = time.monotonic()
        await reader.aclose()
        elapsed = time.monotonic() - start
        release_decode.set()
        await asyncio.to_thread(producer_done.wait, 1)
        return elapsed

    elapsed = asyncio.run(close_while_decoding())

    assert elapsed < 0.1
    assert producer_done.is_set()


@pytest.mark.integration()
def test_read_video_frames_s3(pytestconfig):
    """Test that we can read video frames from S3."""
    if not pytestconfig.getoption("--credentials"):
        pytest.skip(reason="Video test requires credentials and `--credentials` flag")

    df = daft.read_video_frames(
        "s3://daft-oss-public-datasets/Hollywood2-actions/actionclipautoautotrain00002.avi",
        image_height=480,
        image_width=640,
    ).collect()

    # Verify expected number of frames
    assert len(df) == 231

    # Verify expected columns exist
    expected_columns = {
        "path",
        "frame_index",
        "frame_time",
        "frame_time_base",
        "frame_pts",
        "frame_dts",
        "frame_duration",
        "is_key_frame",
        "data",
    }
    assert set(df.column_names) == expected_columns

    df = df.select("path", "frame_index").to_pydict()

    # Verify path column contains correct S3 path for all rows
    assert all(
        path == "s3://daft-oss-public-datasets/Hollywood2-actions/actionclipautoautotrain00002.avi"
        for path in df["path"]
    )

    # Verify frame_index is sequential starting from 0
    assert df["frame_index"] == list(range(231))


def test_list_frames_no_sampling_returns_all_frames():
    frame_times = [0.0, 0.5, 1.0, 1.5, 2.0]
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=None,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    assert [f.frame_time for f in frames] == frame_times
    assert [f.frame_index for f in frames] == list(range(len(frame_times)))


def test_list_frames_sampling_by_seconds_filters_frames():
    frame_times = [0.0, 0.4, 1.0, 1.4, 2.0, 2.4, 3.0]
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=1.0,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    sampled_times = [f.frame_time for f in frames]
    assert sampled_times == [0.0, 1.0, 2.0, 3.0]


@pytest.mark.parametrize("is_key_frame", [None, True, False])
def test_list_frames_is_key_frame_backward_compatible(is_key_frame):
    frame_times = [0.0, 0.5, 1.0]
    key_flags = [True, False, True]
    mock_container = _make_mock_container(frame_times, key_frames=key_flags)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=is_key_frame,
        io_config=None,
        sample_interval_seconds=None,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    assert [f.frame_time for f in frames] == frame_times
    assert [f.is_key_frame for f in frames] == key_flags


def test_list_frames_invalid_sample_interval_raises():
    mock_container = _make_mock_container([0.0])

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=0.0,
    )

    with patch("av.open", return_value=mock_container), pytest.raises(ValueError):
        list(task._list_frames("test.mp4", "dummy_file"))


def test_list_frames_invalid_sample_interval_negative():
    """Test that negative sample interval raises ValueError."""
    mock_container = _make_mock_container([0.0])

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=-1.0,
    )

    with patch("av.open", return_value=mock_container), pytest.raises(ValueError):
        list(task._list_frames("test.mp4", "dummy_file"))


def test_list_frames_sampling_with_non_uniform_frames():
    """Test sampling with non-uniform frame timestamps."""
    frame_times = [0.0, 0.95, 1.05, 2.0, 2.95, 3.05]
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=1.0,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    sampled_times = [f.frame_time for f in frames]
    assert len(sampled_times) == 4
    assert sampled_times[0] == 0.0
    assert sampled_times[1] in [0.95, 1.05]
    assert sampled_times[2] == 2.0
    assert sampled_times[3] in [2.95, 3.05]


def test_list_frames_sampling_with_large_frame_interval():
    """Test sampling when frame interval is larger than sample interval."""
    frame_times = [0.0, 2.5, 5.0]
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=1.0,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    sampled_times = [f.frame_time for f in frames]
    assert sampled_times == [0.0, 2.5, 5.0]


def test_list_frames_sampling_with_small_interval():
    """Test sampling with very small interval."""
    frame_times = [0.0, 0.033, 0.067, 0.100, 0.133, 0.167]
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=0.05,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    sampled_times = [f.frame_time for f in frames]
    assert len(sampled_times) == 4
    assert sampled_times[0] == 0.0
    assert sampled_times[1] in [0.067]
    assert sampled_times[2] == 0.100
    assert sampled_times[3] in [0.167]


def test_list_frames_sampling_with_none_timestamps():
    """Test sampling with frames that have None timestamps."""
    frame_times = [0.0, None, 1.0, None, 2.0]
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=1.0,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    sampled_times = [f.frame_time for f in frames]
    assert sampled_times == [0.0, 1.0, 2.0]


def test_list_frames_sampling_with_vfr_video():
    """Test sampling with variable frame rate (VFR) video."""
    frame_times = [0.0, 0.033, 0.100, 0.133, 0.233, 0.267, 1.0, 1.033]
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=1.0,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    sampled_times = [f.frame_time for f in frames]
    assert sampled_times == [0.0, 1.0]


def test_list_frames_sampling_with_multiple_frames_near_target():
    """Test sampling when multiple frames are near the target time."""
    frame_times = [0.0, 0.95, 0.96, 0.97, 0.98, 0.99, 1.01, 1.02, 2.0]
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=1.0,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    sampled_times = [f.frame_time for f in frames]
    assert len(sampled_times) == 3
    assert sampled_times[0] == 0.0
    assert sampled_times[1] >= 1.0
    assert sampled_times[2] == 2.0


def test_list_frames_sampling_with_floating_point_precision():
    """Test sampling with floating point precision edge cases."""
    frame_times = [0.0, 0.3333333333333333, 0.6666666666666666, 1.0]
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=0.3333333333333333,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    sampled_times = [f.frame_time for f in frames]
    assert len(sampled_times) == 4
    assert sampled_times[0] == 0.0
    assert sampled_times[1] in [0.3333333333333333]
    assert sampled_times[2] in [0.6666666666666666]
    assert sampled_times[3] == 1.0


def test_list_frames_sampling_with_single_frame():
    """Test sampling with a single frame video."""
    frame_times = [0.0]
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=1.0,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    assert len(frames) == 1
    assert frames[0].frame_time == 0.0


def test_list_frames_sampling_with_empty_video():
    """Test sampling with an empty video."""
    frame_times = []
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=1.0,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    assert len(frames) == 0


def test_list_frames_sampling_with_very_small_interval():
    """Test sampling with very small interval."""
    frame_times = [0.0, 0.001, 0.002, 0.003, 0.004, 0.005]
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=0.001,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    sampled_times = [f.frame_time for f in frames]
    assert sampled_times == frame_times


def test_list_frames_sampling_with_very_large_interval():
    """Test sampling with very large interval."""
    frame_times = [0.0, 0.5, 1.0, 1.5, 2.0, 100.0, 200.0]
    mock_container = _make_mock_container(frame_times)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=None,
        io_config=None,
        sample_interval_seconds=100.0,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    sampled_times = [f.frame_time for f in frames]
    assert sampled_times == [0.0, 100.0, 200.0]


def test_list_frames_sampling_with_is_key_frame_true():
    """Test sampling combined with is_key_frame=True."""
    frame_times = [0.0, 0.5, 1.0, 1.5, 2.0]
    key_flags = [True, False, True, False, True]
    mock_container = _make_mock_container(frame_times, key_frames=key_flags)

    task = _VideoFramesSourceTask(
        path="test.mp4",
        image_height=480,
        image_width=640,
        is_key_frame=True,
        io_config=None,
        sample_interval_seconds=1.0,
    )

    with patch("av.open", return_value=mock_container):
        frames = list(task._list_frames("test.mp4", "dummy_file"))

    sampled_times = [f.frame_time for f in frames]
    sampled_keys = [f.is_key_frame for f in frames]
    assert sampled_times == [0.0, 1.0, 2.0]
    assert all(sampled_keys)
