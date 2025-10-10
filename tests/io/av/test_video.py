from __future__ import annotations

from unittest.mock import MagicMock, patch

import av
import pytest

import daft
from daft.io.av._read_video_frames import _VideoFramesSourceTask


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


@pytest.mark.integration()
def test_read_video_frames_s3(pytestconfig):
    """Test that we can read video frames from S3."""
    if not pytestconfig.getoption("--credentials"):
        pytest.skip(reason="Video test requires credentials and `--credentials` flag")

    df = daft.read_video_frames(
        "s3://daft-public-datasets/Hollywood2-actions/actionclipautoautotrain00002.avi",
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
        path == "s3://daft-public-datasets/Hollywood2-actions/actionclipautoautotrain00002.avi" for path in df["path"]
    )

    # Verify frame_index is sequential starting from 0
    assert df["frame_index"] == list(range(231))
