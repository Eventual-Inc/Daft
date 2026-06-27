from __future__ import annotations

import pytest

import daft
from daft.datasets.droid import camera_frames
from daft.functions import video_file


@pytest.mark.integration()
def test_droid_camera_frames_decodes_real_video_end_to_end() -> None:
    pytest.importorskip("av")

    sample_video_path = "tests/assets/sample_video.mp4"
    episodes = daft.from_pydict(
        {
            "uuid": ["episode-1"],
            "wrist_cam_video": [sample_video_path],
        }
    ).with_column("wrist_cam_video", video_file(daft.col("wrist_cam_video")))

    result = (
        camera_frames(
            episodes,
            cameras="wrist",
            start_time=0,
            end_time=0.05,
            width=64,
            height=48,
        )
        .select("uuid", "wrist_cam_frames")
        .collect()
        .to_pydict()
    )

    frames = result["wrist_cam_frames"][0]
    assert result["uuid"] == ["episode-1"]
    assert len(frames) == 1
    assert frames[0]["frame_index"] == 0
    assert frames[0]["frame_time"] == pytest.approx(0.033)
    assert frames[0]["is_key_frame"] is True
    assert frames[0]["data"].shape == (48, 64, 3)
