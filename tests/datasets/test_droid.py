from __future__ import annotations

import pytest

import daft
from daft import DataType, MediaType
from daft.expressions import col

pytestmark = pytest.mark.integration()

DROID_RAW_GCS_PREFIX = "gs://gresearch/robotics/droid_raw"


@pytest.fixture(scope="module")
def droid_raw_df():
    return daft.datasets.droid.raw()


def test_droid_discovers_episodes_and_metadata(droid_raw_df) -> None:
    result = droid_raw_df.select("uuid", "building", "success").limit(1).to_pydict()

    assert len(result["uuid"]) == 1
    assert isinstance(result["uuid"][0], str) and result["uuid"][0]
    assert isinstance(result["building"][0], str) and result["building"][0]
    assert isinstance(result["success"][0], bool)


def test_droid_unnests_metadata_columns(droid_raw_df) -> None:
    schema = {field.name: field.dtype for field in droid_raw_df.schema()}
    assert "building" in schema
    assert "success" in schema
    assert "metadata" not in schema
    assert schema["building"] == DataType.string
    assert schema["success"] == DataType.bool


def test_droid_adds_trajectory_and_video_file_columns(droid_raw_df) -> None:
    schema = {field.name: field.dtype for field in droid_raw_df.schema()}
    assert schema["trajectory"] == DataType.file(MediaType.hdf5())
    assert schema["wrist_video"] == DataType.file(MediaType.video())
    assert schema["ext1_video"] == DataType.file(MediaType.video())
    assert schema["ext2_video"] == DataType.file(MediaType.video())

    result = (
        droid_raw_df.select(
            "episode_dir",
            "wrist_cam_serial",
            "ext1_cam_serial",
            "ext2_cam_serial",
            col("trajectory").file_path().alias("trajectory_path"),
            col("wrist_video").file_path().alias("wrist_video_path"),
            col("ext1_video").file_path().alias("ext1_video_path"),
            col("ext2_video").file_path().alias("ext2_video_path"),
        )
        .limit(1)
        .to_pydict()
    )

    episode_dir = result["episode_dir"][0]
    assert episode_dir.startswith(f"{DROID_RAW_GCS_PREFIX}/")

    trajectory_path = result["trajectory_path"][0]
    assert trajectory_path == f"{episode_dir}/trajectory.h5"

    assert result["wrist_video_path"][0] == f"{episode_dir}/recordings/MP4/{result['wrist_cam_serial'][0]}.mp4"
    assert result["ext1_video_path"][0] == f"{episode_dir}/recordings/MP4/{result['ext1_cam_serial'][0]}.mp4"
    assert result["ext2_video_path"][0] == f"{episode_dir}/recordings/MP4/{result['ext2_cam_serial'][0]}.mp4"
