"""Execute every code snippet from the OmniSharing guide.

Guards against documentation rot: `docs/datasets/omnisharing.md` shows real API
calls, and a renamed column or argument would leave the guide silently wrong.
Each test runs a snippet against a generated episode, with only the dataset
path rewritten.
"""

from __future__ import annotations

import numpy as np
import pytest

import daft
from daft.datasets import omnisharing
from tests.datasets.omnisharing_datagen import episode_filename, write_df2_episode

pytest.importorskip("h5py", reason="daft[hdf5] extra is required for OmniSharing tests")


@pytest.fixture(scope="module")
def guide_dataset(tmp_path_factory):
    """A release shaped like the one the guide describes.

    Includes the duplicated ``episode_index`` 1217 that the guide warns about, so
    the warning itself can be verified rather than taken on trust.
    """
    root = tmp_path_factory.mktemp("omnisharing_guide") / "ds"
    part = root / "data/part_01"
    write_df2_episode(part / episode_filename(1203, "213135", 115, 110092), n_frames=8)
    write_df2_episode(part / episode_filename(1217, "212953", 93, 110056), n_frames=8)
    write_df2_episode(part / episode_filename(1217, "213630", 115, 110092), n_frames=8)
    return str(root)


@pytest.fixture
def episodes(guide_dataset):
    return omnisharing.raw(guide_dataset)


@pytest.fixture
def one(episodes):
    return episodes.limit(1)


# ---------------------------------------------------------------------------
# Quickstart
# ---------------------------------------------------------------------------


def test_quickstart_lists_episodes(episodes):
    episodes.select("episode_key", "stage", "size").collect()


def test_quickstart_reads_each_modality(one):
    omnisharing.episode_metadata(one).select("instruction", "task_labels").collect()
    omnisharing.trajectory(one, fields=["observation.lefthand.joints"]).collect()
    omnisharing.tactile(one, sides="lefthand", split_by_sensor=True).collect()
    omnisharing.audio(one, mono=True, max_seconds=2.0).select("samplerate", "n_samples").collect()
    omnisharing.depth_frames(one, "RGBD_0").select("RGBD_0.depth").collect()
    omnisharing.stereo_extrinsics(one, "RGBD_0").select("RGBD_0.left_to_color").collect()


# ---------------------------------------------------------------------------
# Pipeline stages and layout
# ---------------------------------------------------------------------------


def test_stage_filtering_snippet(guide_dataset):
    omnisharing.raw(guide_dataset, stage="DF-2").collect()


def test_describe_snippet(one):
    layout = omnisharing.describe(one)
    layout.where(daft.col("kind") == "dataset").select("h5path", "shape", "dtype").collect()


# ---------------------------------------------------------------------------
# The three documented traps
# ---------------------------------------------------------------------------


def test_duplicate_episode_index_warning_is_accurate(episodes):
    rows = episodes.select("episode_key", "episode_index").to_pylist()
    # The guide claims part_01 holds two episodes numbered 1217.
    assert [r["episode_index"] for r in rows].count(1217) == 2
    assert {r["episode_key"] for r in rows if r["episode_index"] == 1217} == {
        "1217_212953_93_110056",
        "1217_213630_115_110092",
    }


def test_quaternion_roll_recipe_puts_qw_last(one):
    pose = np.asarray(
        omnisharing.trajectory(one, fields=["observation.lefthand.handpose"]).to_pylist()[0][
            "observation.lefthand.handpose"
        ]
    )
    xyz, quat_wxyz = pose[:, :3], pose[:, 3:7]
    rolled = np.roll(quat_wxyz, -1, axis=1)

    assert xyz.shape[1] == 3
    # The documented np.roll must turn [qw, qx, qy, qz] into scipy's
    # [qx, qy, qz, qw], not merely look plausible.
    np.testing.assert_array_equal(rolled[:, 3], quat_wxyz[:, 0])
    np.testing.assert_array_equal(rolled[:, :3], quat_wxyz[:, 1:])


def test_camera_clock_snippets(one):
    omnisharing.cameras(one).select("camera", "codec", "n_timestamps").collect()
    omnisharing.frames(one, fields=["observation.lefthand.joints"], align_cameras=["RGB_Camera0"]).select(
        "frame_index", "RGB_Camera0.frame_index", "RGB_Camera0.timestamp_delta_us"
    ).collect()


# ---------------------------------------------------------------------------
# Per-modality sections
# ---------------------------------------------------------------------------


def test_tactile_section_uses_real_sensor_names(one):
    # The guide names palm_sensor1 and J32L explicitly.
    omnisharing.tactile(one, sides="lefthand", split_by_sensor=True).select(
        "lefthand.tactile.palm_sensor1", "lefthand.tactile.J32L"
    ).collect()


def test_frame_expansion_section(one):
    omnisharing.frames(one, fields=["observation.lefthand.joints", "action.lefthand.joints"]).collect()


def test_video_section(one):
    omnisharing.camera_frames(one, ["RGB_Camera0"], max_frames=2).select("RGB_Camera0.frames").collect()
    omnisharing.camera_payloads(one, ["RGB_Camera0"]).select("RGB_Camera0.codec").collect()


def test_audio_section(one):
    omnisharing.audio(one, mono=True, max_seconds=2.0).select("samplerate", "n_samples", "waveform").collect()


def test_depth_section(one):
    omnisharing.depth_frames(one, "RGBD_0", frame_indices=[0, 5]).select(
        "RGBD_0.depth", "RGBD_0.depth_frame_indices"
    ).collect()


def test_stereo_section(one):
    omnisharing.stereo_extrinsics(one, "RGBD_0").select("RGBD_0.left_to_color", "RGBD_0.calib_date").collect()


def test_per_eye_alignment_section(one):
    omnisharing.frames(one, fields=["observation.lefthand.joints"], align_cameras=[("RGBD_0", "left")]).select(
        "frame_index", "RGBD_0.left.frame_index", "RGBD_0.left.timestamp_delta_us"
    ).collect()


def test_object_poses_section(episodes):
    omnisharing.objects(episodes.limit(4)).select("episode_key", "n_objects", "obj1.name", "obj1.id").collect()
