"""Integration tests against the real PX OmniSharing release.

Excluded from the default run (`-m 'not integration'`); enable with
`pytest -m integration`. These stream one episode over `hf://` range reads and
write nothing to disk, but a full pass takes several minutes because each read is
a separate high-latency request.

They exist to catch the class of bug synthetic data cannot: values that are
structurally valid but physically wrong. Where possible an assertion is anchored
to something independently checkable -- a unit quaternion norm, a stereo baseline
in millimetres, a sample count that must agree with a different code path.
"""

from __future__ import annotations

import numpy as np
import pytest

import daft
from daft.datasets import omnisharing
from daft.functions import endswith

pytest.importorskip("h5py", reason="daft[hdf5] extra is required for OmniSharing tests")

#: Smallest episode in part_01 (~440 MB), pinned so failures are reproducible.
DIRECTORY = "hf://datasets/paxini/Omnisharing_DB_SampleData/data/part_01"
EPISODE = "episode_1203_213135_115_110092_glove.hdf5"

#: Frame count of the pinned episode's observation clock.
N_FRAMES = 207


@pytest.fixture(scope="module")
def episodes():
    """The pinned episode, selected by path rather than by index."""
    return omnisharing.raw(DIRECTORY).where(endswith(daft.col("path"), EPISODE))


@pytest.fixture(scope="module")
def metadata(episodes):
    (row,) = omnisharing.episode_metadata(episodes).to_pylist()
    return row


# ---------------------------------------------------------------------------
# Episode discovery and metadata
# ---------------------------------------------------------------------------


@pytest.mark.integration()
def test_raw_parses_the_real_filename(episodes):
    (row,) = episodes.to_pylist()
    assert row["episode_key"] == "1203_213135_115_110092"
    assert row["stage"] == "DF-2"
    assert row["room_id"] == 115
    assert row["personnel_id"] == 110092
    assert 4e8 < row["size"] < 5e8


@pytest.mark.integration()
def test_episode_metadata_reads_the_undocumented_meta_group(metadata):
    # vendor and the task labels live in a group absent from the published layout.
    assert metadata["vendor"] == "paxini"
    assert metadata["n_frames"] == N_FRAMES
    assert metadata["audio_samplerate"] == 8000
    assert metadata["instruction"]
    # The instruction is Chinese free text.
    assert any("\u4e00" <= char <= "\u9fff" for char in metadata["instruction"])


# ---------------------------------------------------------------------------
# Trajectory
# ---------------------------------------------------------------------------


@pytest.mark.integration()
def test_handpose_quaternions_are_unit_norm(episodes):
    (row,) = omnisharing.trajectory(episodes, fields=["observation.lefthand.handpose"], include_attrs=False).to_pylist()
    pose = np.asarray(row["observation.lefthand.handpose"])
    assert pose.shape == (N_FRAMES, 7)

    # Independent evidence for the [x, y, z, qw, qx, qy, qz] layout: slicing the
    # last four elements must yield unit quaternions. A wrong split would not.
    norms = np.linalg.norm(pose[:, 3:7], axis=1)
    np.testing.assert_allclose(norms, 1.0, atol=1e-3)


@pytest.mark.integration()
def test_trajectory_shapes_match_the_published_layout(episodes):
    (row,) = omnisharing.trajectory(
        episodes,
        fields=["observation.lefthand.joints", "observation.righthand.joints"],
        include_attrs=False,
    ).to_pylist()
    left = np.asarray(row["observation.lefthand.joints"])
    right = np.asarray(row["observation.righthand.joints"])

    assert left.shape == (N_FRAMES, 29)
    assert left.dtype == np.float32
    assert np.isfinite(left).all()
    assert not np.array_equal(left, right)


# ---------------------------------------------------------------------------
# Tactile
# ---------------------------------------------------------------------------


@pytest.mark.integration()
def test_tactile_sensor_widths_sum_to_the_stored_vector(episodes):
    (row,) = omnisharing.tactile(episodes, sides="lefthand", split_by_sensor=True).to_pylist()
    names = row["lefthand.tactile.sensor_names"]
    lengths = row["lefthand.tactile.sensor_lengths"]

    assert len(names) == 15
    assert sum(lengths) == 3465

    widths = [np.asarray(row[f"lefthand.tactile.{name}"]).shape[1] for name in names]
    assert widths == lengths
    assert all(np.asarray(row[f"lefthand.tactile.{name}"]).shape[0] == N_FRAMES for name in names)
    # Real contact data, not zero padding.
    assert sum(float(np.abs(np.asarray(row[f"lefthand.tactile.{n}"])).sum()) for n in names) > 0


# ---------------------------------------------------------------------------
# Audio
# ---------------------------------------------------------------------------


@pytest.mark.integration()
def test_audio_sample_count_agrees_with_metadata(episodes, metadata):
    (row,) = omnisharing.audio(episodes).to_pylist()
    waveform = np.asarray(row["waveform"])

    # Two independent code paths must agree on the length.
    assert row["n_samples"] == metadata["audio_samples"]
    assert row["samplerate"] == 8000
    # Raw PCM, so float64 with a channel axis rather than decoded bytes.
    assert waveform.dtype == np.float64
    assert waveform.ndim == 2
    assert np.isfinite(waveform).all()


@pytest.mark.integration()
def test_audio_max_seconds_truncates_the_real_stream(episodes):
    (full,) = omnisharing.audio(episodes).to_pylist()
    (clipped,) = omnisharing.audio(episodes, max_seconds=0.5).to_pylist()

    assert clipped["n_samples"] == 4000
    assert clipped["n_samples"] < full["n_samples"]
    assert clipped["samplerate"] == full["samplerate"]


# ---------------------------------------------------------------------------
# Video and depth
# ---------------------------------------------------------------------------


@pytest.mark.integration()
def test_camera_frames_decode_to_their_declared_resolution(episodes):
    pytest.importorskip("av", reason="daft[video] extra is required to decode payloads")
    (row,) = omnisharing.camera_frames(episodes, ["RGB_Camera0", ("RGBD_0", "color")], max_frames=1).to_pylist()

    # HEVC Annex-B has no container, Matroska does; both must resolve to the
    # width/height carried in their intrinsics attrs.
    assert row["RGB_Camera0.codec"] == "h26x-annexb"
    assert row["RGBD_0.color.codec"] == "matroska"

    rgb = np.asarray(row["RGB_Camera0.frames"][0])
    rgbd = np.asarray(row["RGBD_0.color.frames"][0])
    assert rgb.shape == (1200, 1920, 3)
    assert rgbd.shape == (720, 1280, 3)
    # Real imagery rather than a flat frame.
    assert float(rgb.std()) > 5
    assert float(rgbd.std()) > 5


@pytest.mark.integration()
def test_depth_frames_are_real_depth_maps(episodes):
    (row,) = omnisharing.depth_frames(episodes, "RGBD_0", frame_indices=[0, 100]).to_pylist()
    depth = np.asarray(row["RGBD_0.depth"])

    assert depth.dtype == np.uint16
    assert depth.shape == (2, 720, 1280)
    assert row["RGBD_0.depth_frame_indices"] == [0, 100]
    # Mostly populated, and the two frames are not identical.
    assert np.count_nonzero(depth[0]) > depth[0].size // 2
    assert not np.array_equal(depth[0], depth[1])


@pytest.mark.integration()
def test_depth_is_absent_on_the_other_rgbd_cameras(episodes):
    # Only RGBD_0 carries aligned_depth in this release.
    (row,) = omnisharing.depth_frames(episodes, ["RGBD_1", "RGBD_2"]).to_pylist()
    assert np.asarray(row["RGBD_1.depth"]).size == 0
    assert np.asarray(row["RGBD_2.depth"]).size == 0


@pytest.mark.integration()
def test_depth_out_of_range_names_the_real_bound(episodes):
    with pytest.raises(IndexError, match=rf"RGBD_0/aligned_depth has {N_FRAMES} frames"):
        omnisharing.depth_frames(episodes, "RGBD_0", frame_indices=[99999])


# ---------------------------------------------------------------------------
# Stereo calibration
# ---------------------------------------------------------------------------


@pytest.mark.integration()
def test_stereo_baseline_is_physically_plausible(episodes):
    (row,) = omnisharing.stereo_extrinsics(episodes, ["RGBD_0", "RGBD_1", "RGBD_2"]).to_pylist()

    matrices = []
    for camera in ("RGBD_0", "RGBD_1", "RGBD_2"):
        matrix = np.asarray(row[f"{camera}.left_to_color"])
        assert matrix.shape == (4, 4)
        assert matrix.dtype == np.float64
        assert row[f"{camera}.calib_date"]
        matrices.append(matrix)

    primary = matrices[0]
    # A rigid transform whose translation is a real stereo separation. Shape
    # alone would not catch a mis-parsed matrix; ~59 mm is checkable against the
    # physical camera.
    np.testing.assert_allclose(primary[:3, :3], np.eye(3), atol=1e-2)
    np.testing.assert_array_equal(primary[3], [0.0, 0.0, 0.0, 1.0])
    assert 0.01 < abs(float(primary[0, 3])) < 0.2

    # Each camera is calibrated separately.
    assert not all(np.array_equal(matrices[0], m) for m in matrices[1:])


# ---------------------------------------------------------------------------
# Frame-level alignment
# ---------------------------------------------------------------------------


@pytest.mark.integration()
def test_frames_apply_the_action_offset_across_the_whole_episode(episodes):
    fields = ["observation.lefthand.joints", "action.lefthand.joints"]
    (episode_row,) = omnisharing.trajectory(episodes, fields=fields, include_attrs=False).to_pylist()
    raw_action = np.asarray(episode_row["action.lefthand.joints"])

    rows = omnisharing.frames(episodes, fields=fields).sort("frame_index").to_pylist()
    assert len(rows) == N_FRAMES

    for i, row in enumerate(rows):
        expected = raw_action[min(i + 1, N_FRAMES - 1)]
        np.testing.assert_array_equal(np.asarray(row["action.lefthand.joints"]), expected)

    # The shift is genuinely applied rather than a no-op.
    assert not np.array_equal(np.asarray(rows[0]["action.lefthand.joints"]), raw_action[0])


@pytest.mark.integration()
def test_rgb_alignment_is_not_frame_index_identity(episodes):
    rows = (
        omnisharing.frames(
            episodes,
            fields=["observation.lefthand.joints"],
            align_cameras=["RGB_Camera0", ("RGBD_0", "color")],
        )
        .sort("frame_index")
        .to_pylist()
    )
    rgb_index = [r["RGB_Camera0.frame_index"] for r in rows]
    rgbd_index = [r["RGBD_0.color.frame_index"] for r in rows]

    # RGB_Camera0 started recording before the observation clock, so index-based
    # alignment would offset every frame.
    assert rgb_index != list(range(N_FRAMES))
    assert rgb_index[0] > 0
    assert all(rgb_index[i] <= rgb_index[i + 1] for i in range(len(rgb_index) - 1))

    # RGBD colour shares the observation clock and does align 1:1.
    assert rgbd_index == list(range(N_FRAMES))


@pytest.mark.integration()
def test_rgbd_eyes_align_on_their_own_clocks(episodes):
    rows = (
        omnisharing.frames(
            episodes,
            fields=["observation.lefthand.joints"],
            align_cameras=[("RGBD_0", "left"), ("RGBD_0", "right")],
        )
        .sort("frame_index")
        .to_pylist()
    )
    assert len(rows) == N_FRAMES
    left_index = [r["RGBD_0.left.frame_index"] for r in rows]

    assert all(index is not None for index in left_index)
    assert all(left_index[i] <= left_index[i + 1] for i in range(len(left_index) - 1))
    # This hardware is tightly synchronised, so the residual is sub-frame; the
    # point is that a per-eye clock is consulted at all.
    assert all(abs(r["RGBD_0.left.timestamp_delta_us"]) < 1000 for r in rows)
