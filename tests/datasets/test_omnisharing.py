from __future__ import annotations

import json

import numpy as np
import pytest

import daft
from daft import DataType, MediaType
from daft.datasets import omnisharing
from daft.datasets.omnisharing import (
    _EPISODE_FILENAME_RE,
    _normalize_camera_targets,
    _normalize_dataset_root,
    _sniff_codec,
)
from tests.datasets.omnisharing_datagen import (
    JOINT_NAMES,
    N_JOINTS,
    PLAYABLE_FRAMES,
    PLAYABLE_HEIGHT,
    PLAYABLE_WIDTH,
    SENSOR_LENGTHS,
    SENSOR_NAMES,
    STEREO_BASELINE,
    TACTILE_WIDTH,
    episode_filename,
    left_to_color_matrix,
    write_df2_episode,
)

pytest.importorskip("h5py", reason="daft[hdf5] extra is required for OmniSharing tests")

N_FRAMES = 8


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def df2_root(tmp_path):
    """One DF-2 episode with objects, meta and audio."""
    root = tmp_path / "ds"
    write_df2_episode(
        root / "data/part_01" / episode_filename(1203, "213135", 115, 110092),
        n_frames=N_FRAMES,
        n_objects=2,
    )
    return root


@pytest.fixture
def df2_episodes(df2_root):
    return omnisharing.raw(str(df2_root))


@pytest.fixture
def mixed_root(tmp_path):
    """A release spanning all three stages, with a duplicated episode_index."""
    root = tmp_path / "mixed"
    part = root / "data/part_01"
    # Same episode_index 1217, different capture groups.
    write_df2_episode(part / episode_filename(1217, "212953", 93, 110056), n_frames=4)
    write_df2_episode(part / episode_filename(1217, "213630", 115, 110092), n_frames=4)
    # DF-1 (no suffix) and DF-2R (retargeted).
    write_df2_episode(part / episode_filename(300, "101302", 138, 120105, None), n_frames=4)
    write_df2_episode(
        part / episode_filename(301, "101414", 138, 120105, "dh13"),
        n_frames=4,
        tactile_width=3750,
        n_joints=17,
    )
    # Not an episode: must be ignored.
    write_df2_episode(part / "quality_report.hdf5", n_frames=4)
    return root


# ---------------------------------------------------------------------------
# Path + filename parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("paxini/Omnisharing_DB_SampleData", "hf://datasets/paxini/Omnisharing_DB_SampleData"),
        ("hf://datasets/paxini/X/", "hf://datasets/paxini/X"),
        ("s3://bucket/key/", "s3://bucket/key"),
        ("/data/omni/", "/data/omni"),
        ("./omni/", "./omni"),
    ],
)
def test_normalize_dataset_root(uri, expected):
    assert _normalize_dataset_root(uri) == expected


def test_normalize_dataset_root_rejects_empty():
    with pytest.raises(ValueError, match="non-empty"):
        _normalize_dataset_root("   ")


@pytest.mark.parametrize("extension", ["hdf5", "h5"])
def test_episode_filename_regex_matches_both_extensions(extension):
    import re

    assert re.search(_EPISODE_FILENAME_RE, f"episode_1_212953_93_110056_glove.{extension}")


# ---------------------------------------------------------------------------
# raw()
# ---------------------------------------------------------------------------


def test_raw_schema_order_and_dtypes(df2_episodes):
    assert [f.name for f in df2_episodes.schema()] == [
        "episode_key",
        "episode_index",
        "capture_time",
        "room_id",
        "personnel_id",
        "stage",
        "hand_model",
        "episode",
        "path",
        "size",
    ]
    dtypes = {f.name: f.dtype for f in df2_episodes.schema()}
    assert dtypes["episode"] == DataType.file(MediaType.hdf5())
    assert dtypes["episode_index"] == DataType.int64()
    assert dtypes["room_id"] == DataType.int64()
    assert dtypes["personnel_id"] == DataType.int64()


def test_raw_parses_filename_metadata(df2_episodes):
    (row,) = df2_episodes.to_pylist()
    assert row["episode_key"] == "1203_213135_115_110092"
    assert row["episode_index"] == 1203
    assert row["capture_time"] == "213135"
    assert row["room_id"] == 115
    assert row["personnel_id"] == 110092
    assert row["stage"] == "DF-2"
    assert row["hand_model"] is None
    assert row["size"] > 0


def test_raw_discovers_h5_extension(tmp_path):
    root = tmp_path / "ds"
    write_df2_episode(root / "episode_1_212953_93_110056_glove.h5", n_frames=4)
    assert omnisharing.raw(str(root)).count_rows() == 1


def test_raw_ignores_non_episode_files(mixed_root):
    keys = {r["episode_key"] for r in omnisharing.raw(str(mixed_root)).to_pylist()}
    assert len(keys) == 4


def test_raw_episode_key_disambiguates_duplicate_episode_index(mixed_root):
    rows = omnisharing.raw(str(mixed_root)).to_pylist()
    # episode_index alone is ambiguous; the composite key is not.
    assert len({r["episode_index"] for r in rows}) == 3
    assert len({r["episode_key"] for r in rows}) == 4
    assert {"1217_212953_93_110056", "1217_213630_115_110092"} <= {r["episode_key"] for r in rows}


@pytest.mark.parametrize(
    ("stage", "expected_count", "expected_hand_model"),
    [("DF-1", 1, None), ("DF-2", 2, None), ("DF-2R", 1, "dh13")],
)
def test_raw_stage_filtering(mixed_root, stage, expected_count, expected_hand_model):
    rows = omnisharing.raw(str(mixed_root), stage=stage).to_pylist()
    assert len(rows) == expected_count
    assert all(r["stage"] == stage for r in rows)
    assert all(r["hand_model"] == expected_hand_model for r in rows)


def test_raw_rejects_unknown_stage(df2_root):
    with pytest.raises(ValueError, match="DF-9"):
        omnisharing.raw(str(df2_root), stage="DF-9")


# ---------------------------------------------------------------------------
# describe()
# ---------------------------------------------------------------------------


def test_describe_reports_shapes_dtypes_and_attrs(df2_episodes):
    layout = omnisharing.describe(df2_episodes)
    assert [f.name for f in layout.schema()] == [
        "episode_key",
        "h5path",
        "kind",
        "shape",
        "dtype",
        "attrs",
    ]

    by_path = {r["h5path"]: r for r in layout.to_pylist()}
    tactile = by_path["dataset/observation/lefthand/tactile/data"]
    assert tactile["kind"] == "dataset"
    assert tactile["shape"] == [N_FRAMES, TACTILE_WIDTH]
    assert tactile["dtype"] == "float32"

    attrs = json.loads(tactile["attrs"])
    assert attrs["sensor_names"] == SENSOR_NAMES
    assert sum(attrs["sensor_lengths"]) == TACTILE_WIDTH

    joints = json.loads(by_path["dataset/observation/lefthand/joints/data"]["attrs"])
    assert joints["joint_names"] == JOINT_NAMES


def test_describe_surfaces_non_contiguous_camera_ids(df2_episodes):
    paths = {r["h5path"] for r in omnisharing.describe(df2_episodes).to_pylist()}
    cameras = {p.split("/")[-1] for p in paths if p.count("/") == 3 and "/image/" in p}
    assert "RGB_Camera4" in cameras
    assert "RGB_Camera3" not in cameras
    assert {"RGBD_0", "RGBD_1"} <= cameras


# ---------------------------------------------------------------------------
# episode_metadata()
# ---------------------------------------------------------------------------


def test_episode_metadata_reads_attrs_and_instruction(df2_episodes):
    (row,) = omnisharing.episode_metadata(df2_episodes).to_pylist()
    assert row["generated_time"] == "2025-12-11-21:31:35"
    assert row["data_id"] == "gARdlC4="
    assert row["vendor"] == "paxini"
    assert row["instruction"] == "将燕京啤酒放入红色啤酒架。"
    assert row["audio_samplerate"] == 8000
    assert row["n_frames"] == N_FRAMES
    assert row["checked_cam_name"] == "Camera4"
    assert row["object_names"] == ["obj1", "obj2"]
    # Identity columns survive.
    assert row["episode_key"] == "1203_213135_115_110092"


def test_episode_metadata_task_labels_roundtrip_unicode_keys(df2_episodes):
    (row,) = omnisharing.episode_metadata(df2_episodes).to_pylist()
    labels = json.loads(row["task_labels"])
    assert labels["任务物品"] == ["燕京啤酒+红色啤酒架"]
    # vendor is promoted to its own column, not left in the free-form labels.
    assert "vendor" not in labels


def test_episode_metadata_tolerates_missing_meta_audio_and_objects(tmp_path):
    root = tmp_path / "bare"
    write_df2_episode(
        root / episode_filename(1, "212953", 93, 110056),
        n_frames=4,
        n_objects=0,
        include_meta=False,
        include_audio=False,
    )
    (row,) = omnisharing.episode_metadata(omnisharing.raw(str(root))).to_pylist()
    assert row["vendor"] is None
    assert row["instruction"] is None
    assert row["audio_samplerate"] is None
    assert row["object_names"] == []


# ---------------------------------------------------------------------------
# trajectory()
# ---------------------------------------------------------------------------


def test_trajectory_default_fields_and_shapes(df2_episodes):
    (row,) = omnisharing.trajectory(df2_episodes).to_pylist()
    for side in ("lefthand", "righthand"):
        for branch in ("observation", "action"):
            assert np.asarray(row[f"{branch}.{side}.joints"]).shape == (N_FRAMES, N_JOINTS)
            assert np.asarray(row[f"{branch}.{side}.handpose"]).shape == (N_FRAMES, 7)


def test_trajectory_tensor_dtype(df2_episodes):
    traj = omnisharing.trajectory(df2_episodes, fields=["observation.lefthand.joints"])
    dtypes = {f.name: f.dtype for f in traj.schema()}
    assert dtypes["observation.lefthand.joints"] == DataType.tensor(DataType.float32())
    assert np.asarray(traj.to_pylist()[0]["observation.lefthand.joints"]).dtype == np.float32


def test_trajectory_emits_layout_attrs(df2_episodes):
    (row,) = omnisharing.trajectory(df2_episodes).to_pylist()
    assert row["observation.lefthand.joints.joint_names"] == JOINT_NAMES
    # Quaternion is qw-first, which is the opposite of scipy's convention.
    assert row["handpose_order"] == "[x, y, z, qw, qx, qy, qz]"
    assert "exoskeleton" in row["lefthand.description"]


def test_trajectory_include_attrs_false_omits_metadata_columns(df2_episodes):
    traj = omnisharing.trajectory(df2_episodes, fields=["observation.lefthand.joints"], include_attrs=False)
    assert "observation.lefthand.joints" in traj.column_names
    assert "observation.lefthand.joints.joint_names" not in traj.column_names
    assert "handpose_order" not in traj.column_names


def test_trajectory_description_is_branch_order_independent(df2_episodes):
    row = omnisharing.trajectory(
        df2_episodes, fields=["action.lefthand.joints", "observation.righthand.joints"]
    ).to_pylist()[0]
    assert "left" in row["lefthand.description"]
    assert "right" in row["righthand.description"]


@pytest.mark.parametrize("fields", [[], ["nope"], ["observation.lefthand.tactile"], ["observation.thirdhand.joints"]])
def test_trajectory_rejects_invalid_fields(df2_episodes, fields):
    with pytest.raises(ValueError):
        omnisharing.trajectory(df2_episodes, fields=fields)


def test_trajectory_handles_df2r_widths(mixed_root):
    episodes = omnisharing.raw(str(mixed_root), stage="DF-2R")
    (row,) = omnisharing.trajectory(episodes, fields=["observation.lefthand.joints"]).to_pylist()
    # DF-2R retargets to 17 joints; nothing may be hardcoded to 29.
    assert np.asarray(row["observation.lefthand.joints"]).shape == (4, 17)
    assert len(row["observation.lefthand.joints.joint_names"]) == 17


# ---------------------------------------------------------------------------
# tactile()
# ---------------------------------------------------------------------------


def test_tactile_returns_flat_vector_and_layout(df2_episodes):
    (row,) = omnisharing.tactile(df2_episodes).to_pylist()
    for side in ("lefthand", "righthand"):
        assert np.asarray(row[f"{side}.tactile"]).shape == (N_FRAMES, TACTILE_WIDTH)
        assert row[f"{side}.tactile.sensor_names"] == SENSOR_NAMES
        assert row[f"{side}.tactile.sensor_lengths"] == SENSOR_LENGTHS


def test_tactile_split_by_sensor_widths_match_declared_lengths(df2_episodes):
    split = omnisharing.tactile(df2_episodes, sides="lefthand", split_by_sensor=True)
    # The flat column is replaced by one column per sensor.
    assert "lefthand.tactile" not in split.column_names
    assert not any(c.startswith("righthand") for c in split.column_names)

    (row,) = split.to_pylist()
    widths = [np.asarray(row[f"lefthand.tactile.{name}"]).shape[1] for name in SENSOR_NAMES]
    assert widths == SENSOR_LENGTHS
    assert sum(widths) == TACTILE_WIDTH


def test_tactile_split_slices_match_flat_vector(df2_episodes):
    (flat_row,) = omnisharing.tactile(df2_episodes, sides="lefthand").to_pylist()
    (split_row,) = omnisharing.tactile(df2_episodes, sides="lefthand", split_by_sensor=True).to_pylist()

    flat = np.asarray(flat_row["lefthand.tactile"])
    offset = 0
    for name, length in zip(SENSOR_NAMES, SENSOR_LENGTHS):
        piece = np.asarray(split_row[f"lefthand.tactile.{name}"])
        np.testing.assert_array_equal(piece, flat[:, offset : offset + length])
        offset += length


def test_tactile_split_handles_df2r_width(mixed_root):
    episodes = omnisharing.raw(str(mixed_root), stage="DF-2R")
    (row,) = omnisharing.tactile(episodes, sides="lefthand", split_by_sensor=True).to_pylist()
    total = sum(np.asarray(row[f"lefthand.tactile.{name}"]).shape[1] for name in SENSOR_NAMES)
    assert total == 3750


@pytest.mark.parametrize("sides", [[], ["thirdhand"]])
def test_tactile_rejects_invalid_sides(df2_episodes, sides):
    with pytest.raises(ValueError):
        omnisharing.tactile(df2_episodes, sides=sides)


# ---------------------------------------------------------------------------
# audio()
# ---------------------------------------------------------------------------


def test_audio_returns_float64_waveform(df2_episodes):
    df = omnisharing.audio(df2_episodes)
    dtypes = {f.name: f.dtype for f in df.schema()}
    assert dtypes["waveform"] == DataType.tensor(DataType.float64())
    assert dtypes["samplerate"] == DataType.int64()

    (row,) = df.to_pylist()
    waveform = np.asarray(row["waveform"])
    # Stored as raw PCM with a channel axis, not as a compressed stream.
    assert waveform.ndim == 2
    assert waveform.dtype == np.float64
    assert row["samplerate"] == 8000
    assert row["n_samples"] == waveform.shape[0]
    assert np.abs(waveform).sum() > 0


def test_audio_mono_averages_channels(df2_episodes):
    (stereo,) = omnisharing.audio(df2_episodes).to_pylist()
    (mono,) = omnisharing.audio(df2_episodes, mono=True).to_pylist()

    stereo_waveform = np.asarray(stereo["waveform"])
    mono_waveform = np.asarray(mono["waveform"])
    assert mono_waveform.ndim == 1
    assert mono_waveform.shape[0] == stereo_waveform.shape[0]
    np.testing.assert_allclose(mono_waveform, stereo_waveform.mean(axis=1))
    assert mono["n_samples"] == stereo["n_samples"]


def test_audio_mono_downmix_is_exact(tmp_path):
    root = tmp_path / "stereo"
    path = root / episode_filename(1, "212953", 93, 110056)
    write_df2_episode(path, n_frames=4)

    import h5py

    with h5py.File(path, "a") as h5:
        samplerate = h5["dataset/observation/audio"].attrs["samplerate"]
        del h5["dataset/observation/audio"]
        channels = np.stack([np.ones(50), np.full(50, 3.0)], axis=1)
        dataset = h5["dataset/observation"].create_dataset("audio", data=channels)
        dataset.attrs["samplerate"] = samplerate

    (row,) = omnisharing.audio(omnisharing.raw(str(root)), mono=True).to_pylist()
    waveform = np.asarray(row["waveform"])
    assert waveform.shape == (50,)
    np.testing.assert_allclose(waveform, 2.0)


def test_audio_mono_handles_waveform_without_channel_axis(tmp_path):
    root = tmp_path / "flat"
    path = root / episode_filename(1, "212953", 93, 110056)
    write_df2_episode(path, n_frames=4)

    import h5py

    with h5py.File(path, "a") as h5:
        samplerate = h5["dataset/observation/audio"].attrs["samplerate"]
        del h5["dataset/observation/audio"]
        dataset = h5["dataset/observation"].create_dataset("audio", data=np.linspace(-1, 1, 100, dtype="float64"))
        dataset.attrs["samplerate"] = samplerate

    (row,) = omnisharing.audio(omnisharing.raw(str(root)), mono=True).to_pylist()
    assert np.asarray(row["waveform"]).shape == (100,)


def test_audio_max_seconds_truncates_samples_only(df2_episodes):
    (full,) = omnisharing.audio(df2_episodes).to_pylist()
    total = np.asarray(full["waveform"]).shape[0]
    cut_seconds = (total // 2) / full["samplerate"]
    expected = int(cut_seconds * full["samplerate"])

    (clipped,) = omnisharing.audio(df2_episodes, max_seconds=cut_seconds).to_pylist()
    waveform = np.asarray(clipped["waveform"])
    assert waveform.shape[0] == expected < total
    assert clipped["n_samples"] == expected
    # Truncating must not rescale time.
    assert clipped["samplerate"] == full["samplerate"]
    np.testing.assert_array_equal(waveform, np.asarray(full["waveform"])[:expected])


def test_audio_max_seconds_beyond_duration_returns_everything(df2_episodes):
    (full,) = omnisharing.audio(df2_episodes).to_pylist()
    (asked_for_more,) = omnisharing.audio(df2_episodes, max_seconds=1e6).to_pylist()
    assert asked_for_more["n_samples"] == full["n_samples"]


def test_audio_max_seconds_clamps_to_at_least_one_sample(df2_episodes):
    (row,) = omnisharing.audio(df2_episodes, max_seconds=1e-9).to_pylist()
    assert row["n_samples"] == 1


@pytest.mark.parametrize("max_seconds", [0, -1, -0.5])
def test_audio_rejects_non_positive_max_seconds(df2_episodes, max_seconds):
    with pytest.raises(ValueError, match="must be positive"):
        omnisharing.audio(df2_episodes, max_seconds=max_seconds)


def test_audio_missing_dataset_reads_cleanly(tmp_path):
    root = tmp_path / "silent"
    write_df2_episode(root / episode_filename(1, "212953", 93, 110056), n_frames=4, include_audio=False)
    # An all-null tensor column trips a Daft concat error, so absent audio is
    # reported as an empty waveform rather than null.
    (row,) = omnisharing.audio(omnisharing.raw(str(root))).to_pylist()
    assert np.asarray(row["waveform"]).size == 0
    assert row["samplerate"] is None
    assert row["n_samples"] is None


def test_audio_mixed_release_reads_both_episodes(tmp_path):
    root = tmp_path / "mixed"
    write_df2_episode(root / episode_filename(1, "212953", 93, 110056), n_frames=4)
    write_df2_episode(root / episode_filename(2, "213630", 115, 110092), n_frames=4, include_audio=False)

    rows = omnisharing.audio(omnisharing.raw(str(root))).to_pylist()
    assert len(rows) == 2
    sizes = sorted(np.asarray(r["waveform"]).size for r in rows)
    assert sizes[0] == 0
    assert sizes[1] > 0


def test_audio_zero_length_dataset_reports_zero_samples(tmp_path):
    root = tmp_path / "zero"
    path = root / episode_filename(1, "212953", 93, 110056)
    write_df2_episode(path, n_frames=4)

    import h5py

    with h5py.File(path, "a") as h5:
        del h5["dataset/observation/audio"]
        dataset = h5["dataset/observation"].create_dataset("audio", data=np.empty((0, 1), dtype="float64"))
        dataset.attrs["samplerate"] = 8000

    (row,) = omnisharing.audio(omnisharing.raw(str(root))).to_pylist()
    assert row["n_samples"] == 0
    assert row["samplerate"] == 8000


def test_audio_without_samplerate_ignores_max_seconds(tmp_path):
    root = tmp_path / "nosr"
    path = root / episode_filename(1, "212953", 93, 110056)
    write_df2_episode(path, n_frames=4)

    import h5py

    with h5py.File(path, "a") as h5:
        del h5["dataset/observation/audio"].attrs["samplerate"]

    (full,) = omnisharing.audio(omnisharing.raw(str(root))).to_pylist()
    (clipped,) = omnisharing.audio(omnisharing.raw(str(root)), max_seconds=0.01).to_pylist()
    # Without a samplerate the sample count cannot be derived, so the limit is
    # ignored rather than guessed at.
    assert clipped["samplerate"] is None
    assert clipped["n_samples"] == full["n_samples"]


def test_audio_keeps_episode_handle_for_chaining(df2_episodes):
    df = omnisharing.audio(df2_episodes)
    assert "episode" in df.column_names
    assert "episode_key" in df.column_names
    # Episode-level readers compose; only frames() drops the handle.
    chained = omnisharing.tactile(df, sides="lefthand")
    assert "waveform" in chained.column_names
    assert "lefthand.tactile" in chained.column_names


# ---------------------------------------------------------------------------
# objects()
# ---------------------------------------------------------------------------


def test_objects_probes_max_slots_and_null_fills(tmp_path):
    root = tmp_path / "objs"
    write_df2_episode(root / episode_filename(1, "212953", 93, 110056), n_frames=4, n_objects=2)
    write_df2_episode(root / episode_filename(2, "213630", 115, 110092), n_frames=4, n_objects=0)
    write_df2_episode(root / episode_filename(3, "214000", 115, 110092), n_frames=4, n_objects=4)

    objs = omnisharing.objects(omnisharing.raw(str(root))).sort("episode_key")
    # Schema is fixed to the widest episode.
    assert "obj4" in objs.column_names
    assert "obj5" not in objs.column_names

    two, none, four = objs.to_pylist()
    assert [two["n_objects"], none["n_objects"], four["n_objects"]] == [2, 0, 4]
    assert np.asarray(two["obj1"]).shape == (4, omnisharing.OBJECT_POSE_WIDTH)
    assert two["obj1.name"] == "object_1"
    assert two["obj1.id"] == 101
    # Slots beyond an episode's object count are null, not zero-filled.
    assert two["obj3"] is None
    assert none["obj1"] is None
    assert four["obj4.id"] == 104


def test_objects_without_any_objects_emits_no_object_columns(tmp_path):
    root = tmp_path / "noobj"
    write_df2_episode(root / episode_filename(1, "212953", 93, 110056), n_frames=4, n_objects=0)
    objs = omnisharing.objects(omnisharing.raw(str(root)))
    assert "n_objects" in objs.column_names
    assert not any(c.startswith("obj") for c in objs.column_names)


def test_objects_max_objects_caps_columns_but_not_count(tmp_path):
    root = tmp_path / "cap"
    write_df2_episode(root / episode_filename(1, "212953", 93, 110056), n_frames=4, n_objects=4)
    objs = omnisharing.objects(omnisharing.raw(str(root)), max_objects=2)
    assert "obj2" in objs.column_names
    assert "obj3" not in objs.column_names
    # The true count is still reported even though fewer columns are emitted.
    assert objs.to_pylist()[0]["n_objects"] == 4


def test_objects_rejects_negative_max_objects(df2_episodes):
    with pytest.raises(ValueError, match="non-negative"):
        omnisharing.objects(df2_episodes, max_objects=-1)


# ---------------------------------------------------------------------------
# cameras() + codec sniffing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("head", "expected"),
    [
        (bytes([0x00, 0x00, 0x01, 0x40, 0x01, 0x0C]), "h26x-annexb"),
        (bytes([0x00, 0x00, 0x00, 0x01, 0x40, 0x01]), "h26x-annexb"),
        (bytes([0x1A, 0x45, 0xDF, 0xA3]), "matroska"),
        (bytes([0xFF, 0xD8, 0xFF, 0xE0]), "jpeg"),
        (b"\x89PNG\r\n\x1a\n", "png"),
        (b"RIFF\x00\x00\x00\x00WAVE", "riff"),
        (b"\x00\x00\x00\x18ftypisom", "mp4"),
        (b"definitely-not-a-codec", "unknown"),
    ],
)
def test_sniff_codec(head, expected):
    assert _sniff_codec(head) == expected


def test_cameras_reports_one_row_per_stream(df2_episodes):
    rows = omnisharing.cameras(df2_episodes).to_pylist()
    rgb = [r for r in rows if r["kind"] == "rgb"]
    rgbd = [r for r in rows if r["kind"] == "rgbd"]
    # 5 RGB cameras with one stream each, 2 RGBD cameras with three each.
    assert len(rgb) == 5
    assert len(rgbd) == 6
    assert sorted({r["stream"] for r in rgbd}) == ["color", "left", "right"]
    assert all(r["stream"] == "" for r in rgb)


def test_cameras_detects_per_family_codecs(df2_episodes):
    rows = omnisharing.cameras(df2_episodes).to_pylist()
    assert all(r["codec"] == "h26x-annexb" for r in rows if r["kind"] == "rgb")
    assert all(r["codec"] == "matroska" for r in rows if r["kind"] == "rgbd")


def test_cameras_reports_both_reference_frames(df2_episodes):
    rows = omnisharing.cameras(df2_episodes).to_pylist()
    rgb = next(r for r in rows if r["kind"] == "rgb")
    color = next(r for r in rows if r["kind"] == "rgbd" and r["stream"] == "color")

    assert rgb["relative_to_who"] == "RGB_Camera6"
    assert rgb["width"] == 1920
    assert rgb["height"] == 1200
    assert np.asarray(rgb["intrinsics"]).shape == (3, 3)
    assert np.asarray(rgb["extrinsics"]).shape == (4, 4)

    assert color["relative_to_who"] == "RGBD_0"
    assert color["width"] == 1280
    assert color["height"] == 720
    assert json.loads(color["inner_extrinsic"])["calib_date"] == "20250925171218"


def test_cameras_rgbd_substreams_inherit_camera_extrinsics(df2_episodes):
    rows = omnisharing.cameras(df2_episodes).to_pylist()
    left = next(r for r in rows if r["kind"] == "rgbd" and r["stream"] == "left")
    # left/right carry no extrinsics of their own; they share the camera pose.
    assert np.asarray(left["extrinsics"]).shape == (4, 4)


def test_cameras_stream_frame_counts_differ_from_observation(df2_episodes):
    rows = omnisharing.cameras(df2_episodes).to_pylist()
    rgb_counts = {r["camera"]: r["n_timestamps"] for r in rows if r["kind"] == "rgb"}
    # This is why frame-index alignment is wrong for RGB cameras.
    assert all(count > N_FRAMES for count in rgb_counts.values())
    assert len(set(rgb_counts.values())) > 1
    # RGBD colour tracks the observation clock.
    colour = [r for r in rows if r["kind"] == "rgbd" and r["stream"] == "color"]
    assert colour
    assert all(r["n_timestamps"] == N_FRAMES for r in colour)


def test_cameras_reports_per_eye_clocks(df2_episodes):
    rows = omnisharing.cameras(df2_episodes).to_pylist()
    by_stream = {r["stream"]: r["n_timestamps"] for r in rows if r["camera"] == "RGBD_0" and r["kind"] == "rgbd"}
    # RGBD carries left_timestamp and right_timestamp alongside timestamp, so a
    # per-eye count must come from that eye's own clock rather than the colour
    # one. The generated episode gives the left eye an extra sample to prove it.
    assert by_stream["color"] == N_FRAMES
    assert by_stream["right"] == N_FRAMES
    assert by_stream["left"] == N_FRAMES + 1


def test_cameras_flags_checked_camera(df2_episodes):
    flagged = [r["camera"] for r in omnisharing.cameras(df2_episodes).to_pylist() if r["is_checked_camera"]]
    assert flagged
    assert all("Camera4" in name for name in flagged)


# ---------------------------------------------------------------------------
# camera selector normalization
# ---------------------------------------------------------------------------


def test_normalize_camera_targets_expands_bare_rgbd():
    assert _normalize_camera_targets(["RGBD_0"]) == (
        ("RGBD_0", "color"),
        ("RGBD_0", "left"),
        ("RGBD_0", "right"),
    )


@pytest.mark.parametrize(
    ("selector", "expected"),
    [
        (["RGB_Camera0"], (("RGB_Camera0", ""),)),
        ("RGB_Camera2", (("RGB_Camera2", ""),)),
        ([("RGBD_1", "left")], (("RGBD_1", "left"),)),
        (["RGB_Camera0", "RGB_Camera0"], (("RGB_Camera0", ""),)),
    ],
)
def test_normalize_camera_targets(selector, expected):
    assert _normalize_camera_targets(selector) == expected


@pytest.mark.parametrize("selector", [[], [("RGBD_0", "depth")], [("a", "b", "c")]])
def test_normalize_camera_targets_rejects_invalid(selector):
    with pytest.raises(ValueError):
        _normalize_camera_targets(selector)


# ---------------------------------------------------------------------------
# camera_payloads() / camera_frames()
# ---------------------------------------------------------------------------


def test_camera_payloads_returns_bytes_and_codec(df2_episodes):
    payloads = omnisharing.camera_payloads(df2_episodes, ["RGB_Camera0", ("RGBD_0", "color")])
    dtypes = {f.name: f.dtype for f in payloads.schema()}
    assert dtypes["RGB_Camera0.payload"] == DataType.binary()

    (row,) = payloads.to_pylist()
    assert isinstance(row["RGB_Camera0.payload"], bytes)
    assert row["RGB_Camera0.codec"] == "h26x-annexb"
    assert row["RGBD_0.color.codec"] == "matroska"
    assert row["RGBD_0.color.payload"][:4] == bytes([0x1A, 0x45, 0xDF, 0xA3])


def test_camera_payloads_absent_stream_is_null(df2_episodes):
    (row,) = omnisharing.camera_payloads(df2_episodes, ["RGB_Camera99"]).to_pylist()
    assert row["RGB_Camera99.payload"] is None
    assert row["RGB_Camera99.codec"] is None


@pytest.fixture
def playable_episodes(tmp_path):
    """An episode whose camera payloads are real, locally-encoded video."""
    pytest.importorskip("av", reason="daft[video] extra is required to decode payloads")
    root = tmp_path / "playable"
    write_df2_episode(root / episode_filename(1, "212953", 93, 110056), n_frames=N_FRAMES, playable=True)
    return omnisharing.raw(str(root))


def test_camera_frames_decodes_hevc(playable_episodes):
    frames = omnisharing.camera_frames(playable_episodes, ["RGB_Camera0"], max_frames=2)
    dtypes = {f.name: f.dtype for f in frames.schema()}
    assert dtypes["RGB_Camera0.frames"] == DataType.list(DataType.image())

    (row,) = frames.to_pylist()
    assert row["RGB_Camera0.codec"] == "h26x-annexb"
    assert len(row["RGB_Camera0.frames"]) == 2
    first = np.asarray(row["RGB_Camera0.frames"][0])
    assert first.shape == (PLAYABLE_HEIGHT, PLAYABLE_WIDTH, 3)
    assert first.dtype == np.uint8


def test_camera_frames_decodes_matroska(playable_episodes):
    (row,) = omnisharing.camera_frames(playable_episodes, [("RGBD_0", "color")], max_frames=2).to_pylist()
    assert row["RGBD_0.color.codec"] == "matroska"
    assert len(row["RGBD_0.color.frames"]) == 2


def test_camera_frames_defaults_to_single_frame(playable_episodes):
    (row,) = omnisharing.camera_frames(playable_episodes, ["RGB_Camera0"]).to_pylist()
    assert len(row["RGB_Camera0.frames"]) == 1


def test_camera_frames_max_frames_none_decodes_all(playable_episodes):
    (row,) = omnisharing.camera_frames(playable_episodes, ["RGB_Camera0"], max_frames=None).to_pylist()
    assert len(row["RGB_Camera0.frames"]) == PLAYABLE_FRAMES


def test_camera_frames_resizes(playable_episodes):
    (row,) = omnisharing.camera_frames(
        playable_episodes, ["RGB_Camera0"], max_frames=1, width=32, height=24
    ).to_pylist()
    assert np.asarray(row["RGB_Camera0.frames"][0]).shape == (24, 32, 3)


@pytest.mark.parametrize("kwargs", [{"width": 10}, {"height": 10}, {"sample_every": 0}, {"max_frames": 0}])
def test_camera_frames_rejects_invalid_arguments(playable_episodes, kwargs):
    with pytest.raises(ValueError):
        omnisharing.camera_frames(playable_episodes, ["RGB_Camera0"], **kwargs)


def test_camera_payloads_still_works_for_undecodable_payload(df2_episodes):
    # df2_episodes has placeholder payloads: sniffing works, decoding would not.
    (row,) = omnisharing.camera_payloads(df2_episodes, ["RGB_Camera0"]).to_pylist()
    assert len(row["RGB_Camera0.payload"]) > 0
    assert row["RGB_Camera0.codec"] == "h26x-annexb"


# ---------------------------------------------------------------------------
# depth_frames()
# ---------------------------------------------------------------------------


@pytest.fixture
def depth_truth(df2_root):
    """The stored aligned_depth array, read directly with h5py."""
    import h5py

    (path,) = df2_root.rglob("*.hdf5")
    with h5py.File(path, "r") as h5:
        return np.asarray(h5["dataset/observation/image/RGBD_0/aligned_depth"][()])


def test_depth_frames_returns_uint16_tensor(df2_episodes, depth_truth):
    df = omnisharing.depth_frames(df2_episodes, "RGBD_0")
    dtypes = {f.name: f.dtype for f in df.schema()}
    assert dtypes["RGBD_0.depth"] == DataType.tensor(DataType.uint16())

    (row,) = df.to_pylist()
    depth = np.asarray(row["RGBD_0.depth"])
    assert depth.ndim == 3
    assert depth.dtype == np.uint16
    assert depth.shape[1:] == depth_truth.shape[1:]


def test_depth_frames_defaults_to_first_frame_only(df2_episodes, depth_truth):
    (row,) = omnisharing.depth_frames(df2_episodes, "RGBD_0").to_pylist()
    depth = np.asarray(row["RGBD_0.depth"])
    # A whole episode is ~380 MB per camera, so reading everything is a hazard.
    assert depth.shape[0] == 1 < depth_truth.shape[0]
    np.testing.assert_array_equal(depth[0], depth_truth[0])
    assert row["RGBD_0.depth_frame_indices"] == [0]


def test_depth_frames_honours_requested_order(df2_episodes, depth_truth):
    requested = [3, 1, 6]
    (row,) = omnisharing.depth_frames(df2_episodes, "RGBD_0", frame_indices=requested).to_pylist()
    depth = np.asarray(row["RGBD_0.depth"])

    assert depth.shape[0] == len(requested)
    for position, index in enumerate(requested):
        np.testing.assert_array_equal(depth[position], depth_truth[index])
    assert row["RGBD_0.depth_frame_indices"] == requested


def test_depth_frames_accepts_a_single_int(df2_episodes, depth_truth):
    (row,) = omnisharing.depth_frames(df2_episodes, "RGBD_0", frame_indices=2).to_pylist()
    np.testing.assert_array_equal(np.asarray(row["RGBD_0.depth"])[0], depth_truth[2])


def test_depth_frames_reads_last_valid_index(df2_episodes, depth_truth):
    last = depth_truth.shape[0] - 1
    (row,) = omnisharing.depth_frames(df2_episodes, "RGBD_0", frame_indices=[last]).to_pylist()
    np.testing.assert_array_equal(np.asarray(row["RGBD_0.depth"])[0], depth_truth[last])


def test_depth_frames_absent_camera_yields_empty(df2_episodes):
    # RGBD_1 exists but has no aligned_depth, mirroring the real dataset where
    # only RGBD_0 carries depth.
    (row,) = omnisharing.depth_frames(df2_episodes, "RGBD_1").to_pylist()
    assert np.asarray(row["RGBD_1.depth"]).size == 0
    assert row["RGBD_1.depth_frame_indices"] is None


def test_depth_frames_mixes_present_and_absent_cameras(df2_episodes):
    (row,) = omnisharing.depth_frames(df2_episodes, ["RGBD_0", "RGBD_1"]).to_pylist()
    assert np.asarray(row["RGBD_0.depth"]).size > 0
    assert np.asarray(row["RGBD_1.depth"]).size == 0


def test_depth_frames_unknown_camera_yields_empty(df2_episodes):
    (row,) = omnisharing.depth_frames(df2_episodes, "RGBD_99").to_pylist()
    assert np.asarray(row["RGBD_99.depth"]).size == 0


def test_depth_frames_zero_length_depth_yields_empty(tmp_path):
    root = tmp_path / "zero"
    path = root / episode_filename(1, "212953", 93, 110056)
    write_df2_episode(path, n_frames=4)

    import h5py

    with h5py.File(path, "a") as h5:
        del h5["dataset/observation/image/RGBD_0/aligned_depth"]
        h5["dataset/observation/image/RGBD_0"].create_dataset(
            "aligned_depth", data=np.empty((0, 8, 12), dtype=np.uint16)
        )

    # "Present but empty" must behave like "absent", not raise.
    (row,) = omnisharing.depth_frames(omnisharing.raw(str(root)), "RGBD_0").to_pylist()
    assert np.asarray(row["RGBD_0.depth"]).size == 0
    assert row["RGBD_0.depth_frame_indices"] is None


def test_depth_frames_out_of_range_raises_with_camera_and_bound(df2_episodes):
    with pytest.raises(IndexError, match=rf"RGBD_0/aligned_depth has {N_FRAMES} frames"):
        omnisharing.depth_frames(df2_episodes, "RGBD_0", frame_indices=[0, N_FRAMES])


def test_depth_frames_ghost_camera_does_not_mask_a_real_bound(df2_episodes):
    # A camera with unknown depth length must not suppress the real error.
    with pytest.raises(IndexError, match="RGBD_0"):
        omnisharing.depth_frames(df2_episodes, ["RGBD_99", "RGBD_0"], frame_indices=[N_FRAMES + 10])


@pytest.mark.parametrize("frame_indices", [-1, [-1], [0, -2]])
def test_depth_frames_rejects_negative_indices(df2_episodes, frame_indices):
    with pytest.raises(IndexError, match="non-negative"):
        omnisharing.depth_frames(df2_episodes, "RGBD_0", frame_indices=frame_indices)


def test_depth_frames_rejects_empty_arguments(df2_episodes):
    with pytest.raises(ValueError, match="at least one RGBD camera"):
        omnisharing.depth_frames(df2_episodes, [])
    with pytest.raises(ValueError, match="at least one index"):
        omnisharing.depth_frames(df2_episodes, "RGBD_0", frame_indices=[])


def test_depth_frames_shorter_episode_degrades_gracefully(tmp_path):
    root = tmp_path / "uneven"
    write_df2_episode(root / episode_filename(1, "212953", 93, 110056), n_frames=8)
    write_df2_episode(root / episode_filename(2, "213630", 115, 110092), n_frames=3)

    # Bounds are probed from the first episode, so a shorter one must clamp
    # rather than fail mid-execution.
    rows = (
        omnisharing.depth_frames(omnisharing.raw(str(root)), "RGBD_0", frame_indices=[0, 5])
        .sort("episode_key")
        .to_pylist()
    )
    assert [np.asarray(r["RGBD_0.depth"]).shape[0] for r in rows] == [2, 1]
    assert [r["RGBD_0.depth_frame_indices"] for r in rows] == [[0, 5], [0]]


def test_depth_frames_deduplicates_camera_names(df2_episodes):
    df = omnisharing.depth_frames(df2_episodes, ["RGBD_0", "RGBD_0"])
    assert sum(name == "RGBD_0.depth" for name in df.column_names) == 1


def test_depth_frames_documents_the_undocumented_unit():
    doc = omnisharing.depth_frames.__doc__ or ""
    # The dataset ships no attrs on aligned_depth, so no scale can be derived.
    assert "undocumented" in doc


# ---------------------------------------------------------------------------
# stereo_extrinsics()
# ---------------------------------------------------------------------------


def test_stereo_extrinsics_parses_the_transform(df2_episodes):
    df = omnisharing.stereo_extrinsics(df2_episodes, "RGBD_0")
    dtypes = {f.name: f.dtype for f in df.schema()}
    assert dtypes["RGBD_0.left_to_color"] == DataType.tensor(DataType.float64())
    assert dtypes["RGBD_0.calib_date"] == DataType.string()

    (row,) = df.to_pylist()
    matrix = np.asarray(row["RGBD_0.left_to_color"])
    assert matrix.shape == (4, 4)
    assert matrix.dtype == np.float64
    assert row["RGBD_0.calib_date"] == "20250925171218"
    np.testing.assert_array_equal(matrix, np.asarray(left_to_color_matrix(0), dtype=np.float64))


def test_stereo_extrinsics_matches_the_bytes_on_disk(df2_root):
    import h5py

    (path,) = df2_root.rglob("*.hdf5")
    with h5py.File(path, "r") as h5:
        blob = h5["dataset/observation/image/RGBD_0/inner_extrinsic"][()][0]
    on_disk = np.asarray(json.loads(blob.decode())["left_to_color"], dtype=np.float64)

    (row,) = omnisharing.stereo_extrinsics(omnisharing.raw(str(df2_root)), "RGBD_0").to_pylist()
    np.testing.assert_array_equal(np.asarray(row["RGBD_0.left_to_color"]), on_disk)


def test_stereo_extrinsics_transform_is_well_formed(df2_episodes):
    (row,) = omnisharing.stereo_extrinsics(df2_episodes, "RGBD_0").to_pylist()
    matrix = np.asarray(row["RGBD_0.left_to_color"])
    # A rigid transform: near-identity rotation, homogeneous bottom row, and the
    # stereo baseline carried in the x translation.
    np.testing.assert_allclose(matrix[:3, :3], np.eye(3), atol=2e-3)
    np.testing.assert_array_equal(matrix[3], [0.0, 0.0, 0.0, 1.0])
    assert matrix[0, 3] == pytest.approx(STEREO_BASELINE)


def test_stereo_extrinsics_reads_each_camera_separately(df2_episodes):
    (row,) = omnisharing.stereo_extrinsics(df2_episodes, ["RGBD_0", "RGBD_1"]).to_pylist()
    first = np.asarray(row["RGBD_0.left_to_color"])
    second = np.asarray(row["RGBD_1.left_to_color"])
    assert not np.array_equal(first, second)
    np.testing.assert_array_equal(second, np.asarray(left_to_color_matrix(1), dtype=np.float64))


def test_stereo_extrinsics_missing_dataset_yields_empty(tmp_path):
    root = tmp_path / "nocalib"
    path = root / episode_filename(1, "212953", 93, 110056)
    write_df2_episode(path, n_frames=4)

    import h5py

    with h5py.File(path, "a") as h5:
        del h5["dataset/observation/image/RGBD_0/inner_extrinsic"]

    (row,) = omnisharing.stereo_extrinsics(omnisharing.raw(str(root)), "RGBD_0").to_pylist()
    assert np.asarray(row["RGBD_0.left_to_color"]).size == 0
    assert row["RGBD_0.calib_date"] is None


def test_stereo_extrinsics_unknown_camera_yields_empty(df2_episodes):
    (row,) = omnisharing.stereo_extrinsics(df2_episodes, "RGBD_99").to_pylist()
    assert np.asarray(row["RGBD_99.left_to_color"]).size == 0


@pytest.mark.parametrize(
    ("label", "payload"),
    [
        ("not json", b"{{{not json"),
        ("json array", b"[1, 2, 3]"),
        ("no left_to_color", b'{"calib_date": "20250101000000"}'),
        ("wrong shape", b'{"calib_date": "x", "left_to_color": [[1, 2], [3, 4]]}'),
        ("non numeric", b'{"calib_date": "x", "left_to_color": "nope"}'),
    ],
)
def test_stereo_extrinsics_malformed_json_yields_empty(tmp_path, label, payload):
    root = tmp_path / f"bad_{label.replace(' ', '_')}"
    path = root / episode_filename(1, "212953", 93, 110056)
    write_df2_episode(path, n_frames=4)

    import h5py

    with h5py.File(path, "a") as h5:
        del h5["dataset/observation/image/RGBD_0/inner_extrinsic"]
        h5["dataset/observation/image/RGBD_0"].create_dataset("inner_extrinsic", data=np.array([payload]))

    # Calibration is optional metadata; a bad blob must not fail the read.
    (row,) = omnisharing.stereo_extrinsics(omnisharing.raw(str(root)), "RGBD_0").to_pylist()
    assert np.asarray(row["RGBD_0.left_to_color"]).size == 0


def test_stereo_extrinsics_recovers_calib_date_without_a_matrix(tmp_path):
    root = tmp_path / "partial"
    path = root / episode_filename(1, "212953", 93, 110056)
    write_df2_episode(path, n_frames=4)

    import h5py

    with h5py.File(path, "a") as h5:
        del h5["dataset/observation/image/RGBD_0/inner_extrinsic"]
        h5["dataset/observation/image/RGBD_0"].create_dataset(
            "inner_extrinsic", data=np.array([b'{"calib_date": "20260101120000"}'])
        )

    (row,) = omnisharing.stereo_extrinsics(omnisharing.raw(str(root)), "RGBD_0").to_pylist()
    assert row["RGBD_0.calib_date"] == "20260101120000"
    assert np.asarray(row["RGBD_0.left_to_color"]).size == 0


def test_stereo_extrinsics_rejects_empty_cameras(df2_episodes):
    with pytest.raises(ValueError, match="at least one RGBD camera"):
        omnisharing.stereo_extrinsics(df2_episodes, [])


def test_stereo_extrinsics_deduplicates_camera_names(df2_episodes):
    df = omnisharing.stereo_extrinsics(df2_episodes, ["RGBD_0", "RGBD_0"])
    assert sum(name == "RGBD_0.left_to_color" for name in df.column_names) == 1


# ---------------------------------------------------------------------------
# frames()
# ---------------------------------------------------------------------------


def test_frames_requires_explicit_field_whitelist(df2_episodes):
    with pytest.raises(ValueError, match="memory hazard"):
        omnisharing.frames(df2_episodes, fields=[])


@pytest.mark.parametrize("fields", [["nope"], ["action.lefthand.tactile"]])
def test_frames_rejects_invalid_fields(df2_episodes, fields):
    with pytest.raises(ValueError, match="Unknown frame field"):
        omnisharing.frames(df2_episodes, fields=fields)


def test_frames_expands_one_row_per_frame(df2_episodes):
    rows = omnisharing.frames(df2_episodes, fields=["observation.lefthand.joints"]).sort("frame_index").to_pylist()
    assert len(rows) == N_FRAMES
    assert [r["frame_index"] for r in rows] == list(range(N_FRAMES))
    assert all(rows[i + 1]["timestamp"] > rows[i]["timestamp"] for i in range(N_FRAMES - 1))
    # Per-frame vectors are 1-D slices of the episode tensor.
    assert np.asarray(rows[0]["observation.lefthand.joints"]).shape == (N_JOINTS,)


def test_frames_drops_episode_handle_but_keeps_identity(df2_episodes):
    columns = omnisharing.frames(df2_episodes, fields=["observation.lefthand.joints"]).column_names
    assert "episode" not in columns
    assert "episode_key" in columns
    assert "stage" in columns


def test_frames_applies_action_leads_observation_by_one(df2_episodes):
    fields = ["observation.lefthand.joints", "action.lefthand.joints"]
    (episode_row,) = omnisharing.trajectory(df2_episodes, fields=fields, include_attrs=False).to_pylist()
    raw_obs = np.asarray(episode_row["observation.lefthand.joints"])
    raw_act = np.asarray(episode_row["action.lefthand.joints"])

    rows = omnisharing.frames(df2_episodes, fields=fields).sort("frame_index").to_pylist()
    for i, row in enumerate(rows):
        np.testing.assert_array_equal(np.asarray(row["observation.lefthand.joints"]), raw_obs[i])
        expected_action = raw_act[min(i + 1, N_FRAMES - 1)]
        np.testing.assert_array_equal(np.asarray(row["action.lefthand.joints"]), expected_action)

    # The shift is genuinely applied, and the tail is duplicated.
    assert not np.array_equal(np.asarray(rows[0]["action.lefthand.joints"]), raw_act[0])
    np.testing.assert_array_equal(
        np.asarray(rows[-2]["action.lefthand.joints"]),
        np.asarray(rows[-1]["action.lefthand.joints"]),
    )


def test_frames_aligns_cameras_by_nearest_timestamp(tmp_path):
    # Enough frames for the faster RGB clock to drift past a full frame period,
    # which is what makes frame-index alignment wrong.
    root = tmp_path / "drift"
    write_df2_episode(root / episode_filename(1, "212953", 93, 110056), n_frames=10)
    episodes = omnisharing.raw(str(root))

    rows = (
        omnisharing.frames(
            episodes,
            fields=["observation.lefthand.joints"],
            align_cameras=["RGB_Camera0", ("RGBD_0", "color")],
        )
        .sort("frame_index")
        .to_pylist()
    )
    assert len(rows) == 10

    rgb_index = [r["RGB_Camera0.frame_index"] for r in rows]
    rgb_delta = [r["RGB_Camera0.timestamp_delta_us"] for r in rows]
    # The RGB clock runs faster, so nearest-neighbour drifts away from identity
    # and eventually skips a source frame entirely.
    assert rgb_index != list(range(10))
    assert all(rgb_index[i] <= rgb_index[i + 1] for i in range(9))
    assert rgb_index[0] == 0
    assert rgb_delta[0] == 0
    assert abs(rgb_delta[-1]) > abs(rgb_delta[1])

    # RGBD shares the observation clock, so it aligns exactly.
    assert [r["RGBD_0.color.frame_index"] for r in rows] == list(range(10))
    assert all(r["RGBD_0.color.timestamp_delta_us"] == 0 for r in rows)


def test_frames_camera_alignment_tracks_clock_drift(df2_episodes):
    # Even before the drift is large enough to skip a frame, the residual must
    # grow monotonically rather than being reported as a perfect match.
    rows = (
        omnisharing.frames(
            df2_episodes,
            fields=["observation.lefthand.joints"],
            align_cameras=["RGB_Camera0"],
        )
        .sort("frame_index")
        .to_pylist()
    )
    deltas = [r["RGB_Camera0.timestamp_delta_us"] for r in rows]
    assert deltas[0] == 0
    assert any(d != 0 for d in deltas[1:])
    assert abs(deltas[-1]) > abs(deltas[1])


def test_frames_tolerates_absent_camera(df2_episodes):
    rows = omnisharing.frames(
        df2_episodes, fields=["observation.lefthand.joints"], align_cameras=["RGB_Camera99"]
    ).to_pylist()
    assert len(rows) == N_FRAMES
    assert rows[0]["RGB_Camera99.frame_index"] is None


def test_frames_aligns_rgbd_eyes_on_their_own_clocks(df2_root):
    import h5py

    (path,) = df2_root.rglob("*.hdf5")
    with h5py.File(path, "r") as h5:
        group = h5["dataset/observation/image/RGBD_0"]
        left_clock = np.asarray(group["left_timestamp"][()])
        observation_clock = np.asarray(h5["dataset/observation/aligned_timestamp"][()])

    rows = (
        omnisharing.frames(
            omnisharing.raw(str(df2_root)),
            fields=["observation.lefthand.joints"],
            align_cameras=[("RGBD_0", "left"), ("RGBD_0", "color"), ("RGBD_0", "right")],
        )
        .sort("frame_index")
        .to_pylist()
    )

    # The left eye runs on left_timestamp, so its residuals must match a
    # nearest-neighbour search against that clock, not the colour one.
    expected_index = [int(np.argmin(np.abs(left_clock - t))) for t in observation_clock]
    expected_delta = [int(left_clock[j]) - int(t) for j, t in zip(expected_index, observation_clock)]
    assert [r["RGBD_0.left.frame_index"] for r in rows] == expected_index
    assert [r["RGBD_0.left.timestamp_delta_us"] for r in rows] == expected_delta
    assert any(delta != 0 for delta in expected_delta)

    # Colour and right share the observation clock, so they align exactly.
    assert [r["RGBD_0.color.frame_index"] for r in rows] == list(range(N_FRAMES))
    assert all(r["RGBD_0.color.timestamp_delta_us"] == 0 for r in rows)
    assert [r["RGBD_0.right.frame_index"] for r in rows] == list(range(N_FRAMES))


def test_frames_left_eye_alignment_diverges_from_colour(tmp_path):
    # Index divergence only appears once drift exceeds half a frame period, so
    # this needs a longer episode than the shared one provides.
    root = tmp_path / "drift"
    write_df2_episode(root / episode_filename(1, "212953", 93, 110056), n_frames=20)

    rows = (
        omnisharing.frames(
            omnisharing.raw(str(root)),
            fields=["observation.lefthand.joints"],
            align_cameras=[("RGBD_0", "left"), ("RGBD_0", "color")],
        )
        .sort("frame_index")
        .to_pylist()
    )
    left_index = [r["RGBD_0.left.frame_index"] for r in rows]
    colour_index = [r["RGBD_0.color.frame_index"] for r in rows]

    assert left_index != colour_index
    assert all(left_index[i] <= left_index[i + 1] for i in range(len(left_index) - 1))


def test_frames_falls_back_to_camera_clock_when_eye_clock_is_absent(tmp_path):
    root = tmp_path / "noeye"
    path = root / episode_filename(1, "212953", 93, 110056)
    write_df2_episode(path, n_frames=8)

    import h5py

    with h5py.File(path, "a") as h5:
        del h5["dataset/observation/image/RGBD_0/left_timestamp"]

    rows = (
        omnisharing.frames(
            omnisharing.raw(str(root)),
            fields=["observation.lefthand.joints"],
            align_cameras=[("RGBD_0", "left")],
        )
        .sort("frame_index")
        .to_pylist()
    )
    # Without a per-eye clock the camera clock is used, preserving old behaviour.
    assert [r["RGBD_0.left.frame_index"] for r in rows] == list(range(8))
    assert all(r["RGBD_0.left.timestamp_delta_us"] == 0 for r in rows)


def test_cameras_falls_back_to_camera_clock_when_eye_clock_is_absent(tmp_path):
    root = tmp_path / "noeye_cameras"
    path = root / episode_filename(1, "212953", 93, 110056)
    write_df2_episode(path, n_frames=8)

    import h5py

    with h5py.File(path, "a") as h5:
        del h5["dataset/observation/image/RGBD_0/left_timestamp"]

    rows = omnisharing.cameras(omnisharing.raw(str(root))).to_pylist()
    left = next(r for r in rows if r["camera"] == "RGBD_0" and r["stream"] == "left")
    assert left["n_timestamps"] == 8


def test_frames_expands_multiple_episodes(tmp_path):
    root = tmp_path / "multi"
    write_df2_episode(root / episode_filename(1, "212953", 93, 110056), n_frames=4)
    write_df2_episode(root / episode_filename(2, "213630", 115, 110092), n_frames=6)

    rows = omnisharing.frames(omnisharing.raw(str(root)), fields=["observation.lefthand.joints"]).to_pylist()
    assert len(rows) == 10

    counts: dict[str, int] = {}
    for row in rows:
        counts[row["episode_key"]] = counts.get(row["episode_key"], 0) + 1
    assert sorted(counts.values()) == [4, 6]


# ---------------------------------------------------------------------------
# Module surface
# ---------------------------------------------------------------------------


def test_module_is_registered_on_daft_datasets():
    assert daft.datasets.omnisharing is omnisharing


def test_public_api_is_exported():
    for name in (
        "raw",
        "describe",
        "episode_metadata",
        "trajectory",
        "tactile",
        "audio",
        "objects",
        "cameras",
        "camera_payloads",
        "camera_frames",
        "depth_frames",
        "stereo_extrinsics",
        "frames",
    ):
        assert name in omnisharing.__all__
        assert callable(getattr(omnisharing, name))


def test_handpose_order_is_qw_first():
    # Guards against a silent switch to the scipy [qx, qy, qz, qw] convention.
    assert omnisharing.HANDPOSE_ORDER == ("x", "y", "z", "qw", "qx", "qy", "qz")
