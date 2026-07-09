from __future__ import annotations

import os
from pathlib import Path

import pytest

import daft
import daft.datasets.egodex as egodex_module
from daft import DataType, MediaType
from daft.datasets.egodex import (
    DEFAULT_TRAJECTORY_FIELDS,
    JOINTS,
    TRAJECTORY_FIELDS,
    TRANSFORM_JOINTS,
    camera_frames,
    raw,
    trajectory,
)
from daft.expressions import col

NUM_FRAMES = 4

_DEFAULT_ATTRS = {
    "task": "fold_towel",
    "llm_description": "Fold the towel in half.",
    "llm_verbs": ["fold", "grasp"],
    "llm_objects": ["towel"],
    "annotated": True,
}


def _write_episode(
    root: Path,
    task: str = "fold_towel",
    episode_id: int = 0,
    *,
    with_video: bool = True,
    num_frames: int = NUM_FRAMES,
    attrs: dict | None = None,
) -> Path:
    h5py = pytest.importorskip("h5py")
    np = pytest.importorskip("numpy")

    task_dir = root / task
    task_dir.mkdir(parents=True, exist_ok=True)
    hdf5_path = task_dir / f"{episode_id}.hdf5"

    intrinsic = np.eye(3, dtype=np.float32) * 736.0
    confidences = np.linspace(0.0, 1.0, num_frames, dtype=np.float32)
    transforms = np.tile(np.eye(4, dtype=np.float32), (num_frames, 1, 1))

    with h5py.File(hdf5_path, "w") as f:
        f.create_dataset("camera/intrinsic", data=intrinsic)
        for joint in JOINTS:
            f.create_dataset(f"confidences/{joint}", data=confidences)
        for joint in TRANSFORM_JOINTS:
            f.create_dataset(f"transforms/{joint}", data=transforms)
        for key, value in (_DEFAULT_ATTRS if attrs is None else attrs).items():
            f.attrs[key] = value

    if with_video:
        (task_dir / f"{episode_id}.mp4").write_bytes(b"")

    return hdf5_path


def _write_playable_video(path: Path, num_frames: int = 6, width: int = 64, height: int = 48) -> None:
    av = pytest.importorskip("av")
    np = pytest.importorskip("numpy")

    with av.open(str(path), "w") as container:
        stream = container.add_stream("mpeg4", rate=30)
        stream.width = width
        stream.height = height
        stream.pix_fmt = "yuv420p"
        for i in range(num_frames):
            image = np.full((height, width, 3), (i * 37) % 256, dtype=np.uint8)
            frame = av.VideoFrame.from_ndarray(image, format="rgb24")
            for packet in stream.encode(frame):
                container.mux(packet)
        for packet in stream.encode():
            container.mux(packet)


def _daft_file_uri(path: Path) -> str:
    return f"file://{path}"


def _as_list(value):
    return value.tolist() if hasattr(value, "tolist") else value


def test_egodex_raw_local_schema_order_and_file_columns(tmp_path) -> None:
    _write_episode(tmp_path, task="fold_towel", episode_id=0)
    _write_episode(tmp_path, task="stack_cups", episode_id=3)

    df = raw(str(tmp_path))

    assert [field.name for field in df.schema()] == [
        "task",
        "episode_id",
        "metadata",
        "trajectory",
        "video",
    ]

    schema = {field.name: field.dtype for field in df.schema()}
    assert schema["task"] == DataType.string()
    assert schema["episode_id"] == DataType.int64()
    assert schema["metadata"] == egodex_module._METADATA_DTYPE
    assert schema["trajectory"] == DataType.file(MediaType.hdf5())
    assert schema["video"] == DataType.file(MediaType.video())

    result = (
        df.select(
            "task",
            "episode_id",
            col("trajectory").file_path().alias("trajectory_path"),
            col("video").file_path().alias("video_path"),
        )
        .sort(["task", "episode_id"])
        .collect()
        .to_pydict()
    )

    assert result["task"] == ["fold_towel", "stack_cups"]
    assert result["episode_id"] == [0, 3]
    assert result["trajectory_path"] == [
        _daft_file_uri(tmp_path / "fold_towel" / "0.hdf5"),
        _daft_file_uri(tmp_path / "stack_cups" / "3.hdf5"),
    ]
    assert result["video_path"] == [
        _daft_file_uri(tmp_path / "fold_towel" / "0.mp4"),
        _daft_file_uri(tmp_path / "stack_cups" / "3.mp4"),
    ]


def test_egodex_raw_derives_task_from_parent_directory_at_any_depth(tmp_path) -> None:
    _write_episode(tmp_path / "part1", task="open_drawer", episode_id=12)

    result = raw(str(tmp_path)).select("task", "episode_id").collect().to_pydict()

    assert result["task"] == ["open_drawer"]
    assert result["episode_id"] == [12]


def test_egodex_raw_reads_metadata_attrs(tmp_path) -> None:
    _write_episode(tmp_path)

    result = raw(str(tmp_path)).select("metadata").collect().to_pydict()
    metadata = result["metadata"][0]

    assert metadata["task"] == "fold_towel"
    assert metadata["llm_description"] == "Fold the towel in half."
    assert metadata["llm_verbs"] == ["fold", "grasp"]
    assert metadata["llm_objects"] == ["towel"]
    assert metadata["annotated"] is True
    # Attributes absent from the file surface as nulls.
    assert metadata["llm_description2"] is None
    assert metadata["environment"] is None


def test_egodex_raw_handles_minimal_metadata_attrs(tmp_path) -> None:
    # Older EgoDex files carry only a subset of attributes.
    _write_episode(tmp_path, attrs={"llm_description": "minimal episode"})

    result = raw(str(tmp_path)).select("metadata").collect().to_pydict()
    metadata = result["metadata"][0]

    assert metadata["llm_description"] == "minimal episode"
    assert metadata["task"] is None
    assert metadata["annotated"] is None
    # Missing list-valued attributes surface as empty lists.
    assert metadata["llm_verbs"] == []
    assert metadata["llm_objects"] == []


def test_egodex_metadata_attr_value_coerces_h5py_values() -> None:
    np = pytest.importorskip("numpy")

    assert egodex_module._attr_value(None) is None
    assert egodex_module._attr_value(b"fold_towel") == "fold_towel"
    assert egodex_module._attr_value(np.array(b"stack_cups")) == "stack_cups"
    assert egodex_module._attr_value(np.array([b"fold", b"grasp"])) == ["fold", "grasp"]
    assert egodex_module._attr_value(np.float32(1.5)) == pytest.approx(1.5)
    assert egodex_module._attr_value("plain") == "plain"


def test_egodex_raw_sets_missing_video_to_null(tmp_path) -> None:
    _write_episode(tmp_path, task="fold_towel", episode_id=0, with_video=False)
    _write_episode(tmp_path, task="fold_towel", episode_id=1)

    result = (
        raw(str(tmp_path))
        .select("episode_id", col("video").file_path().alias("video_path"))
        .sort("episode_id")
        .collect()
        .to_pydict()
    )

    assert result["video_path"] == [None, _daft_file_uri(tmp_path / "fold_towel" / "1.mp4")]


def test_egodex_raw_filters_by_single_task_before_returning_catalog(tmp_path) -> None:
    _write_episode(tmp_path, task="fold_towel", episode_id=0)
    _write_episode(tmp_path, task="stack_cups", episode_id=1)

    result = raw(str(tmp_path), tasks="stack_cups").select("task", "episode_id").collect().to_pydict()

    assert result["task"] == ["stack_cups"]
    assert result["episode_id"] == [1]


def test_egodex_raw_filters_by_task_list_and_episode_id_list(tmp_path) -> None:
    _write_episode(tmp_path, task="fold_towel", episode_id=0)
    _write_episode(tmp_path, task="fold_towel", episode_id=1)
    _write_episode(tmp_path, task="stack_cups", episode_id=0)
    _write_episode(tmp_path, task="open_drawer", episode_id=4)

    result = (
        raw(str(tmp_path), tasks=["fold_towel", "stack_cups"], episode_ids=[0, 2])
        .select("task", "episode_id")
        .sort("task")
        .collect()
        .to_pydict()
    )

    assert result["task"] == ["fold_towel", "stack_cups"]
    assert result["episode_id"] == [0, 0]


def test_egodex_raw_filters_by_single_episode_id(tmp_path) -> None:
    _write_episode(tmp_path, task="fold_towel", episode_id=0)
    _write_episode(tmp_path, task="fold_towel", episode_id=1)

    result = raw(str(tmp_path), episode_ids=1).select("task", "episode_id").collect().to_pydict()

    assert result["task"] == ["fold_towel"]
    assert result["episode_id"] == [1]


@pytest.fixture
def sample_episodes_df(tmp_path):
    _write_episode(tmp_path)
    return raw(str(tmp_path))


def test_trajectory_reads_selected_fields(sample_episodes_df) -> None:
    result = (
        trajectory(
            sample_episodes_df,
            fields=["camera/intrinsic", "transforms/leftHand", "confidences/leftHand"],
        )
        .collect()
        .to_pydict()
    )

    assert _as_list(result["camera/intrinsic"][0]) == [
        [736.0, 0.0, 0.0],
        [0.0, 736.0, 0.0],
        [0.0, 0.0, 736.0],
    ]
    assert (
        _as_list(result["transforms/leftHand"][0])
        == [
            [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
                [0.0, 0.0, 1.0, 0.0],
                [0.0, 0.0, 0.0, 1.0],
            ]
        ]
        * NUM_FRAMES
    )
    confidences = _as_list(result["confidences/leftHand"][0])
    assert len(confidences) == NUM_FRAMES
    assert confidences[0] == pytest.approx(0.0)
    assert confidences[-1] == pytest.approx(1.0)


def test_trajectory_default_fields_and_column_order(sample_episodes_df) -> None:
    result = trajectory(sample_episodes_df)

    assert [field.name for field in result.schema()] == [
        "task",
        "episode_id",
        "metadata",
        "camera/intrinsic",
        "transforms/camera",
        "transforms/leftHand",
        "transforms/rightHand",
        "confidences/leftHand",
        "confidences/rightHand",
        "video",
    ]


def test_trajectory_schema_uses_known_hdf5_dtypes(sample_episodes_df) -> None:
    result = trajectory(sample_episodes_df, fields=["transforms/rightHand", "confidences/hip"])

    schema = result.schema()
    assert schema["transforms/rightHand"].dtype == DataType.tensor(DataType.float32())
    assert schema["confidences/hip"].dtype == DataType.tensor(DataType.float32())


def test_trajectory_filters_missing_trajectory(sample_episodes_df) -> None:
    episodes = sample_episodes_df.collect().with_column(
        "trajectory",
        daft.lit(None).cast(DataType.file(MediaType.hdf5())),
    )

    result = trajectory(episodes, fields=["confidences/hip"]).collect().to_pydict()

    assert result["confidences/hip"] == []


def test_trajectory_requires_trajectory_column() -> None:
    episodes = daft.from_pydict({"task": ["fold_towel"]})
    with pytest.raises(ValueError, match="trajectory"):
        trajectory(episodes, fields=["confidences/hip"])


def test_trajectory_rejects_empty_fields(sample_episodes_df) -> None:
    with pytest.raises(ValueError, match="at least one"):
        trajectory(sample_episodes_df, fields=[])


def test_trajectory_rejects_unknown_fields(sample_episodes_df) -> None:
    with pytest.raises(ValueError, match="Unknown trajectory field"):
        trajectory(sample_episodes_df, fields=["transforms/notAJoint"])


def test_trajectory_requires_hdf5_extra(sample_episodes_df, monkeypatch) -> None:
    from daft.dependencies import h5py

    monkeypatch.setattr(h5py, "module_available", lambda: False)

    with pytest.raises(ImportError, match=r"daft\[hdf5\]"):
        trajectory(sample_episodes_df, fields=["confidences/hip"])


@pytest.fixture
def sample_camera_episodes_df(tmp_path):
    pytest.importorskip("av")
    _write_episode(tmp_path, with_video=False)
    _write_playable_video(tmp_path / "fold_towel" / "0.mp4")
    return raw(str(tmp_path))


def test_camera_frames_appends_video_frames_column(sample_camera_episodes_df) -> None:
    result = camera_frames(sample_camera_episodes_df, width=32, height=24)
    schema = [field.name for field in result.schema()]

    assert schema == ["task", "episode_id", "metadata", "trajectory", "video", "video_frames"]


def test_camera_frames_decodes_local_video(sample_camera_episodes_df) -> None:
    result = (
        camera_frames(sample_camera_episodes_df, width=32, height=24)
        .select("episode_id", "video_frames")
        .collect()
        .to_pydict()
    )

    frames = result["video_frames"][0]
    assert len(frames) > 0


def test_camera_frames_returns_empty_frames_for_missing_video(sample_camera_episodes_df) -> None:
    episodes = sample_camera_episodes_df.with_column(
        "video",
        daft.lit(None).cast(DataType.file(MediaType.video())),
    )

    result = camera_frames(episodes).select("video_frames").collect().to_pydict()

    assert result["video_frames"] == [[]]


def test_camera_frames_requires_video_column() -> None:
    pytest.importorskip("av")
    episodes = daft.from_pydict({"task": ["fold_towel"]})
    with pytest.raises(ValueError, match="video"):
        camera_frames(episodes)


def test_camera_frames_requires_video_extra(sample_camera_episodes_df, monkeypatch) -> None:
    from daft.dependencies import av

    monkeypatch.setattr(av, "module_available", lambda: False)

    with pytest.raises(ImportError, match=r"daft\[video\]"):
        camera_frames(sample_camera_episodes_df)


def test_joint_and_field_catalogs_match_egodex_layout() -> None:
    assert len(JOINTS) == 68
    assert TRANSFORM_JOINTS[0] == "camera"
    assert len(TRANSFORM_JOINTS) == 69
    assert len(TRAJECTORY_FIELDS) == 1 + 68 + 69
    assert "camera/intrinsic" in TRAJECTORY_FIELDS
    assert "transforms/leftHand" in TRAJECTORY_FIELDS
    assert "confidences/rightThumbTip" in TRAJECTORY_FIELDS
    assert set(DEFAULT_TRAJECTORY_FIELDS) <= set(TRAJECTORY_FIELDS)
    assert DEFAULT_TRAJECTORY_FIELDS == egodex_module._DEFAULT_TRAJECTORY_FIELDS


EGODEX_DATA_DIR = os.environ.get("EGODEX_DATA_DIR")


@pytest.mark.integration()
@pytest.mark.skipif(
    not EGODEX_DATA_DIR,
    reason="EGODEX_DATA_DIR must point at a local EgoDex download (the CC-BY-NC-ND license prohibits hosting a copy)",
)
def test_egodex_reads_real_local_download() -> None:
    episodes = raw(EGODEX_DATA_DIR).limit(1)
    result = trajectory(episodes).collect().to_pydict()

    assert len(result["task"]) == 1
    transforms = result["transforms/camera"][0]
    assert transforms.shape[1:] == (4, 4)
    assert result["camera/intrinsic"][0].shape == (3, 3)
