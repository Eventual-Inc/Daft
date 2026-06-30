from __future__ import annotations

import json
from pathlib import Path

import pytest

import daft
import daft.datasets.droid as droid_module
from daft import DataType, MediaType
from daft.datasets.droid import SCENE_CLASSIFICATIONS, camera_frames, filter_scenes, trajectory
from daft.expressions import col
from daft.functions import hdf5_file, video_file

DROID_RAW_GCS_PREFIX = "gs://gresearch/robotics/droid_raw"


@pytest.fixture(scope="module")
def droid_raw_df():
    return daft.datasets.droid.raw()


def _write_local_episode(root: Path, missing_videos: set[str] | None = None) -> Path:
    missing_videos = set() if missing_videos is None else missing_videos
    episode_dir = root / "1.0.1" / "LAB" / "success" / "2023-11-07" / "Tue_Nov__7_19:29:35_2023"
    mp4_dir = episode_dir / "recordings" / "MP4"
    mp4_dir.mkdir(parents=True)
    (episode_dir / "trajectory.h5").write_bytes(b"")

    camera_serials = {
        "wrist": "13263313",
        "ext1": "23804457",
        "ext2": "28834630",
    }
    for camera, serial in camera_serials.items():
        if camera not in missing_videos:
            (mp4_dir / f"{serial}.mp4").write_bytes(b"")

    metadata = {
        "uuid": "LAB+user-1+2023-11-07-19h-29m-35s",
        "lab": "LAB",
        "user": "Daft Tester",
        "user_id": "user-1",
        "date": "2023-11-07",
        "timestamp": "2023-11-07-19h-29m-35s",
        "hdf5_path": "trajectory.h5",
        "building": "Test Building",
        "scene_id": 8288363487,
        "success": True,
        "robot_serial": "panda-295341-1325132",
        "r2d2_version": "1.3",
        "current_task": "Move object to a new position",
        "trajectory_length": 435,
        "wrist_cam_serial": camera_serials["wrist"],
        "ext1_cam_serial": camera_serials["ext1"],
        "ext2_cam_serial": camera_serials["ext2"],
        "wrist_cam_extrinsics": [0.37, -0.12, 0.4, 2.91, -0.11, 1.69],
        "ext1_cam_extrinsics": [-0.17, -0.22, 0.17, -1.4, 0.27, -1.54],
        "ext2_cam_extrinsics": [-0.06, 0.48, 0.3, -1.63, -0.0, -2.35],
        "wrist_svo_path": "recordings/SVO/wrist.svo",
        "wrist_mp4_path": f"recordings/MP4/{camera_serials['wrist']}.mp4",
        "ext1_svo_path": "recordings/SVO/ext1.svo",
        "ext1_mp4_path": f"recordings/MP4/{camera_serials['ext1']}.mp4",
        "ext2_svo_path": "recordings/SVO/ext2.svo",
        "ext2_mp4_path": f"recordings/MP4/{camera_serials['ext2']}.mp4",
        "left_mp4_path": "recordings/MP4/left.mp4",
        "right_mp4_path": "recordings/MP4/right.mp4",
    }
    (episode_dir / "metadata_2023-11-07-19h-29m-35s.json").write_text(json.dumps(metadata), encoding="utf-8")
    return episode_dir


def _daft_file_uri(path: Path) -> str:
    return f"file://{path}"


def test_droid_raw_local_schema_order_and_file_columns(tmp_path) -> None:
    episode_dir = _write_local_episode(tmp_path)
    df = daft.datasets.droid.raw(str(tmp_path))

    assert [field.name for field in df.schema()] == [
        "uuid",
        "lab",
        "date",
        "timestamp",
        "scene_id",
        "trajectory_length",
        "current_task",
        "success",
        "episode_dir",
        "user",
        "user_id",
        "building",
        "robot_serial",
        "r2d2_version",
        "trajectory",
        "wrist_cam_serial",
        "wrist_cam_extrinsics",
        "wrist_cam_video",
        "ext1_cam_serial",
        "ext1_cam_extrinsics",
        "ext1_cam_video",
        "ext2_cam_serial",
        "ext2_cam_extrinsics",
        "ext2_cam_video",
    ]

    schema = {field.name: field.dtype for field in df.schema()}
    assert schema["trajectory"] == DataType.file(MediaType.hdf5())
    assert schema["wrist_cam_video"] == DataType.file(MediaType.video())
    assert schema["ext1_cam_video"] == DataType.file(MediaType.video())
    assert schema["ext2_cam_video"] == DataType.file(MediaType.video())

    result = (
        df.select(
            "uuid",
            "episode_dir",
            col("trajectory").file_path().alias("trajectory_path"),
            col("wrist_cam_video").file_path().alias("wrist_cam_video_path"),
            col("ext1_cam_video").file_path().alias("ext1_cam_video_path"),
            col("ext2_cam_video").file_path().alias("ext2_cam_video_path"),
        )
        .collect()
        .to_pydict()
    )

    assert result["uuid"] == ["LAB+user-1+2023-11-07-19h-29m-35s"]
    assert result["episode_dir"] == [_daft_file_uri(episode_dir)]
    assert result["trajectory_path"] == [_daft_file_uri(episode_dir / "trajectory.h5")]
    assert result["wrist_cam_video_path"] == [_daft_file_uri(episode_dir / "recordings" / "MP4" / "13263313.mp4")]
    assert result["ext1_cam_video_path"] == [_daft_file_uri(episode_dir / "recordings" / "MP4" / "23804457.mp4")]
    assert result["ext2_cam_video_path"] == [_daft_file_uri(episode_dir / "recordings" / "MP4" / "28834630.mp4")]


def test_droid_raw_sets_missing_camera_recordings_to_null(tmp_path) -> None:
    episode_dir = _write_local_episode(tmp_path, missing_videos={"ext1"})
    result = (
        daft.datasets.droid.raw(str(tmp_path))
        .select(
            col("wrist_cam_video").file_path().alias("wrist_cam_video_path"),
            col("ext1_cam_video").file_path().alias("ext1_cam_video_path"),
            col("ext2_cam_video").file_path().alias("ext2_cam_video_path"),
        )
        .collect()
        .to_pydict()
    )

    assert result["wrist_cam_video_path"] == [_daft_file_uri(episode_dir / "recordings" / "MP4" / "13263313.mp4")]
    assert result["ext1_cam_video_path"] == [None]
    assert result["ext2_cam_video_path"] == [_daft_file_uri(episode_dir / "recordings" / "MP4" / "28834630.mp4")]


@pytest.mark.integration()
def test_droid_discovers_episodes_and_metadata(droid_raw_df) -> None:
    result = droid_raw_df.select("uuid", "building", "success").limit(1).to_pydict()

    assert len(result["uuid"]) == 1
    assert isinstance(result["uuid"][0], str) and result["uuid"][0]
    assert isinstance(result["building"][0], str) and result["building"][0]
    assert isinstance(result["success"][0], bool)


@pytest.mark.integration()
def test_droid_unnests_metadata_columns(droid_raw_df) -> None:
    schema = {field.name: field.dtype for field in droid_raw_df.schema()}
    assert "building" in schema
    assert "success" in schema
    assert "metadata" not in schema
    assert schema["building"] == DataType.string
    assert schema["success"] == DataType.bool


@pytest.mark.integration()
def test_droid_adds_trajectory_and_video_file_columns(droid_raw_df) -> None:
    schema = {field.name: field.dtype for field in droid_raw_df.schema()}
    assert schema["trajectory"] == DataType.file(MediaType.hdf5())
    assert schema["wrist_cam_video"] == DataType.file(MediaType.video())
    assert schema["ext1_cam_video"] == DataType.file(MediaType.video())
    assert schema["ext2_cam_video"] == DataType.file(MediaType.video())

    result = (
        droid_raw_df.where(
            col("trajectory").not_null()
            & col("wrist_cam_video").not_null()
            & col("ext1_cam_video").not_null()
            & col("ext2_cam_video").not_null()
        )
        .select(
            "episode_dir",
            "wrist_cam_serial",
            "ext1_cam_serial",
            "ext2_cam_serial",
            col("trajectory").file_path().alias("trajectory_path"),
            col("wrist_cam_video").file_path().alias("wrist_cam_video_path"),
            col("ext1_cam_video").file_path().alias("ext1_cam_video_path"),
            col("ext2_cam_video").file_path().alias("ext2_cam_video_path"),
        )
        .limit(1)
        .to_pydict()
    )

    episode_dir = result["episode_dir"][0]
    assert episode_dir.startswith(f"{DROID_RAW_GCS_PREFIX}/")

    trajectory_path = result["trajectory_path"][0]
    assert trajectory_path == f"{episode_dir}/trajectory.h5"

    assert result["wrist_cam_video_path"][0] == (f"{episode_dir}/recordings/MP4/{result['wrist_cam_serial'][0]}.mp4")
    assert result["ext1_cam_video_path"][0] == (f"{episode_dir}/recordings/MP4/{result['ext1_cam_serial'][0]}.mp4")
    assert result["ext2_cam_video_path"][0] == (f"{episode_dir}/recordings/MP4/{result['ext2_cam_serial'][0]}.mp4")


def _as_list(value):
    return value.tolist() if hasattr(value, "tolist") else value


_SAMPLE_TRAJECTORY_FIELDS = [
    "action/joint_position",
    "action/gripper_position",
    "observation/robot_state/joint_positions",
]


@pytest.fixture
def sample_episodes_df(tmp_path):
    h5py = pytest.importorskip("h5py")

    joint_position = [[0.0, 1.0, 2.0], [3.0, 4.0, 5.0], [6.0, 7.0, 8.0], [9.0, 10.0, 11.0]]
    gripper_position = [0.1, 0.2, 0.3, 0.4]
    robot_joint_positions = [
        [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        [7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0],
        [14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0],
        [21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0],
    ]

    path = tmp_path / "trajectory.h5"
    with h5py.File(path, "w") as f:
        f.create_dataset("action/joint_position", data=joint_position)
        f.create_dataset("action/gripper_position", data=gripper_position)
        observation = f.create_group("observation")
        robot_state = observation.create_group("robot_state")
        robot_state.create_dataset("joint_positions", data=robot_joint_positions)
        timestamp = observation.create_group("timestamp")
        timestamp.create_dataset("skip_action", data=[False, False, True, False])
        control = timestamp.create_group("control")
        control.create_dataset("step_start", data=[10, 20, 30, 40])

    return (
        daft.from_pydict(
            {
                "uuid": ["episode-1"],
                "scene_id": [1],
                "robot_serial": ["robot-1"],
                "r2d2_version": ["1.0"],
                "current_task": ["pick up the object"],
                "success": [True],
                "trajectory_length": [4],
                "path": [str(path)],
            }
        )
        .select(
            "uuid",
            "scene_id",
            "robot_serial",
            "r2d2_version",
            "current_task",
            "success",
            "trajectory_length",
            hdf5_file(daft.col("path")).alias("trajectory"),
        )
        .with_columns(
            {
                "wrist_cam_video": video_file(daft.lit("/tmp/wrist.mp4")),
                "wrist_cam_extrinsics": daft.lit([0.0, 1.0, 2.0]),
                "ext1_cam_video": video_file(daft.lit("/tmp/ext1.mp4")),
                "ext1_cam_extrinsics": daft.lit([3.0, 4.0, 5.0]),
                "ext2_cam_video": video_file(daft.lit("/tmp/ext2.mp4")),
                "ext2_cam_extrinsics": daft.lit([6.0, 7.0, 8.0]),
            }
        )
    )


def test_trajectory_reads_selected_fields(sample_episodes_df) -> None:
    result = trajectory(sample_episodes_df, fields=_SAMPLE_TRAJECTORY_FIELDS).collect().to_pydict()

    assert _as_list(result["action/joint_position"][0]) == [
        [0.0, 1.0, 2.0],
        [3.0, 4.0, 5.0],
        [6.0, 7.0, 8.0],
        [9.0, 10.0, 11.0],
    ]
    assert _as_list(result["action/gripper_position"][0]) == [0.1, 0.2, 0.3, 0.4]
    assert _as_list(result["observation/robot_state/joint_positions"][0]) == [
        [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        [7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0],
        [14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0],
        [21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0],
    ]


def test_trajectory_filters_missing_trajectory(sample_episodes_df) -> None:
    episodes = sample_episodes_df.with_column(
        "trajectory",
        daft.lit(None).cast(DataType.file(MediaType.hdf5())),
    )

    result = trajectory(episodes, fields=["action/gripper_position"]).collect().to_pydict()

    assert result["action/gripper_position"] == []


def test_trajectory_unnests_hdf5_fields(sample_episodes_df) -> None:
    result = trajectory(sample_episodes_df, fields=["action/gripper_position"])

    assert [field.name for field in result.schema()] == [
        "uuid",
        "scene_id",
        "robot_serial",
        "r2d2_version",
        "current_task",
        "success",
        "trajectory_length",
        "action/gripper_position",
        "wrist_cam_video",
        "wrist_cam_extrinsics",
        "ext1_cam_video",
        "ext1_cam_extrinsics",
        "ext2_cam_video",
        "ext2_cam_extrinsics",
    ]


def test_trajectory_schema_uses_known_hdf5_dtypes(sample_episodes_df) -> None:
    result = trajectory(
        sample_episodes_df,
        fields=[
            "action/joint_position",
            "action/gripper_position",
            "observation/timestamp/control/step_start",
            "observation/timestamp/skip_action",
        ],
    )

    schema = result.schema()
    assert schema["action/joint_position"].dtype == DataType.tensor(DataType.float64())
    assert schema["action/gripper_position"].dtype == DataType.tensor(DataType.float64())
    assert schema["observation/timestamp/control/step_start"].dtype == DataType.tensor(DataType.int64())
    assert schema["observation/timestamp/skip_action"].dtype == DataType.tensor(DataType.bool())


def test_trajectory_uses_curated_raw_column_order(sample_episodes_df) -> None:
    episodes = sample_episodes_df.with_columns(
        {
            "wrist_cam_video": video_file(daft.lit("/tmp/wrist.mp4")),
            "wrist_cam_extrinsics": daft.lit([0.0, 1.0, 2.0]),
            "ext1_cam_video": video_file(daft.lit("/tmp/ext1.mp4")),
            "ext1_cam_extrinsics": daft.lit([3.0, 4.0, 5.0]),
            "ext2_cam_video": video_file(daft.lit("/tmp/ext2.mp4")),
            "ext2_cam_extrinsics": daft.lit([6.0, 7.0, 8.0]),
            "unused": daft.lit("drop me"),
        }
    )

    result = trajectory(episodes, fields=["action/gripper_position"])

    assert [field.name for field in result.schema()] == [
        "uuid",
        "scene_id",
        "robot_serial",
        "r2d2_version",
        "current_task",
        "success",
        "trajectory_length",
        "action/gripper_position",
        "wrist_cam_video",
        "wrist_cam_extrinsics",
        "ext1_cam_video",
        "ext1_cam_extrinsics",
        "ext2_cam_video",
        "ext2_cam_extrinsics",
    ]


def test_trajectory_reads_full_hdf5_paths(sample_episodes_df) -> None:
    result = (
        trajectory(
            sample_episodes_df,
            fields=["action/joint_position"],
        )
        .collect()
        .to_pydict()
    )

    assert _as_list(result["action/joint_position"][0]) == [
        [0.0, 1.0, 2.0],
        [3.0, 4.0, 5.0],
        [6.0, 7.0, 8.0],
        [9.0, 10.0, 11.0],
    ]


def test_trajectory_requires_trajectory_column() -> None:
    episodes = daft.from_pydict({"uuid": ["episode-1"]})
    with pytest.raises(ValueError, match="trajectory"):
        trajectory(episodes, fields=["action/joint_position"])


def test_trajectory_rejects_empty_fields(sample_episodes_df) -> None:
    with pytest.raises(ValueError, match="at least one"):
        trajectory(sample_episodes_df, fields=[])


def test_trajectory_rejects_unknown_fields(sample_episodes_df) -> None:
    with pytest.raises(ValueError, match="Unknown trajectory field"):
        trajectory(sample_episodes_df, fields=["action/not_a_real_field"])


@pytest.fixture
def sample_scene_classifications_parquet(tmp_path) -> Path:
    path = tmp_path / "scene_classifications.parquet"
    daft.from_pydict(
        {
            "scene_id": [1, 2, 3],
            "scene_classification": [
                "Industrial office",
                "Home kitchen",
                "Bedroom",
            ],
        }
    ).write_parquet(str(path), write_mode="overwrite")
    return path


@pytest.fixture
def local_scene_classifications(monkeypatch, sample_scene_classifications_parquet) -> None:
    monkeypatch.setattr(
        droid_module,
        "_HF_SCENE_CLASSIFICATIONS_PATH",
        str(sample_scene_classifications_parquet),
    )


def test_filter_scenes_joins_and_filters(sample_episodes_df, local_scene_classifications) -> None:
    result = filter_scenes(sample_episodes_df, "Industrial office").collect().to_pydict()

    assert result["uuid"] == ["episode-1"]
    assert result["scene_id"] == [1]
    assert result["scene_classification"] == ["Industrial office"]


def test_filter_scenes_filters_multiple_scene_types(local_scene_classifications) -> None:
    episodes = daft.from_pydict(
        {
            "uuid": ["episode-1", "episode-2", "episode-3"],
            "scene_id": [1, 2, 3],
        }
    )

    result = (
        filter_scenes(
            episodes,
            ["Industrial office", "Home kitchen"],
        )
        .collect()
        .to_pydict()
    )

    assert result["uuid"] == ["episode-1", "episode-2"]
    assert result["scene_classification"] == ["Industrial office", "Home kitchen"]


def test_filter_scenes_excludes_non_matching_scene_labels(sample_episodes_df, local_scene_classifications) -> None:
    result = filter_scenes(sample_episodes_df, "Home kitchen").collect().to_pydict()

    assert result["uuid"] == []


def test_filter_scenes_excludes_unclassified_scene_ids(local_scene_classifications) -> None:
    episodes = daft.from_pydict(
        {
            "uuid": ["episode-1", "episode-2"],
            "scene_id": [1, 999],
        }
    )

    result = filter_scenes(episodes, "Industrial office").collect().to_pydict()

    assert result["uuid"] == ["episode-1"]
    assert result["scene_id"] == [1]


def test_filter_scenes_preserves_episode_columns(sample_episodes_df, local_scene_classifications) -> None:
    result = filter_scenes(sample_episodes_df, "Industrial office")
    schema = {field.name for field in result.schema()}

    assert "uuid" in schema
    assert "scene_id" in schema
    assert "scene_classification" in schema
    assert "trajectory" in schema
    assert "robot_serial" in schema


def test_filter_scenes_reads_hf_parquet_path(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_read_parquet(path: str, io_config=None):
        captured["path"] = path
        captured["io_config"] = io_config
        return daft.from_pydict(
            {
                "scene_id": [1],
                "scene_classification": ["Industrial office"],
            }
        )

    monkeypatch.setattr(daft, "read_parquet", fake_read_parquet)

    episodes = daft.from_pydict({"uuid": ["episode-1"], "scene_id": [1]})
    result = filter_scenes(episodes, "Industrial office").collect().to_pydict()

    assert captured["path"] == droid_module._HF_SCENE_CLASSIFICATIONS_PATH
    assert str(captured["path"]).startswith("hf://datasets/Eventual-Inc/droid-scene-classifications/")
    assert result["uuid"] == ["episode-1"]
    assert result["scene_classification"] == ["Industrial office"]


def test_filter_scenes_passes_io_config(monkeypatch) -> None:
    captured: dict[str, object] = {}
    io_config = daft.io.IOConfig()

    def fake_read_parquet(path: str, io_config=None):
        captured["io_config"] = io_config
        return daft.from_pydict(
            {
                "scene_id": [1],
                "scene_classification": ["Bedroom"],
            }
        )

    monkeypatch.setattr(daft, "read_parquet", fake_read_parquet)

    episodes = daft.from_pydict({"uuid": ["episode-1"], "scene_id": [1]})
    filter_scenes(episodes, "Bedroom", io_config=io_config).collect()

    assert captured["io_config"] is io_config


def test_filter_scenes_requires_scene_id_column() -> None:
    episodes = daft.from_pydict({"uuid": ["episode-1"]})
    with pytest.raises(ValueError, match="scene_id"):
        filter_scenes(episodes, "Industrial office")


def test_filter_scenes_rejects_unknown_scene_type(sample_episodes_df, local_scene_classifications) -> None:
    with pytest.raises(ValueError, match="Unknown scene classification"):
        filter_scenes(sample_episodes_df, "Kitchen")


def test_filter_scenes_rejects_empty_scene_types(sample_episodes_df, local_scene_classifications) -> None:
    with pytest.raises(ValueError, match="at least one"):
        filter_scenes(sample_episodes_df, [])


def test_scene_classifications_matches_official_labels() -> None:
    assert len(SCENE_CLASSIFICATIONS) == 12
    assert "Home kitchen" in SCENE_CLASSIFICATIONS
    assert "Hallway / closet / doorway" in SCENE_CLASSIFICATIONS
    assert "Kitchen" not in SCENE_CLASSIFICATIONS


@pytest.fixture
def sample_camera_episodes_df(sample_episodes_df):
    pytest.importorskip("av")
    return sample_episodes_df.with_columns(
        {
            "wrist_cam_video": video_file(daft.lit("/tmp/wrist.mp4")),
            "ext1_cam_video": video_file(daft.lit("/tmp/ext1.mp4")),
            "ext2_cam_video": video_file(daft.lit("/tmp/ext2.mp4")),
        }
    )


def test_camera_frames_defaults_to_all_cameras(sample_camera_episodes_df) -> None:
    result = camera_frames(sample_camera_episodes_df, width=64, height=48)
    schema = {field.name for field in result.schema()}

    assert "wrist_cam_frames" in schema
    assert "ext1_cam_frames" in schema
    assert "ext2_cam_frames" in schema


def test_camera_frames_returns_empty_frames_for_missing_camera(sample_camera_episodes_df) -> None:
    episodes = sample_camera_episodes_df.with_column(
        "wrist_cam_video",
        daft.lit(None).cast(DataType.file(MediaType.video())),
    )

    result = camera_frames(episodes, cameras="wrist").collect().to_pydict()

    assert result["wrist_cam_frames"] == [[]]


def test_camera_frames_accepts_single_camera_string(sample_camera_episodes_df) -> None:
    result = camera_frames(sample_camera_episodes_df, cameras="wrist", sample_interval_seconds=1.0)
    schema = {field.name for field in result.schema()}

    assert "wrist_cam_frames" in schema
    assert "ext1_cam_frames" not in schema
    assert "ext2_cam_frames" not in schema


def test_camera_frames_accepts_camera_list(sample_camera_episodes_df) -> None:
    result = camera_frames(sample_camera_episodes_df, cameras=["wrist", "ext2"])
    schema = {field.name for field in result.schema()}

    assert "wrist_cam_frames" in schema
    assert "ext1_cam_frames" not in schema
    assert "ext2_cam_frames" in schema


def test_camera_frames_deduplicates_cameras(sample_camera_episodes_df) -> None:
    result = camera_frames(sample_camera_episodes_df, cameras=["wrist", "wrist"], width=64, height=48)
    schema = [field.name for field in result.schema()]

    assert schema.count("wrist_cam_frames") == 1
    assert "ext1_cam_frames" not in schema
    assert "ext2_cam_frames" not in schema


def test_camera_frames_rejects_empty_camera_list(sample_camera_episodes_df) -> None:
    with pytest.raises(ValueError, match="cameras must contain at least one"):
        camera_frames(sample_camera_episodes_df, cameras=[])


def test_camera_frames_rejects_unknown_camera(sample_camera_episodes_df) -> None:
    with pytest.raises(ValueError, match="Unknown camera"):
        camera_frames(sample_camera_episodes_df, cameras=["wrist", "overhead"])


def test_camera_frames_requires_requested_camera_columns(tmp_path) -> None:
    pytest.importorskip("av")
    episodes = daft.from_pydict({"uuid": ["episode-1"]})
    with pytest.raises(ValueError, match="Missing columns"):
        camera_frames(episodes, cameras="wrist")
