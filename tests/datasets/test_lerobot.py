from __future__ import annotations

import json
import pathlib
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from daft.datasets.lerobot import load_episode_frames, read, read_episodes, read_tasks


def _write_table(path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


@pytest.fixture
def tiny_lerobot_v3(tmp_path):
    """Minimal on-disk LeRobot v3 layout (two episodes, one shared data shard)."""
    root = tmp_path / "ds"
    (root / "meta" / "episodes" / "chunk-000").mkdir(parents=True)
    (root / "data" / "chunk-000").mkdir(parents=True)

    episodes_tbl = pa.table(
        {
            "episode_index": [0, 1],
            "length": [2, 2],
            "task_index": [0, 0],
            "data/chunk_index": [0, 0],
            "data/file_index": [0, 0],
            "dataset_from_index": [0, 2],
            "dataset_to_index": [2, 4],
            # Hidden by default; exposed via include_meta / include_stats.
            "meta/episode_uuid": ["a", "b"],
            "stats/action_mean": [0.1, 0.2],
        }
    )
    _write_table(root / "meta/episodes/chunk-000/file-000.parquet", episodes_tbl)

    frames_tbl = pa.table(
        {
            "index": [0, 1, 2, 3],
            "episode_index": [0, 0, 1, 1],
            "frame_index": [0, 1, 0, 1],
            "timestamp": [0.0, 1 / 30, 0.0, 1 / 30],
            "task_index": [0, 0, 0, 0],
        }
    )
    _write_table(root / "data/chunk-000/file-000.parquet", frames_tbl)

    info = {
        "codebase_version": "v3.0",
        "fps": 30,
        "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
        "video_path": "videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4",
        "features": {},
        "total_episodes": 2,
        "total_frames": 4,
        "total_tasks": 1,
    }
    (root / "meta").mkdir(parents=True, exist_ok=True)
    (root / "meta" / "info.json").write_text(json.dumps(info), encoding="utf-8")

    tasks_tbl = pa.table({"task_index": [0], "task": ["pick"]})
    _write_table(root / "meta" / "tasks.parquet", tasks_tbl)

    return str(root)


@pytest.fixture
def tiny_lerobot_v3_video(tmp_path):
    """Single-episode dataset with an MP4 shard for ``load_video_frames`` tests."""
    av = pytest.importorskip("av")
    pytest.importorskip("PIL", reason="Pillow required for decoded image rows")

    root = tmp_path / "ds_vid"
    (root / "meta" / "episodes" / "chunk-000").mkdir(parents=True)
    (root / "data" / "chunk-000").mkdir(parents=True)

    video_key = "camera.test"
    video_dir = root / "videos" / video_key / "chunk-000"
    video_dir.mkdir(parents=True)
    shutil.copy(pathlib.Path("tests/assets/sample_video.mp4"), video_dir / "file-000.mp4")

    # The episode's footage starts at the shard's first frame timestamp; frame
    # timestamps in the data parquet are episode-local (start at 0).
    with av.open(str(video_dir / "file-000.mp4")) as c:
        s = c.streams.video[0]
        fps = float(s.average_rate)
        eps_from_ts = None
        for fr in c.decode(s):
            if fr.pts is not None:
                eps_from_ts = float(fr.pts * s.time_base)
                break
        assert eps_from_ts is not None

    n_frames = 3
    timestamps = [i / fps for i in range(n_frames)]

    episodes_tbl = pa.table(
        {
            "episode_index": [0],
            "length": [n_frames],
            "task_index": [0],
            "data/chunk_index": [0],
            "data/file_index": [0],
            "dataset_from_index": [0],
            "dataset_to_index": [n_frames],
            f"videos/{video_key}/chunk_index": [0],
            f"videos/{video_key}/file_index": [0],
            f"videos/{video_key}/from_timestamp": [eps_from_ts],
        }
    )
    _write_table(root / "meta/episodes/chunk-000/file-000.parquet", episodes_tbl)

    frames_tbl = pa.table(
        {
            "index": list(range(n_frames)),
            "episode_index": [0] * n_frames,
            "frame_index": list(range(n_frames)),
            "timestamp": timestamps,
            "task_index": [0] * n_frames,
        }
    )
    _write_table(root / "data/chunk-000/file-000.parquet", frames_tbl)

    info = {
        "codebase_version": "v3.0",
        "fps": fps,
        "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
        "video_path": "videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4",
        "features": {
            video_key: {"dtype": "video"},
        },
        "total_episodes": 1,
        "total_frames": n_frames,
        "total_tasks": 1,
    }
    (root / "meta" / "info.json").write_text(json.dumps(info), encoding="utf-8")

    tasks_tbl = pa.table({"task_index": [0], "task": ["pick"]})
    _write_table(root / "meta" / "tasks.parquet", tasks_tbl)

    return str(root)


def test_read_episodes_and_load_episode_frames(tiny_lerobot_v3):
    ep = read_episodes(tiny_lerobot_v3).sort("episode_index")
    assert ep.count_rows() == 2

    frames = load_episode_frames(ep, tiny_lerobot_v3).sort("index")
    assert frames.count_rows() == 4
    assert set(frames.to_pydict()["episode_index"]) == {0, 1}

    f0 = load_episode_frames(ep.where(daft.col("episode_index") == 0), tiny_lerobot_v3).sort("frame_index")
    assert f0.count_rows() == 2
    assert f0.to_pydict()["episode_index"] == [0, 0]


def test_read_frame_rows(tiny_lerobot_v3):
    df = read(tiny_lerobot_v3).sort("index").collect()
    assert df.count_rows() == 4
    cols = df.column_names
    for expected in ("episode_index", "frame_index", "timestamp"):
        assert expected in cols


def test_read_episodes_meta_and_stats_columns(tiny_lerobot_v3):
    default_cols = read_episodes(tiny_lerobot_v3).column_names
    assert "meta/episode_uuid" not in default_cols
    assert "stats/action_mean" not in default_cols

    with_meta = read_episodes(tiny_lerobot_v3, include_meta=True).column_names
    assert "meta/episode_uuid" in with_meta

    with_stats = read_episodes(tiny_lerobot_v3, include_stats=True).column_names
    assert "stats/action_mean" in with_stats


def test_read_episodes_rejects_non_v3(tmp_path):
    root = tmp_path / "ds_old"
    (root / "meta").mkdir(parents=True)
    info = {"codebase_version": "v2.1", "fps": 30, "features": {}}
    (root / "meta" / "info.json").write_text(json.dumps(info), encoding="utf-8")

    with pytest.raises(ValueError, match="v3"):
        read_episodes(str(root))


def test_read_tasks_parquet(tiny_lerobot_v3):
    t = read_tasks(tiny_lerobot_v3).collect()
    assert t.count_rows() == 1


def test_read_episodes_has_no_dataset_root_column(tiny_lerobot_v3):
    ep = read_episodes(tiny_lerobot_v3)
    assert "lerobot_dataset_root" not in ep.column_names


def test_read_load_video_frames_explicit_key(tiny_lerobot_v3_video):
    df = read(tiny_lerobot_v3_video, load_video_frames=["camera.test"]).select("camera.test").collect()
    assert df.count_rows() == 3
    img0 = df.to_pydict()["camera.test"][0]
    if hasattr(img0, "mode"):
        assert img0.mode == "RGB"
        assert img0.size[0] > 10 and img0.size[1] > 10
    else:
        import numpy as np

        assert isinstance(img0, np.ndarray)
        assert img0.ndim == 3 and img0.shape[2] >= 3


def test_read_load_video_frames_inferred_keys(tiny_lerobot_v3_video):
    df = read(tiny_lerobot_v3_video, load_video_frames=True).collect()
    assert df.count_rows() == 3
    assert "camera.test" in df.column_names
    # Internal per-episode video metadata must not leak into the result.
    assert not any(c.startswith("videos/") for c in df.column_names)
