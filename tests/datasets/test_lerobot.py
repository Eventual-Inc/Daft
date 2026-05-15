from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from daft.datasets.lerobot import episodes, load_episode_frames, read_info, read_stats, read_tasks


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
        "features": {},
        "total_episodes": 2,
        "total_frames": 4,
        "total_tasks": 1,
    }
    (root / "meta").mkdir(parents=True, exist_ok=True)
    (root / "meta" / "info.json").write_text(json.dumps(info), encoding="utf-8")
    (root / "meta" / "stats.json").write_text(json.dumps({"ok": True}), encoding="utf-8")

    tasks_tbl = pa.table({"task_index": [0], "task": ["pick"]})
    _write_table(root / "meta" / "tasks.parquet", tasks_tbl)

    return str(root)


def test_episodes_and_load_episode_frames(tiny_lerobot_v3):
    ep = episodes(tiny_lerobot_v3).sort("episode_index")
    assert ep.count_rows() == 2
    assert "lerobot_dataset_root" in ep.column_names

    frames = load_episode_frames(ep).sort("index")
    assert frames.count_rows() == 4
    assert set(frames.to_pydict()["episode_index"]) == {0, 1}

    f0 = load_episode_frames(ep.where(daft.col("episode_index") == 0)).sort("frame_index")
    assert f0.count_rows() == 2
    assert f0.to_pydict()["episode_index"] == [0, 0]


def test_read_info_and_stats(tiny_lerobot_v3):
    info = read_info(tiny_lerobot_v3)
    assert info["total_episodes"] == 2
    assert read_stats(tiny_lerobot_v3) == {"ok": True}


def test_read_tasks_parquet(tiny_lerobot_v3):
    t = read_tasks(tiny_lerobot_v3).collect()
    assert t.count_rows() == 1


def test_load_episode_frames_requires_root(tiny_lerobot_v3):
    ep = daft.read_parquet(f"{tiny_lerobot_v3}/meta/episodes/**/*.parquet")
    with pytest.raises(ValueError, match="lerobot_dataset_root"):
        load_episode_frames(ep)
