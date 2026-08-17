from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import textwrap

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

import daft
from daft.datasets.lerobot import read
from daft.io.lerobot.sink import LeRobotSink
from daft.recordbatch import MicroPartition


def _frame_df() -> daft.DataFrame:
    table = pa.table(
        {
            # Deliberately unordered: the writer owns canonical ordering.
            "episode_index": pa.array([1, 0, 0, 1], type=pa.int64()),
            "frame_index": pa.array([1, 1, 0, 0], type=pa.int64()),
            "task": ["place", "pick", "pick", "place"],
            "action": pa.array([[0.3, 0.4], [0.1, 0.2], [0.0, 0.1], [0.2, 0.3]], type=pa.list_(pa.float32(), 2)),
            "success": pa.array([True, False, False, True]),
        }
    )
    return daft.from_arrow(table)


def test_write_lerobot_round_trip(tmp_path):
    root = tmp_path / "dataset"

    result = _frame_df().write_lerobot(root, fps=10, robot_type="test-bot").to_pydict()

    assert result == {
        "path": [str(root)],
        "num_episodes": [2],
        "num_frames": [4],
        "num_tasks": [2],
    }

    info = json.loads((root / "meta" / "info.json").read_text())
    assert info["codebase_version"] == "v3.0"
    assert info["robot_type"] == "test-bot"
    assert info["video_path"] is None
    assert info["total_episodes"] == 2
    assert info["total_frames"] == 4
    assert info["total_tasks"] == 2
    assert info["splits"] == {"train": "0:2"}
    assert info["features"]["action"] == {"dtype": "float32", "shape": [2], "names": None}
    assert info["features"]["success"] == {"dtype": "bool", "shape": [1], "names": None}

    frames = pq.read_table(root / "data/chunk-000/file-000.parquet").to_pydict()
    assert frames["episode_index"] == [0, 0, 1, 1]
    assert frames["frame_index"] == [0, 1, 0, 1]
    assert frames["index"] == [0, 1, 2, 3]
    assert frames["task_index"] == [0, 0, 1, 1]
    assert frames["timestamp"] == pytest.approx([0.0, 0.1, 0.0, 0.1])
    assert frames["action"] == [
        [0.0, pytest.approx(0.1)],
        [pytest.approx(0.1), pytest.approx(0.2)],
        [pytest.approx(0.2), pytest.approx(0.3)],
        [pytest.approx(0.3), pytest.approx(0.4)],
    ]

    episodes = pq.read_table(root / "meta/episodes/chunk-000/file-000.parquet").to_pydict()
    assert episodes["length"] == [2, 2]
    assert episodes["dataset_from_index"] == [0, 2]
    assert episodes["dataset_to_index"] == [2, 4]
    assert episodes["tasks"] == [["pick"], ["place"]]
    assert episodes["stats/action/count"] == [[2], [2]]
    assert episodes["meta/episodes/chunk_index"] == [0, 0]

    tasks = pq.read_table(root / "meta/tasks.parquet").to_pydict()
    assert tasks == {"task_index": [0, 1], "task": ["pick", "place"]}

    stats = json.loads((root / "meta" / "stats.json").read_text())
    assert stats["action"]["count"] == [4]
    assert stats["action"]["min"] == pytest.approx([0.0, 0.1])
    assert stats["action"]["max"] == pytest.approx([0.3, 0.4])

    if os.getenv("DAFT_RUNNER") == "ray":
        # Exercise Daft's LeRobot reader as well as direct Parquet validation.
        round_tripped = read(str(root)).sort("index").to_pydict()
        assert round_tripped["episode_index"] == [0, 0, 1, 1]
        assert round_tripped["frame_index"] == [0, 1, 0, 1]
        assert round_tripped["action"] == frames["action"]


def test_write_lerobot_casts_explicit_fixed_shape_feature(tmp_path):
    table = pa.table(
        {
            "episode_index": [0, 0],
            "frame_index": [0, 1],
            "task": ["pick", "pick"],
            "action": pa.array([[1, 2], [3, 4]], type=pa.list_(pa.int64())),
        }
    )

    daft.from_arrow(table).write_lerobot(
        tmp_path / "dataset",
        fps=30,
        features={"action": {"dtype": "float32", "shape": [2], "names": ["x", "y"]}},
    )

    info = json.loads((tmp_path / "dataset/meta/info.json").read_text())
    assert info["features"]["action"] == {"dtype": "float32", "shape": [2], "names": ["x", "y"]}
    written = pq.read_table(tmp_path / "dataset/data/chunk-000/file-000.parquet").to_pydict()["action"]
    assert written == [[1.0, 2.0], [3.0, 4.0]]


def test_write_lerobot_normalizes_one_element_fixed_size_list_to_scalar(tmp_path):
    root = tmp_path / "dataset"
    table = pa.table(
        {
            "episode_index": [0, 0],
            "frame_index": [0, 1],
            "task": ["pick", "pick"],
            "scalar": pa.array([1.0, 2.0], type=pa.float32()),
            "vector": pa.array([[1.0], [2.0]], type=pa.list_(pa.float32(), 1)),
        }
    )

    daft.from_arrow(table).write_lerobot(root, fps=30)

    info = json.loads((root / "meta/info.json").read_text())
    assert info["features"]["scalar"] == {"dtype": "float32", "shape": [1], "names": None}
    assert info["features"]["vector"] == {"dtype": "float32", "shape": [1], "names": None}
    written = pq.read_table(root / "data/chunk-000/file-000.parquet")
    assert written.schema.field("scalar").type == pa.float32()
    assert written.schema.field("vector").type == pa.float32()
    assert written["vector"].to_pylist() == [1.0, 2.0]


@pytest.mark.parametrize("shape", [[1], [2, 2]])
def test_write_lerobot_normalizes_fixed_shape_tensor_storage(tmp_path, shape):
    root = tmp_path / "dataset"
    tensor_type = pa.fixed_shape_tensor(pa.float32(), shape)
    width = math.prod(shape)
    storage = pa.array(
        [list(map(float, range(width))), list(map(float, range(width, 2 * width)))],
        type=tensor_type.storage_type,
    )
    tensor = pa.ExtensionArray.from_storage(tensor_type, storage)
    table = pa.table(
        {
            "episode_index": [0, 0],
            "frame_index": [0, 1],
            "task": ["pick", "pick"],
            "observation": tensor,
        }
    )

    daft.from_arrow(table).write_lerobot(root, fps=30)

    info = json.loads((root / "meta/info.json").read_text())
    assert info["features"]["observation"] == {"dtype": "float32", "shape": shape, "names": None}
    table = pq.read_table(root / "data/chunk-000/file-000.parquet")
    written = table["observation"].to_pylist()
    if shape == [1]:
        assert table.schema.field("observation").type == pa.float32()
        assert written == [0.0, 1.0]
    else:
        assert written == [[[0.0, 1.0], [2.0, 3.0]], [[4.0, 5.0], [6.0, 7.0]]]


@pytest.mark.parametrize("storage_kind", ["list", "tensor"])
def test_write_lerobot_one_element_container_preserves_parent_null(tmp_path, storage_kind):
    if storage_kind == "list":
        values = pa.array([None], type=pa.list_(pa.float32(), 1))
    else:
        tensor_type = pa.fixed_shape_tensor(pa.float32(), [1])
        values = pa.ExtensionArray.from_storage(tensor_type, pa.array([None], type=tensor_type.storage_type))
    table = pa.table(
        {
            "episode_index": [0],
            "frame_index": [0],
            "task": ["pick"],
            "observation": values,
        }
    )

    with pytest.raises(ValueError, match="contains null values"):
        daft.from_arrow(table).write_lerobot(tmp_path / "dataset", fps=30)


def test_write_lerobot_rejects_mismatched_fixed_shape_tensor_descriptor(tmp_path):
    tensor_type = pa.fixed_shape_tensor(pa.float32(), [2, 2])
    storage = pa.array([[0.0, 1.0, 2.0, 3.0]], type=tensor_type.storage_type)
    table = pa.table(
        {
            "episode_index": [0],
            "frame_index": [0],
            "task": ["pick"],
            "observation": pa.ExtensionArray.from_storage(tensor_type, storage),
        }
    )

    with pytest.raises(ValueError, match=r"FixedShapeTensor shape \[2, 2\].*declared LeRobot shape \[4\]"):
        daft.from_arrow(table).write_lerobot(
            tmp_path / "dataset",
            fps=30,
            features={"observation": {"dtype": "float32", "shape": [4], "names": None}},
        )


@pytest.mark.parametrize(
    ("episode_index", "frame_index", "message"),
    [
        ([1], [0], "episode_index must be zero-based"),
        ([0, 0], [0, 2], "frame_index for episode 0 must be zero-based"),
        ([0, 2], [0, 0], "episode_index must be zero-based"),
    ],
)
def test_write_lerobot_rejects_invalid_episode_frames(tmp_path, episode_index, frame_index, message):
    df = daft.from_pydict(
        {
            "episode_index": episode_index,
            "frame_index": frame_index,
            "task": ["pick"] * len(episode_index),
            "action": [1.0] * len(episode_index),
        }
    )

    with pytest.raises(ValueError, match=message):
        df.write_lerobot(tmp_path / "invalid", fps=30)


def test_write_lerobot_requires_overwrite_for_existing_destination(tmp_path):
    root = tmp_path / "dataset"
    _frame_df().write_lerobot(root, fps=10)

    with pytest.raises(FileExistsError, match="overwrite=True"):
        _frame_df().write_lerobot(root, fps=10)

    result = _frame_df().write_lerobot(root, fps=10, overwrite=True).to_pydict()
    assert result["num_frames"] == [4]


def test_write_lerobot_overwrite_validation_failure_preserves_destination(tmp_path):
    root = tmp_path / "dataset"
    _frame_df().write_lerobot(root, fps=10)
    original_info = (root / "meta/info.json").read_bytes()
    original_frames = pq.read_table(root / "data/chunk-000/file-000.parquet").to_pydict()

    invalid = daft.from_pydict(
        {
            "episode_index": [1],
            "frame_index": [0],
            "task": ["invalid"],
            "action": [999.0],
        }
    )
    with pytest.raises(ValueError, match="episode_index must be zero-based"):
        invalid.write_lerobot(root, fps=10, overwrite=True)

    assert (root / "meta/info.json").read_bytes() == original_info
    assert pq.read_table(root / "data/chunk-000/file-000.parquet").to_pydict() == original_frames
    assert not list(tmp_path.glob(".dataset.daft-lr-*-*"))


def test_write_lerobot_refuses_destructive_local_destinations_before_delete(tmp_path, monkeypatch):
    working_directory = tmp_path / "working"
    working_directory.mkdir()
    sentinel = working_directory / "keep-me"
    sentinel.write_text("safe")
    monkeypatch.chdir(working_directory)

    for unsafe_path in (working_directory, working_directory / "child" / "..", working_directory.parent):
        sink = LeRobotSink(str(unsafe_path), 30, {}, None, True, 1000, 100, None)
        with pytest.raises(ValueError, match="unsafe local path"):
            sink.start()
        assert sentinel.read_text() == "safe"


def test_write_lerobot_refuses_symlink_destination(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    sentinel = target / "keep-me"
    sentinel.write_text("safe")
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)

    sink = LeRobotSink(str(link), 30, {}, None, True, 1000, 100, None)
    with pytest.raises(ValueError, match="symlink"):
        sink.start()
    assert sentinel.read_text() == "safe"


def test_write_lerobot_allows_symlink_ancestor(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)

    _frame_df().write_lerobot(link / "dataset", fps=10)

    assert (target / "dataset/meta/info.json").exists()


def test_write_lerobot_refuses_to_overwrite_non_lerobot_directory(tmp_path):
    root = tmp_path / "ordinary-directory"
    root.mkdir()
    sentinel = root / "keep-me"
    sentinel.write_text("safe")

    with pytest.raises(ValueError, match="non-LeRobot destination"):
        _frame_df().write_lerobot(root, fps=10, overwrite=True)

    assert sentinel.read_text() == "safe"


@pytest.mark.parametrize("marker", ["not-json", '{"codebase_version": "v3.0"}'])
def test_write_lerobot_refuses_invalid_info_marker(tmp_path, marker):
    root = tmp_path / "ordinary-directory"
    (root / "meta").mkdir(parents=True)
    (root / "meta/info.json").write_text(marker)
    sentinel = root / "keep-me"
    sentinel.write_text("safe")

    with pytest.raises(ValueError, match="non-LeRobot destination"):
        _frame_df().write_lerobot(root, fps=10, overwrite=True)

    assert sentinel.read_text() == "safe"
    assert (root / "meta/info.json").read_text() == marker


def test_write_lerobot_refuses_object_store_bucket_root():
    with pytest.raises(ValueError, match="bucket root"):
        LeRobotSink._validate_destination("bucket", pafs._MockFileSystem())


def test_write_lerobot_object_store_backup_failure_preserves_destination(monkeypatch):
    filesystem = pafs._MockFileSystem()
    root = "bucket/dataset"
    sink = LeRobotSink("s3://bucket/dataset", 30, {}, None, True, 1000, 100, None)
    transaction = sink._transaction_path(root)
    backup = sink._sibling_path(root, "backup")

    old_info = json.dumps(
        {
            "codebase_version": "v3.0",
            "fps": 30,
            "features": {},
            "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
            "total_episodes": 0,
            "total_frames": 0,
            "total_tasks": 0,
        }
    ).encode()
    for path, content in (
        (f"{root}/meta/info.json", old_info),
        (f"{root}/data/keep", b"intact"),
        (f"{transaction}/meta/info.json", b'{"version": "new"}'),
    ):
        filesystem.create_dir(path.rsplit("/", 1)[0], recursive=True)
        with filesystem.open_output_stream(path) as stream:
            stream.write(content)

    original_copy = sink._copy_path

    def fail_during_backup(filesystem, source, destination, *, commit_marker_last=False):
        if destination == backup:
            filesystem.create_dir(backup, recursive=True)
            with filesystem.open_output_stream(f"{backup}/partial") as stream:
                stream.write(b"partial")
            raise OSError("injected backup copy failure")
        return original_copy(filesystem, source, destination, commit_marker_last=commit_marker_last)

    monkeypatch.setattr(sink, "_copy_path", fail_during_backup)

    with pytest.raises(OSError, match="injected backup copy failure"):
        sink._publish(filesystem, root)

    with filesystem.open_input_stream(f"{root}/meta/info.json") as stream:
        assert stream.read() == old_info
    with filesystem.open_input_stream(f"{root}/data/keep") as stream:
        assert stream.read() == b"intact"
    assert filesystem.get_file_info(backup).type == pafs.FileType.NotFound


def test_write_lerobot_rejects_unsupported_video_feature(tmp_path):
    df = daft.from_pydict(
        {
            "episode_index": [0],
            "frame_index": [0],
            "task": ["pick"],
            "camera": [b"not-a-frame"],
        }
    )

    with pytest.raises(NotImplementedError, match="video.*camera"):
        df.write_lerobot(
            tmp_path / "dataset",
            fps=30,
            features={"camera": {"dtype": "video", "shape": [3, 32, 32], "names": None}},
        )


def test_write_lerobot_rejects_invalid_timestamp(tmp_path):
    df = daft.from_pydict(
        {
            "episode_index": [0, 0],
            "frame_index": [0, 1],
            "timestamp": [0.0, 0.5],
            "task": ["pick", "pick"],
            "action": [1.0, 2.0],
        }
    )

    with pytest.raises(ValueError, match="frame_index / fps"):
        df.write_lerobot(tmp_path / "dataset", fps=10)


def test_write_lerobot_accepts_generated_float32_timestamp_for_long_episode(tmp_path):
    num_frames = 100_001
    df = daft.from_pydict(
        {
            "episode_index": [0] * num_frames,
            "frame_index": list(range(num_frames)),
            "task": ["long"] * num_frames,
        }
    )

    df.write_lerobot(tmp_path / "dataset", fps=3)

    frames = pq.read_table(tmp_path / "dataset/data/chunk-000/file-000.parquet", columns=["timestamp"])
    assert frames["timestamp"][-1].as_py() == pa.scalar(100_000 / 3, pa.float32()).as_py()


def test_write_lerobot_rejects_non_integer_indices(tmp_path):
    df = daft.from_pydict(
        {
            "episode_index": [0.0],
            "frame_index": [0],
            "task": ["pick"],
            "action": [1.0],
        }
    )

    with pytest.raises(ValueError, match="episode_index must have an integer type"):
        df.write_lerobot(tmp_path / "dataset", fps=10)


def test_write_lerobot_parquet_has_one_row_group_per_episode(tmp_path):
    root = tmp_path / "dataset"
    _frame_df().write_lerobot(root, fps=10)

    parquet = pq.ParquetFile(root / "data/chunk-000/file-000.parquet")
    assert parquet.num_row_groups == 2


def test_write_lerobot_tasks_parquet_has_named_pandas_index(tmp_path):
    pandas = pytest.importorskip("pandas")
    root = tmp_path / "dataset"
    _frame_df().write_lerobot(root, fps=10)

    tasks = pandas.read_parquet(root / "meta/tasks.parquet")

    assert tasks.index.name == "task"
    assert tasks.index.tolist() == ["pick", "place"]
    assert tasks.columns.tolist() == ["task_index"]
    assert tasks["task_index"].tolist() == [0, 1]


def test_write_lerobot_does_not_require_numpy_or_pandas(tmp_path):
    root = tmp_path / "dataset"
    script = textwrap.dedent(
        f"""
        import builtins
        from pathlib import Path

        real_import = builtins.__import__

        def block_optional_dependencies(name, *args, **kwargs):
            if name.split(".")[0] in {{"numpy", "pandas"}}:
                raise ImportError(f"blocked optional dependency: {{name}}")
            return real_import(name, *args, **kwargs)

        builtins.__import__ = block_optional_dependencies

        import daft

        daft.from_pydict(
            {{
                "episode_index": [0],
                "frame_index": [0],
                "task": ["pick"],
                "action": [1.0],
            }}
        ).write_lerobot(Path({str(root)!r}), fps=10)
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "DAFT_RUNNER": "native"},
    )

    assert result.returncode == 0, result.stderr
    assert (root / "meta/info.json").exists()


def test_write_lerobot_aggregates_conservative_quantile_bounds(tmp_path):
    root = tmp_path / "dataset"
    daft.from_pydict(
        {
            "episode_index": [0, 1, 1, 1],
            "frame_index": [0, 0, 1, 2],
            "task": ["task"] * 4,
            "action": [0.0, 10.0, 20.0, 30.0],
        }
    ).write_lerobot(root, fps=10)

    stats = json.loads((root / "meta/stats.json").read_text())
    assert stats["action"]["q01"] == pytest.approx([0.0])
    assert stats["action"]["q50"] == pytest.approx([0.0])
    assert stats["action"]["q90"] == pytest.approx([28.0])
    assert stats["action"]["q99"] == pytest.approx([29.8])


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_write_lerobot_rejects_non_finite_feature_values(tmp_path, value):
    df = daft.from_pydict(
        {
            "episode_index": [0],
            "frame_index": [0],
            "task": ["task"],
            "action": [value],
        }
    )

    with pytest.raises(ValueError, match="non-finite values"):
        df.write_lerobot(tmp_path / "dataset", fps=10)


def test_write_lerobot_stats_handle_extreme_finite_values(tmp_path):
    root = tmp_path / "dataset"
    daft.from_pydict(
        {
            "episode_index": [0, 0, 1, 1],
            "frame_index": [0, 1, 0, 1],
            "task": ["task"] * 4,
            "action": [-1e308, 1e308, -1e308, 1e308],
        }
    ).write_lerobot(root, fps=10)

    stats = json.loads((root / "meta/stats.json").read_text())
    assert stats["action"]["mean"] == [0.0]
    assert math.isinf(stats["action"]["std"][0])


@pytest.mark.parametrize(
    ("arrow_type", "value"),
    [
        (pa.int64(), 2**53 + 1),
        (pa.uint64(), 2**63 + 1),
    ],
)
def test_write_lerobot_stats_accept_large_integer_features(tmp_path, arrow_type, value):
    root = tmp_path / "dataset"
    table = pa.table(
        {
            "episode_index": [0],
            "frame_index": [0],
            "task": ["task"],
            "value": pa.array([value], type=arrow_type),
        }
    )

    daft.from_arrow(table).write_lerobot(root, fps=10)

    frames = pq.read_table(root / "data/chunk-000/file-000.parquet")
    assert frames["value"][0].as_py() == value
    stats = json.loads((root / "meta/stats.json").read_text())
    assert stats["value"]["count"] == [1]


def test_write_lerobot_rejects_slash_in_feature_name(tmp_path):
    df = daft.from_pydict(
        {
            "episode_index": [0],
            "frame_index": [0],
            "task": ["pick"],
            "observation/state": [1.0],
        }
    )

    with pytest.raises(ValueError, match="feature names cannot contain"):
        df.write_lerobot(tmp_path / "dataset", fps=10)


@pytest.mark.skipif(os.getenv("DAFT_RUNNER") == "ray", reason="exercises the native streaming fallback")
def test_write_lerobot_native_streams_prepared_partitions(tmp_path, monkeypatch):
    table = _frame_df().to_arrow()
    df = daft.DataFrame._from_micropartitions(
        MicroPartition.from_arrow(table.slice(0, 2)),
        MicroPartition.from_arrow(table.slice(2, 2)),
    )

    def fail_to_arrow(*args, **kwargs):
        raise AssertionError("write_lerobot must not materialize the full DataFrame with to_arrow")

    monkeypatch.setattr(daft.DataFrame, "to_arrow", fail_to_arrow)
    df.write_lerobot(tmp_path / "dataset", fps=10)

    assert pq.read_metadata(tmp_path / "dataset/data/chunk-000/file-000.parquet").num_rows == 4


@pytest.mark.skipif(os.getenv("DAFT_RUNNER") != "ray", reason="exercises the distributed Ray sink path")
def test_write_lerobot_merges_episodes_across_partitions(tmp_path):
    root = tmp_path / "dataset"
    episode_index = [0] * 8 + [1] * 8
    frame_index = list(range(8)) * 2
    # Repartition before writing so Ray has several independently executing
    # sink tasks, including an episode boundary that is not a partition boundary.
    df = daft.from_pydict(
        {
            "episode_index": episode_index,
            "frame_index": frame_index,
            "task": ["pick"] * 8 + ["place"] * 8,
            "action": [float(value) for value in range(16)],
        }
    ).repartition(3)

    result = df.write_lerobot(root, fps=20).to_pydict()

    assert result["num_episodes"] == [2]
    assert result["num_frames"] == [16]
    frames = pq.read_table(root / "data/chunk-000/file-000.parquet").to_pydict()
    assert frames["episode_index"] == episode_index
    assert frames["frame_index"] == frame_index
    assert frames["index"] == list(range(16))
