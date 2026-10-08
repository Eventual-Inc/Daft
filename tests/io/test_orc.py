from __future__ import annotations

import asyncio
import datetime
import threading
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from pyarrow import orc

import daft
from daft.io import _orc
from daft.io._orc import OrcSource, OrcSourceTask
from daft.io.pushdowns import Pushdowns
from daft.pickle import cloudpickle
from daft.recordbatch import RecordBatch


def _write_orc(path: Path, table: pa.Table, **kwargs) -> str:
    orc.write_table(table, path, **kwargs)
    return str(path)


def _native_file_uri(path: Path) -> str:
    prefix = "file:///" if len(path.drive) == 2 and path.drive.endswith(":") else "file://"
    return prefix + path.as_posix()


def _tasks(source: OrcSource, pushdowns: Pushdowns | None = None) -> list[OrcSourceTask]:
    async def collect() -> list[OrcSourceTask]:
        return [task async for task in source.get_tasks(pushdowns or Pushdowns.empty())]

    return asyncio.run(collect())


def _batches(task: OrcSourceTask) -> list[RecordBatch]:
    async def collect() -> list[RecordBatch]:
        return [batch async for batch in task.read()]

    return asyncio.run(collect())


@pytest.fixture
def orc_path(tmp_path: Path) -> str:
    return _write_orc(tmp_path / "data.orc", pa.table({"id": range(9), "name": [f"row-{i}" for i in range(9)]}))


def test_read_orc(orc_path: str) -> None:
    assert daft.read_orc is daft.io.read_orc
    expected = daft.from_arrow(orc.read_table(orc_path)).to_arrow()
    assert daft.read_orc(orc_path, batch_size=2).to_arrow() == expected


@pytest.mark.parametrize("compression", ["uncompressed", "snappy", "zlib", "lz4", "zstd"])
def test_read_orc_compression(tmp_path: Path, compression: str) -> None:
    table = pa.table({"id": [1, 2, None], "value": ["a", None, "汉字"]})
    path = _write_orc(tmp_path / "compressed.orc", table, compression=compression)
    assert daft.read_orc(path).to_pydict() == table.to_pydict()


@pytest.mark.parametrize(
    "dtype,values",
    [
        (pa.int8(), [1, None, -1]),
        (pa.int16(), [1, None, -1]),
        (pa.int32(), [1, None, -1]),
        (pa.int64(), [1, None, -1]),
        (pa.float32(), [1.5, None, -1.5]),
        (pa.float64(), [1.5, None, -1.5]),
        (pa.bool_(), [True, None, False]),
        (pa.string(), ["汉字", None, ""]),
        (pa.binary(), [b"\x00\xff", None, b""]),
        (pa.date32(), [datetime.date(2024, 1, 1), None, datetime.date(1970, 1, 1)]),
        (pa.timestamp("ns"), [1704067200123456789, None, 0]),
        (pa.timestamp("ns", tz="UTC"), [1704067200123456789, None, 0]),
        (pa.decimal128(10, 2), [Decimal("1.25"), None, Decimal("-2.00")]),
        (pa.list_(pa.int64()), [[1, None], None, []]),
        (pa.struct([("x", pa.int64()), ("y", pa.string())]), [{"x": 1, "y": None}, None, {"x": None, "y": ""}]),
        (pa.map_(pa.string(), pa.int64()), [[("a", 1), ("b", None)], None, []]),
        (pa.int64(), [None, None, None]),
    ],
)
def test_read_orc_types(tmp_path: Path, dtype: pa.DataType, values: list) -> None:
    path = _write_orc(tmp_path / "types.orc", pa.table({"value": pa.array(values, type=dtype)}))
    expected = daft.from_arrow(orc.read_table(path)).to_arrow()
    assert daft.read_orc(path, batch_size=1).to_arrow() == expected


@pytest.mark.parametrize("kind", ["directory", "trailing_slash", "glob", "list", "overlap"])
def test_read_orc_paths(tmp_path: Path, kind: str) -> None:
    first = _write_orc(tmp_path / "first.orc", pa.table({"id": [1, 2]}))
    nested = tmp_path / "nested"
    nested.mkdir()
    second = _write_orc(nested / "second.orc", pa.table({"id": [3, 4]}))
    (tmp_path / "unrelated.txt").write_text("not ORC")
    paths = {
        "directory": str(tmp_path),
        "trailing_slash": str(tmp_path) + "/",
        "glob": str(tmp_path / "**" / "*.orc"),
        "list": [first, second],
        "overlap": [first, first, str(tmp_path / "**" / "*.orc")],
    }
    assert daft.read_orc(paths[kind]).sort("id").to_pydict() == {"id": [1, 2, 3, 4]}


@pytest.mark.parametrize("filename", ["data", "data.bin", "data.ORC", "file with spaces.orc", "数据.orc"])
def test_read_orc_exact_file(tmp_path: Path, filename: str) -> None:
    path = _write_orc(tmp_path / filename, pa.table({"id": [1, 2]}))
    assert daft.read_orc(path).to_pydict() == {"id": [1, 2]}
    assert daft.read_orc(_native_file_uri(Path(path))).to_pydict() == {"id": [1, 2]}


@pytest.mark.parametrize("kind", ["path", "file_uri"])
def test_read_orc_glob_character_filename(tmp_path: Path, kind: str) -> None:
    literal_path = tmp_path / "a[1].orc"
    _write_orc(literal_path, pa.table({"id": [1]}))
    _write_orc(tmp_path / "a1.orc", pa.table({"id": [2]}))
    pattern = _native_file_uri(literal_path) if kind == "file_uri" else str(literal_path)
    assert daft.read_orc(pattern).to_pydict() == {"id": [2]}

    escaped_path = tmp_path / "a[[]1[]].orc"
    escaped_pattern = _native_file_uri(escaped_path) if kind == "file_uri" else str(escaped_path)
    assert daft.read_orc(escaped_pattern).to_pydict() == {"id": [1]}


def test_read_orc_empty(tmp_path: Path) -> None:
    table = pa.table({"id": pa.array([], pa.int64()), "name": pa.array([], pa.string())})
    path = _write_orc(tmp_path / "empty.orc", table)
    df = daft.read_orc(path)
    assert df.schema() == daft.from_arrow(table).schema()
    assert df.to_pydict() == {"id": [], "name": []}
    assert df.count_rows() == 0


def test_read_orc_relative_path(orc_path: str, monkeypatch) -> None:
    monkeypatch.chdir(Path(orc_path).parent)
    df = daft.read_orc("data.orc")
    monkeypatch.chdir(Path(orc_path).parent.parent)
    assert df.to_pydict()["id"] == list(range(9))


@pytest.mark.parametrize("authority", ["localhost", "other-host"])
def test_read_orc_file_uri_with_authority(tmp_path: Path, authority: str) -> None:
    table = pa.table({"id": [1, 2]})
    orc_path = tmp_path / "data.orc"
    _write_orc(orc_path, table)
    orc_uri = _native_file_uri(orc_path).replace("file://", f"file://{authority}")
    with pytest.raises(FileNotFoundError):
        daft.read_orc(orc_uri)


@pytest.mark.parametrize("stem", ["a%2A", "a%5B1%5D", "a%20b"])
def test_read_orc_file_uri_with_percent_characters(tmp_path: Path, stem: str) -> None:
    table = pa.table({"id": [1]})
    orc_path = tmp_path / f"{stem}.orc"
    _write_orc(orc_path, table)
    for neighbor in ["ab", "a1", "a b"]:
        _write_orc(tmp_path / f"{neighbor}.orc", pa.table({"id": [2]}))
    assert daft.read_orc(_native_file_uri(orc_path)).to_pydict() == table.to_pydict()


def test_read_orc_file_uri_does_not_decode_missing_path(tmp_path: Path) -> None:
    orc_path = tmp_path / "a b.orc"
    _write_orc(orc_path, pa.table({"id": [1]}))
    with pytest.raises(FileNotFoundError):
        daft.read_orc(orc_path.as_uri())


def test_read_orc_discovered_paths_are_reused(tmp_path: Path) -> None:
    _write_orc(tmp_path / "first.orc", pa.table({"id": [1]}))
    df = daft.read_orc(str(tmp_path / "*.orc"))
    _write_orc(tmp_path / "later.orc", pa.table({"id": [2]}))
    assert df.to_pydict() == {"id": [1]}


def test_read_orc_many_stripes(tmp_path: Path) -> None:
    table = pa.table({"id": range(12000), "value": [f"value-{i:08d}" for i in range(12000)]})
    path = _write_orc(tmp_path / "stripes.orc", table, stripe_size=65536, batch_size=1024)
    assert orc.ORCFile(path).nstripes > 1
    batches = _batches(_tasks(OrcSource(path, None, 257))[0])
    assert all(len(batch) <= 257 for batch in batches)
    assert sum(len(batch) for batch in batches) == len(table)
    assert daft.read_orc(path, batch_size=257).to_pydict() == table.to_pydict()


def test_read_orc_empty_first_file(tmp_path: Path) -> None:
    first = _write_orc(tmp_path / "empty.orc", pa.table({"id": pa.array([], pa.int64())}))
    second = _write_orc(tmp_path / "data.orc", pa.table({"id": [1, 2]}))
    assert daft.read_orc([first, second]).sort("id").to_pydict() == {"id": [1, 2]}


def test_read_orc_schema_alignment(tmp_path: Path) -> None:
    first = _write_orc(tmp_path / "first.orc", pa.table({"id": [1], "name": ["a"]}))
    second = _write_orc(tmp_path / "second.orc", pa.table({"extra": [True], "id": pa.array([2], pa.int32())}))
    third = _write_orc(tmp_path / "third.orc", pa.table({"name": ["c"], "id": [3]}))
    df = daft.read_orc([first, second, third], batch_size=1)
    assert df.schema() == daft.read_orc(first).schema()
    assert df.sort("id").to_pydict() == {"id": [1, 2, 3], "name": ["a", None, "c"]}
    assert df.where(daft.col("name").is_null()).select("id").to_pydict() == {"id": [2]}


def test_read_orc_all_projected_fields_missing(tmp_path: Path) -> None:
    first = _write_orc(tmp_path / "first.orc", pa.table({"id": [1], "name": ["a"]}))
    second = _write_orc(tmp_path / "second.orc", pa.table({"id": [2, 3]}))
    df = daft.read_orc([first, second], batch_size=1)
    assert df.select("name").count_rows() == 3
    assert df.where(daft.col("name").is_null()).count_rows() == 2


def test_read_orc_nested_missing_fields(tmp_path: Path) -> None:
    first = _write_orc(tmp_path / "first.orc", pa.table({"id": [1], "value": [{"a": 1, "b": "x"}]}))
    second = _write_orc(tmp_path / "second.orc", pa.table({"id": [2], "value": [{"a": 2}]}))
    result = daft.read_orc([first, second]).sort("id").to_pydict()
    assert result == {"id": [1, 2], "value": [{"a": 1, "b": "x"}, {"a": 2, "b": None}]}


def test_read_orc_incompatible_schema(tmp_path: Path) -> None:
    first = _write_orc(tmp_path / "first.orc", pa.table({"id": [1]}))
    second = _write_orc(tmp_path / "second.orc", pa.table({"id": [{"value": 2}]}))
    with pytest.raises(Exception, match="(?i)(cast|convert|parse)"):
        daft.read_orc([first, second]).collect()


def test_read_orc_casts_to_inferred_schema(tmp_path: Path) -> None:
    tables = [pa.table({"id": [1], "seq": [1]}), pa.table({"id": ["2", "not-an-integer"], "seq": [2, 3]})]
    orc_paths = [_write_orc(tmp_path / f"{index}.orc", table) for index, table in enumerate(tables)]
    assert daft.read_orc(orc_paths).sort("seq").to_pydict() == {"id": [1, 2, None], "seq": [1, 2, 3]}


def test_read_orc_projection_and_filter(orc_path: str) -> None:
    df = daft.read_orc(orc_path, batch_size=2)
    assert df.select("name", "id").column_names == ["name", "id"]
    result = df.where(daft.col("id") >= 6).select("name").limit(2).sort("name").to_pydict()
    assert result == {"name": ["row-6", "row-7"]}
    assert df.where(daft.col("id") > 100).select("name").to_pydict() == {"name": []}
    assert df.where(daft.col("id") >= 6).count_rows() == 3
    assert df.select(daft.lit(1).alias("constant")).to_pydict() == {"constant": [1] * 9}


@pytest.mark.parametrize("limit", [0, 1, 2, 9, 20])
def test_read_orc_limit(orc_path: str, limit: int) -> None:
    assert daft.read_orc(orc_path, batch_size=2).limit(limit).count_rows() == min(limit, 9)


@pytest.mark.parametrize("batch_size", [0, -1, True, 1.5, "2"])
def test_read_orc_invalid_batch_size(orc_path: str, batch_size) -> None:
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        daft.read_orc(orc_path, batch_size=batch_size)


def test_read_orc_empty_paths(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="empty list of ORC"):
        daft.read_orc([])
    with pytest.raises(FileNotFoundError, match="No ORC files"):
        daft.read_orc(str(tmp_path / "*.orc"))


@pytest.mark.parametrize("kind", ["string", "list", "mixed"])
def test_read_orc_rejects_empty_string_before_io(orc_path: str, monkeypatch, kind: str) -> None:
    monkeypatch.chdir(Path(orc_path).parent)
    paths = {"string": "", "list": [""], "mixed": [orc_path, ""]}

    def unexpected_glob(*args, **kwargs):
        pytest.fail("Empty filepaths must be rejected before filesystem access")

    monkeypatch.setattr(_orc, "glob_path_with_stats", unexpected_glob)
    with pytest.raises(ValueError, match="empty ORC filepath"):
        daft.read_orc(paths[kind])


def test_read_orc_whitespace_filepath(tmp_path: Path, monkeypatch) -> None:
    table = pa.table({"id": [1]})
    _write_orc(tmp_path / "   ", table)
    monkeypatch.chdir(tmp_path)
    assert daft.read_orc("   ").to_pydict() == table.to_pydict()


def test_read_orc_missing_input(orc_path: str, tmp_path: Path) -> None:
    with pytest.raises(Exception, match="(?i)(missing|not found|no such file|No ORC files)"):
        daft.read_orc([orc_path, str(tmp_path / "missing.orc")]).collect()


def test_read_orc_invalid_file(tmp_path: Path) -> None:
    path = tmp_path / "bad.orc"
    path.write_bytes(b"not an ORC file")
    with pytest.raises(OSError, match="bad.orc") as error:
        daft.read_orc(str(path))
    assert error.value.__cause__ is not None


def test_orc_tasks_and_batching(orc_path: str) -> None:
    source = OrcSource([orc_path, orc_path], None, 2)
    tasks = _tasks(source)
    assert len(tasks) == 1
    batches = _batches(tasks[0])
    assert [len(batch) for batch in batches] == [2, 2, 2, 2, 1]
    assert all(batch.schema() == source.schema for batch in batches)
    assert [value for batch in batches for value in batch.to_pydict()["id"]] == list(range(9))


def test_orc_task_projection_retains_filter_columns(orc_path: str) -> None:
    task = _tasks(OrcSource(orc_path, None, 2), Pushdowns(columns=["name"], filters=daft.col("id") >= 6))[0]
    assert task.schema.column_names() == ["id", "name"]
    assert all(batch.schema() == task.schema for batch in _batches(task))


def test_orc_task_empty_projection(orc_path: str) -> None:
    task = _tasks(OrcSource(orc_path, None, 2), Pushdowns(columns=[]))[0]
    batches = _batches(task)
    assert task.schema.column_names() == []
    assert sum(len(batch) for batch in batches) == 9
    assert all(batch.schema().column_names() == [] for batch in batches)


def test_orc_serialization(orc_path: str) -> None:
    source = cloudpickle.loads(cloudpickle.dumps(OrcSource(orc_path, None, 2)))
    task = cloudpickle.loads(cloudpickle.dumps(_tasks(source)[0]))
    assert source.schema == task.schema
    assert sum(len(batch) for batch in _batches(task)) == 9


def test_orc_file_is_opened_at_execution(orc_path: str, monkeypatch) -> None:
    task = _tasks(OrcSource(orc_path, None, 2))[0]

    def fail_open(*args, **kwargs):
        raise PermissionError("read permission denied")

    monkeypatch.setattr(_orc, "open_file", fail_open)
    with pytest.raises(PermissionError, match="read permission denied"):
        _batches(task)


@pytest.mark.parametrize("finish", ["exhaust", "close", "error"])
def test_orc_file_cleanup(orc_path: str, monkeypatch, finish: str) -> None:
    task = _tasks(OrcSource(orc_path, None, 2))[0]
    opened = []
    original = _orc.open_file

    def track_open(*args, **kwargs):
        file = original(*args, **kwargs)
        opened.append(file)
        return file

    monkeypatch.setattr(_orc, "open_file", track_open)

    async def read() -> None:
        if finish == "exhaust":
            assert sum([len(batch) async for batch in task.read()]) == 9
        elif finish == "close":
            iterator = task.read()
            assert len(await anext(iterator)) == 2
            assert not opened[0].closed
            await iterator.aclose()
        else:
            Path(orc_path).write_bytes(b"corrupt after inference")
            with pytest.raises(OSError):
                await anext(task.read())

    asyncio.run(read())
    assert opened and all(file.closed for file in opened)


def test_orc_task_cancellation_waits_for_reader(orc_path: str, monkeypatch) -> None:
    entered = threading.Event()
    release = threading.Event()
    closed = threading.Event()

    def blocking_batches(*args, **kwargs):
        try:
            entered.set()
            assert release.wait(timeout=10)
            yield RecordBatch.empty()
        finally:
            closed.set()

    monkeypatch.setattr(_orc, "_iter_orc_batches", blocking_batches)
    task = _tasks(OrcSource(orc_path, None, 2))[0]

    async def read() -> None:
        pending = asyncio.create_task(anext(task.read()))
        assert await asyncio.to_thread(entered.wait, 10)
        pending.cancel()
        await asyncio.sleep(0)
        assert not closed.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending

    try:
        asyncio.run(read())
        assert closed.is_set()
    finally:
        release.set()
