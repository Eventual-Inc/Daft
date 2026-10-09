from __future__ import annotations

import asyncio
import datetime
import math
import threading
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote

import pyarrow as pa
import pytest
from pyarrow import orc

import daft
from daft.daft import _infer_hive_partition_schema, _parse_hive_partition_values
from daft.exceptions import DaftCoreException
from daft.io import _orc
from daft.io._orc import OrcSource, OrcSourceTask
from daft.io.pushdowns import Pushdowns
from daft.pickle import cloudpickle
from daft.recordbatch import RecordBatch
from daft.schema import Schema
from daft.series import Series


def _write_hive_orc(root: Path, directory: str, ids: list[int] | pa.Array, **columns) -> str:
    path = root / directory / "data.orc"
    path.parent.mkdir(parents=True, exist_ok=True)
    return _write_orc(path, pa.table({"id": ids, **columns}))


def _hive_value(value: str, dtype: pa.DataType) -> Series:
    schema = Schema.from_pyarrow_schema(pa.schema([pa.field("p", dtype)]))
    values = _parse_hive_partition_values(f"/p={quote(value, safe='')}/data.orc", schema._schema)
    return Series._from_pyseries(values[0])


@pytest.mark.parametrize(
    "path,expected",
    [
        ("s3://bucket/year=2024/plain/month=03/data.orc", {"year": "2024", "month": "03"}),
        (r"C:\year=2024\month=03\data.orc", {"year": "2024", "month": "03"}),
        ("/p=first/p=last/q=x/data.orc", {"p": "last", "q": "x"}),
        ("/p=/q=__HIVE_DEFAULT_PARTITION__/data.orc", {"p": None, "q": None}),
        ("/p=null/q=None/data.orc", {"p": "null", "q": "None"}),
        ("/key%3Dname=a%2Fb/city=%E5%8C%97%E4%BA%AC/p=a+b/data.orc", {"key=name": "a/b", "city": "北京", "p": "a+b"}),
        ("/p=%252F/data.orc", {"p": "%2F"}),
        ("/invalid==value/=empty/p=ok/data.orc", {"p": "ok"}),
        ("/p=x/data.orc?query=other", {"p": "x"}),
        ("/p=x/\nq=y/data.orc", {"p": "x"}),
        ("/plain/p=filename.orc", {}),
        ("", {}),
    ],
)
def test_orc_hive_directory_parser(path: str, expected: dict[str, str | None]) -> None:
    schema = Schema._from_pyschema(_infer_hive_partition_schema(path))
    assert schema.column_names() == list(expected)
    string_schema = Schema.from_field_name_and_types([(key, daft.DataType.string()) for key in expected])
    values = [Series._from_pyseries(value) for value in _parse_hive_partition_values(path, string_schema._schema)]
    assert {value.name(): value.to_pylist()[0] for value in values} == expected


def test_orc_hive_invalid_utf8() -> None:
    with pytest.raises(DaftCoreException, match="(?i)utf-8"):
        _infer_hive_partition_schema("/p=%FF/data.orc")
    schema = Schema.from_field_name_and_types([("p", daft.DataType.string())])
    with pytest.raises(DaftCoreException, match="(?i)utf-8"):
        _parse_hive_partition_values("/p=%FF/data.orc", schema._schema)


@pytest.mark.parametrize(
    "value,dtype,physical",
    [
        ("03", pa.int64(), 3),
        ("+0003", pa.int64(), 3),
        ("-9223372036854775808", pa.int64(), -(2**63)),
        ("9223372036854775808", pa.float64(), float(2**63)),
        ("1.5", pa.float64(), 1.5),
        ("1e3", pa.float64(), 1000.0),
        ("inf", pa.float64(), math.inf),
        ("1e400", pa.float64(), math.inf),
        ("TRUE", pa.bool_(), True),
        ("False", pa.bool_(), False),
        ("2024-01-01", pa.date32(), 19723),
        ("2024/01/01", pa.date32(), 19723),
        ("12:30:00", pa.time64("us"), 45000000000),
        ("12:30:00.123456789", pa.time64("ns"), 45000123456789),
        ("2024-01-01T00:00:00.000", pa.timestamp("s"), 1704067200),
        ("2024-01-01 00:00:00.123", pa.timestamp("ms"), 1704067200123),
        ("2024-01-01T00:00:00.123456", pa.timestamp("us"), 1704067200123456),
        ("2024-01-01T00:00:00.123456789", pa.timestamp("ns"), 1704067200123456789),
        ("2024-01-01T08:00:00.123456789+08:00", pa.timestamp("ns", "+08:00"), 1704067200123456789),
        ("2024-01-01T00:00:00Z", pa.timestamp("s", "+00:00"), 1704067200),
        ("1969-12-31T23:59:59.123456789", pa.timestamp("ns"), -876543211),
        ("2500-01-01T00:00:00", pa.timestamp("s"), 16725225600),
        ("2500-01-01T00:00:00.123456789", pa.timestamp("ns"), None),
        ("hello", pa.string(), "hello"),
        ("İNF", pa.string(), "İNF"),
        ("1_000", pa.string(), "1_000"),
        (" 3 ", pa.string(), " 3 "),
        ("2024-02-30", pa.string(), "2024-02-30"),
        ("__HIVE_DEFAULT_PARTITION__", pa.string(), None),
    ],
)
def test_read_orc_hive_types(tmp_path: Path, value: str, dtype: pa.DataType, physical) -> None:
    path = _write_hive_orc(tmp_path, f"p={quote(value, safe='')}", [1, 2])
    df = daft.read_orc(path, hive_partitioning=True, batch_size=1)
    result = df.select("p").to_arrow().column("p")
    assert result.type == (pa.large_string() if pa.types.is_string(dtype) else dtype)
    if pa.types.is_temporal(dtype):
        result = result.cast(pa.int32() if pa.types.is_date32(dtype) else pa.int64())
    assert result.to_pylist() == [physical, physical]


@pytest.mark.parametrize(
    "first,second,expected",
    [
        ("03", "abc", [3, None]),
        ("03", "9223372036854775808", [3, None]),
        ("03", " 4 ", [3, None]),
        ("true", "1", [True, None]),
        ("true", "FALSE", [True, False]),
        ("1.5", "bad", [1.5, None]),
        ("__HIVE_DEFAULT_PARTITION__", "2024", [None, "2024"]),
        ("2024-01-01", "2024-02-30", [datetime.date(2024, 1, 1), None]),
    ],
)
def test_read_orc_hive_fixed_types(tmp_path: Path, first: str, second: str, expected: list) -> None:
    paths = [
        _write_hive_orc(tmp_path, f"p={quote(first, safe='')}", [1]),
        _write_hive_orc(tmp_path, f"p={quote(second, safe='')}", [2]),
    ]
    assert daft.read_orc(paths, hive_partitioning=True).sort("id").to_pydict()["p"] == expected


def test_orc_hive_nan_and_conversion_errors(tmp_path: Path) -> None:
    path = _write_hive_orc(tmp_path, "p=NaN", [1])
    df = daft.read_orc(path, hive_partitioning=True)
    assert df.schema()["p"].dtype == daft.DataType.float64()
    assert math.isnan(df.to_pydict()["p"][0])
    with pytest.raises(DaftCoreException, match="Deserializing type"):
        _hive_value("value", pa.list_(pa.int64()))
    assert _hive_value("0" * 5000 + "3", pa.int64()).to_pylist() == [3]


def test_orc_hive_temporal_fixed_precision_and_failures(tmp_path: Path) -> None:
    paths = [
        _write_hive_orc(tmp_path, "p=2024-01-01T00%3A00%3A00.123", [1]),
        _write_hive_orc(tmp_path, "p=2024-01-01T00%3A00%3A00.123456789", [2]),
        _write_hive_orc(tmp_path, "p=2024-01-01T25%3A00%3A00", [3]),
    ]
    result = daft.read_orc(paths, hive_partitioning=True).sort("id").to_arrow().column("p")
    assert result.type == pa.timestamp("ms")
    assert result.cast(pa.int64()).to_pylist() == [1704067200123, 1704067200123, None]
    assert _hive_value("12:00:00", pa.timestamp("s")).to_pylist() == [None]
    assert _hive_value("2024-01-01T00:00:00Z", pa.timestamp("s")).to_pylist() == [None]


def test_orc_hive_duplicate_keys_and_field_order(tmp_path: Path) -> None:
    path = _write_hive_orc(tmp_path, "p=1/q=x/p=3", [1], p=[99], z=[True])
    df = daft.read_orc(path, hive_partitioning=True)
    assert df.column_names == ["id", "p", "z", "q"]
    assert df.to_pydict() == {"id": [1], "p": [3], "z": [True], "q": ["x"]}


@pytest.mark.parametrize(
    "value,dtype,ticks",
    [
        ("12:30", pa.time64("us"), 45000000000),
        (" 12 : 30:00 ", pa.time64("us"), 45000000000),
        (" 12 : 30:00.123456789 ", pa.time64("ns"), 45000123456789),
        ("2024-01-01  00:00:00", pa.timestamp("s"), 1704067200),
        ("2024-01-01\t00:00:00", pa.timestamp("s"), 1704067200),
        ("2024-01-0100:00:00", pa.timestamp("s"), 1704067200),
        ("2024-01-01T08:00:00+0800", pa.timestamp("s", "+08:00"), 1704067200),
        ("2024-01-01T00:00:00-05:00", pa.timestamp("s", "-05:00"), 1704085200),
        ("2024-01-01 00:00:00Z", pa.timestamp("s", "+00:00"), 1704067200),
        ("2024-01-01t00:00:00z", pa.timestamp("s", "+00:00"), 1704067200),
        ("2024-01-01t00:00:00", pa.string(), "2024-01-01t00:00:00"),
        ("2024-01-01T23:59:60Z", pa.timestamp("s", "+00:00"), 1704153599),
    ],
)
def test_read_orc_hive_temporal_formats(tmp_path: Path, value: str, dtype: pa.DataType, ticks) -> None:
    path = _write_hive_orc(tmp_path, f"p={quote(value, safe='')}", [1])
    result = daft.read_orc(path, hive_partitioning=True).select("p").to_arrow().column("p")
    if pa.types.is_temporal(dtype):
        assert result.type == dtype
        result = result.cast(pa.int64())
    else:
        assert result.type == pa.large_string()
    assert result.to_pylist() == [ticks]


def test_orc_hive_leap_second_conversion() -> None:
    seconds = _hive_value("2024-01-01T23:59:60.123Z", pa.timestamp("s", "+00:00"))
    milliseconds = _hive_value("2024-01-01T23:59:60Z", pa.timestamp("ms", "+00:00"))
    assert seconds.cast(daft.DataType.int64()).to_pylist() == [1704153599]
    assert milliseconds.cast(daft.DataType.int64()).to_pylist() == [1704153600000]


@pytest.mark.parametrize(
    "value,dtype,ticks",
    [
        ("2024-01-01T00:00:00\u221205:00", pa.timestamp("s", "-05:00"), 1704085200),
        ("2024-01-01T00:00:00.123456789\u221205:00", pa.timestamp("ns", "-05:00"), 1704085200123456789),
        ("2024-01-01T00:00:00+05: 00", pa.timestamp("s", "+05:00"), 1704049200),
        ("2024-01-01T00:00:00+05 :00", pa.timestamp("s", "+05:00"), 1704049200),
        ("2024-01-01T00:00:00+05 : : 00", pa.timestamp("s", "+05:00"), 1704049200),
    ],
)
def test_read_orc_hive_timezone_tokens(tmp_path: Path, value: str, dtype: pa.DataType, ticks: int) -> None:
    path = _write_hive_orc(tmp_path, f"p={quote(value, safe='')}", [1, 2])
    result = daft.read_orc(path, hive_partitioning=True).select("p").to_arrow().column("p")
    assert result.type == dtype
    assert result.cast(pa.int64()).to_pylist() == [ticks, ticks]


@pytest.mark.parametrize(
    "value,ticks",
    [
        ("2024-01-01T00:00:00\u221205:00", 1704085200),
        ("2024-01-01T00:00:00+05: 00", 1704049200),
        ("2024-01-01T00:00:00+05 : : 00", 1704049200),
    ],
)
def test_orc_hive_timezone_token_pruning(tmp_path: Path, monkeypatch, value: str, ticks: int) -> None:
    paths = [
        _write_hive_orc(tmp_path, "p=2024-01-01T00%3A00%3A00Z", [1]),
        _write_hive_orc(tmp_path, f"p={quote(value, safe='')}", [2]),
        _write_hive_orc(tmp_path, "p=invalid-timestamp", [3]),
    ]
    source = OrcSource(paths, None, 1, hive_partitioning=True)
    predicate = daft.col("p") == daft.lit(ticks).cast(daft.DataType.timestamp("s", "+00:00"))
    tasks = _tasks(source, Pushdowns(columns=["id"], partition_filters=predicate))
    assert [task._path for task in tasks] == [_native_file_uri(Path(paths[1]))]
    scanned: list[str] = []
    original = _orc._iter_orc_batches

    def tracking_batches(path, *args, **kwargs):
        scanned.append(path)
        yield from original(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(_orc, "_iter_orc_batches", tracking_batches)
        assert [batch.to_pydict() for batch in _batches(tasks[0])] == [{"id": [2]}]
    assert scanned == [_native_file_uri(Path(paths[1]))]
    df = source.read()
    result = df.sort("id").to_arrow().column("p")
    assert result.type == pa.timestamp("s", "+00:00")
    assert result.cast(pa.int64()).to_pylist() == [1704067200, ticks, None]
    assert df.where(predicate).select("id").to_pydict() == {"id": [2]}


@pytest.mark.parametrize("offset", ["-05:30", "-00:30"])
def test_orc_hive_invalid_timezone_metadata(tmp_path: Path, offset: str) -> None:
    # Preserve the current Hive inference's negative-minute offset metadata,
    # and expose its conversion error rather than silently producing a null.
    path = _write_hive_orc(tmp_path, f"p={quote(f'2024-01-01T00:00:00{offset}', safe='')}", [1])
    with pytest.raises(ValueError, match="(?i)timezone"):
        _tasks(OrcSource(path, None, 1, hive_partitioning=True))


def test_read_orc_hive_timezone_conversion(tmp_path: Path) -> None:
    paths = [
        _write_hive_orc(tmp_path, "p=2024-01-01T08%3A00%3A00.123456789%2B08%3A00", [1]),
        _write_hive_orc(tmp_path, "p=2024-01-01T00%3A00%3A00.123456789Z", [2]),
    ]
    result = daft.read_orc(paths, hive_partitioning=True).select("p").to_arrow().column("p")
    assert result.type == pa.timestamp("ns", "+08:00")
    assert result.cast(pa.int64()).to_pylist() == [1704067200123456789] * 2


def test_orc_hive_first_path_fields(tmp_path: Path) -> None:
    first = _write_hive_orc(tmp_path, "p=3", [1])
    second = _write_hive_orc(tmp_path, "q=4", [2])
    plain = _write_hive_orc(tmp_path, "plain", [3])
    df = daft.read_orc([first, second, plain], hive_partitioning=True)
    assert df.column_names == ["id", "p"]
    assert df.sort("id").to_pydict() == {"id": [1, 2, 3], "p": [3, None, None]}
    assert daft.read_orc([plain, first], hive_partitioning=True).column_names == ["id"]


def test_orc_hive_missing_keys_and_tasks(tmp_path: Path) -> None:
    first = _write_hive_orc(tmp_path, "p=3/q=x", [1])
    second = _write_hive_orc(tmp_path, "q=y", [2])
    source = OrcSource([first, second], None, 1, hive_partitioning=True)
    assert [partition.field.name for partition in source.get_partition_fields()] == ["p", "q"]
    assert [task._path for task in _tasks(source, Pushdowns(columns=["id"], partition_filters=daft.col("p") == 3))] == [
        _native_file_uri(Path(first))
    ]
    assert [task._path for task in _tasks(source, Pushdowns(partition_filters=daft.col("p").is_null()))] == [
        _native_file_uri(Path(second))
    ]
    df = source.read()
    assert df.where(daft.col("p").is_null()).select("id").to_pydict() == {"id": [2]}
    assert df.where(daft.col("p") == 3).select("id").to_pydict() == {"id": [1]}


def test_orc_hive_physical_conflict(tmp_path: Path) -> None:
    first = _write_hive_orc(tmp_path, "p=3", [1, 2], p=["wrong", "values"])
    second = _write_hive_orc(tmp_path, "plain", [3], p=["physical"])
    df = daft.read_orc([first, second], hive_partitioning=True)
    assert df.column_names == ["id", "p"]
    assert df.sort("id").to_pydict() == {"id": [1, 2, 3], "p": [3, 3, None]}
    assert df.select("p").to_pydict() == {"p": [3, 3, None]}
    assert df.where(daft.col("p") == 3).select("id").sort("id").to_pydict() == {"id": [1, 2]}
    assert daft.read_orc(first).to_pydict()["p"] == ["wrong", "values"]


def test_orc_hive_scanner_excludes_partition_columns(tmp_path: Path, monkeypatch) -> None:
    path = _write_hive_orc(tmp_path, "p=3", [1, 2], p=[99, 99])
    source = OrcSource(path, None, 1, hive_partitioning=True)
    original = _orc.pads.Scanner
    requested: list[list[str]] = []

    class TrackingScanner:
        @staticmethod
        def from_fragment(*args, **kwargs):
            requested.append(kwargs["columns"])
            return original.from_fragment(*args, **kwargs)

    monkeypatch.setattr(_orc.pads, "Scanner", TrackingScanner)
    for columns in [None, ["p"], []]:
        batches = _batches(_tasks(source, Pushdowns(columns=columns))[0])
        assert sum(len(batch) for batch in batches) == 2
    assert requested == [["id"], [], []]


def test_read_orc_hive_queries(tmp_path: Path) -> None:
    paths = [_write_hive_orc(tmp_path, f"p={p}", [p * 10, p * 10 + 1]) for p in [1, 2]]
    df = daft.read_orc(paths, hive_partitioning=True, batch_size=1)
    assert df.select("p").sort("p").to_pydict() == {"p": [1, 1, 2, 2]}
    assert df.where((daft.col("p") == 2) & (daft.col("id") > 20)).select("id").to_pydict() == {"id": [21]}
    assert df.where((daft.col("p") == 1) | (daft.col("id") > 20)).select("id").sort("id").to_pydict() == {
        "id": [10, 11, 21]
    }
    assert df.where(daft.col("id") > daft.col("p") * 10).sort("id").to_pydict() == {"id": [11, 21], "p": [1, 2]}
    assert df.where(daft.col("p") == 2).count_rows() == 2
    assert df.where(daft.col("p") == 2).limit(1).count_rows() == 1
    empty = df.where(daft.col("p") == 99).to_arrow()
    assert empty.num_rows == 0 and empty.schema == df.to_arrow().schema


def test_orc_hive_empty_file_and_serialization(tmp_path: Path) -> None:
    empty = _write_hive_orc(tmp_path, "p=1", pa.array([], pa.int64()))
    data = _write_hive_orc(tmp_path, "p=2", [1, 2])
    source = cloudpickle.loads(cloudpickle.dumps(OrcSource([empty, data], None, 1, hive_partitioning=True)))
    tasks = _tasks(source)
    task = cloudpickle.loads(cloudpickle.dumps(tasks[1]))
    assert [batch.to_pydict() for batch in _batches(task)] == [{"id": [1], "p": [2]}, {"id": [2], "p": [2]}]
    assert source.read().to_pydict() == {"id": [1, 2], "p": [2, 2]}


def test_hive_binding_declared_and_missing_fields() -> None:
    schema = Schema.from_field_name_and_types([("p", daft.DataType.int64()), ("q", daft.DataType.bool())])
    values = [
        Series._from_pyseries(value)
        for value in _parse_hive_partition_values("/extra=x/q=TRUE/p=bad/data.orc", schema._schema)
    ]
    assert [value.name() for value in values] == ["q", "p"]
    assert [value.datatype() for value in values] == [daft.DataType.bool(), daft.DataType.int64()]
    assert [value.to_pylist() for value in values] == [[True], [None]]
    assert _parse_hive_partition_values("/plain/data.orc", schema._schema) == []
    empty = Schema.from_field_name_and_types([])
    assert _parse_hive_partition_values("/p=3/data.orc", empty._schema) == []


@pytest.mark.parametrize(
    "value,dtype,physical",
    [
        ("3", pa.int64(), 3),
        ("TRUE", pa.bool_(), True),
        ("1.5", pa.float64(), 1.5),
        ("hello", pa.large_string(), "hello"),
        ("__HIVE_DEFAULT_PARTITION__", pa.large_string(), None),
        ("2024-01-01", pa.date32(), 19723),
        ("12:30:00.123456789", pa.time64("ns"), 45000123456789),
        ("1969-12-31T23:59:59.123456789", pa.timestamp("ns"), -876543211),
        ("2024-01-01T08:00:00.123456789+08:00", pa.timestamp("ns", "+08:00"), 1704067200123456789),
    ],
)
def test_orc_hive_typed_constant_serialization(tmp_path: Path, value: str, dtype: pa.DataType, physical) -> None:
    paths = [
        _write_hive_orc(tmp_path, f"p={quote(value, safe='')}", [1, 2], p=["physical", "physical"]),
        _write_hive_orc(tmp_path, "plain", [3], p=["physical"]),
    ]
    source = cloudpickle.loads(cloudpickle.dumps(OrcSource(paths, None, 1, hive_partitioning=True)))
    tasks = [cloudpickle.loads(cloudpickle.dumps(task)) for task in _tasks(source, Pushdowns(columns=["p"]))]
    assert [task._partition_values["p"].datatype() for task in tasks] == [daft.DataType.from_arrow_type(dtype)] * 2
    batches = [batch for task in tasks for batch in _batches(task)]
    assert sum(len(batch) for batch in batches) == 3
    result = pa.concat_tables([batch.to_arrow_table() for batch in batches]).column("p")
    assert result.type == dtype
    if pa.types.is_temporal(dtype):
        result = result.cast(pa.int32() if pa.types.is_date32(dtype) else pa.int64())
    assert result.to_pylist() == [physical, physical, None]
    assert source.read().select("p").to_arrow().column("p").type == dtype


def test_orc_hive_real_file_pruning(tmp_path: Path, monkeypatch) -> None:
    paths = [_write_hive_orc(tmp_path, f"p={p}", [p]) for p in [1, 2]]
    source = OrcSource(paths, None, 1, hive_partitioning=True)
    original = _orc._iter_orc_batches
    scanned: list[str] = []

    def tracking_batches(path, *args, **kwargs):
        scanned.append(path)
        yield from original(path, *args, **kwargs)

    monkeypatch.setattr(_orc, "_iter_orc_batches", tracking_batches)
    tasks = _tasks(source, Pushdowns(partition_filters=daft.col("p") == 2))
    assert [task._path for task in tasks] == [_native_file_uri(Path(paths[1]))]
    assert _batches(tasks[0])[0].to_pydict() == {"id": [2], "p": [2]}
    assert scanned == [_native_file_uri(Path(paths[1]))]


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
