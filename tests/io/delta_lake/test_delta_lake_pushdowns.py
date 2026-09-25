from __future__ import annotations

import asyncio
import datetime
import decimal
import io
import json
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

deltalake = pytest.importorskip("deltalake")

import daft
from daft import col
from daft.daft import IOConfig, StorageConfig
from daft.io.delta_lake._visitors import convert_filter_to_stats_predicate
from daft.io.delta_lake.delta_lake_scan import DeltaLakeDataSource
from daft.io.pushdowns import Pushdowns
from tests.utils import sort_arrow_table

###
# Helpers
###


def _source(path) -> DeltaLakeDataSource:
    return DeltaLakeDataSource(str(path), StorageConfig(True, IOConfig()))


def _tasks(path, **pushdowns) -> list:
    async def collect():
        return [t async for t in _source(path).get_tasks(Pushdowns(**pushdowns))]

    return asyncio.run(collect())


def _write_files(path, tables: list[pa.Table], **kwargs) -> None:
    """One delta-rs append per table, so each table lands in its own file."""
    for table in tables:
        deltalake.write_deltalake(str(path), table, mode="append", **kwargs)


def _assert_filter_matches_full_scan(path, predicate) -> None:
    """The pruned read must return exactly what filtering an unpruned read returns."""
    actual = daft.read_deltalake(str(path)).where(predicate).to_arrow()
    full = daft.read_deltalake(str(path)).to_arrow()
    expected = daft.from_arrow(full).where(predicate).to_arrow()
    assert sort_arrow_table(actual, *actual.column_names) == sort_arrow_table(expected, *expected.column_names)


def _plan(df) -> str:
    buf = io.StringIO()
    df.explain(show_all=True, file=buf)
    return buf.getvalue()


def _write_log_table(
    path, fields: list[dict], files: list[tuple[pa.Table, dict | None]], config=None, protocol=(1, 2)
) -> None:
    """Hand-write a Delta log so each file's stats are exactly what the test says."""
    (path / "_delta_log").mkdir(parents=True)
    adds = []
    for table, stats in files:
        name = f"part-{uuid.uuid4().hex}.parquet"
        pq.write_table(table, path / name)
        add = {
            "path": name,
            "partitionValues": {},
            "size": (path / name).stat().st_size,
            "modificationTime": 0,
            "dataChange": True,
        }
        if stats is not None:
            add["stats"] = json.dumps(stats)
        adds.append({"add": add})
    actions = [
        {"protocol": {"minReaderVersion": protocol[0], "minWriterVersion": protocol[1]}},
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": fields}),
                "partitionColumns": [],
                "configuration": config or {},
                "createdTime": 0,
            }
        },
        *adds,
    ]
    (path / "_delta_log" / "00000000000000000000.json").write_text("\n".join(json.dumps(a) for a in actions))


def _field(name: str, delta_type: str) -> dict:
    return {"name": name, "type": delta_type, "nullable": True, "metadata": {}}


###
# Visitor: filter -> stats predicate over a hand-built add-actions table
###

# Four files over column `a` (int64), `f` (float64), `s` (string); file 3 has no stats for `a`.
_STATS_TYPE = pa.struct([("a", pa.int64()), ("f", pa.float64()), ("s", pa.string())])
_ADD_ACTIONS = pa.table(
    {
        "num_records": pa.array([10, 10, 10, 10], pa.int64()),
        "null_count": pa.array(
            [{"a": 0, "f": 0, "s": 0}, {"a": 3, "f": 0, "s": 0}, {"a": 10, "f": 0, "s": 0}, {"a": None}],
            pa.struct([("a", pa.int64()), ("f", pa.int64()), ("s", pa.int64())]),
        ),
        "min": pa.array(
            [{"a": 0, "f": 0.0, "s": "a"}, {"a": 10, "f": 10.0, "s": "k"}, {"a": None}, {"a": None}], _STATS_TYPE
        ),
        "max": pa.array(
            [{"a": 9, "f": 9.0, "s": "j"}, {"a": 19, "f": 19.0, "s": "t"}, {"a": None}, {"a": None}], _STATS_TYPE
        ),
    }
)
_COLUMNS = {f.name: (f.name, f.type) for f in _STATS_TYPE}


def _kept(expr) -> list[int]:
    predicate = convert_filter_to_stats_predicate(expr, _COLUMNS)
    assert predicate is not None
    table = _ADD_ACTIONS.append_column("idx", pa.array(range(_ADD_ACTIONS.num_rows)))
    return table.filter(predicate)["idx"].to_pylist()


# Files 2 and 3 have unknown stats for `a`, so every `a` predicate must keep them.
@pytest.mark.parametrize(
    "expr, kept",
    [
        pytest.param(col("a") == 5, [0, 2, 3], id="eq"),
        pytest.param(daft.lit(5) == col("a"), [0, 2, 3], id="eq_swapped"),
        pytest.param(col("a") == 100, [2, 3], id="eq_out_of_range"),
        pytest.param(col("a") != 5, [0, 1, 2, 3], id="ne"),
        pytest.param(col("a") < 10, [0, 2, 3], id="lt"),
        pytest.param(col("a") <= 10, [0, 1, 2, 3], id="le"),
        pytest.param(col("a") > 9, [1, 2, 3], id="gt"),
        pytest.param(col("a") >= 9, [0, 1, 2, 3], id="ge"),
        pytest.param(daft.lit(9) < col("a"), [1, 2, 3], id="lt_swapped"),
        pytest.param(daft.lit(10) >= col("a"), [0, 1, 2, 3], id="ge_swapped"),
        pytest.param(col("a").between(12, 14), [1, 2, 3], id="between"),
        pytest.param(col("a").is_in([1, 15]), [0, 1, 2, 3], id="is_in_both"),
        pytest.param(col("a").is_in([15, 16]), [1, 2, 3], id="is_in_one"),
        pytest.param(col("a").is_in([100]), [2, 3], id="is_in_none"),
        pytest.param(col("a").is_null(), [1, 2, 3], id="is_null"),
        pytest.param(col("a").not_null(), [0, 1, 3], id="not_null"),
        pytest.param((col("a") < 5) | (col("a") > 15), [0, 1, 2, 3], id="or"),
        pytest.param((col("a") > 5) & (col("a") < 8), [0, 2, 3], id="and"),
        pytest.param((col("a") == 100) | (col("a") == 5), [0, 2, 3], id="or_prunes"),
    ],
)
def test_visitor_prunes_by_stats(expr, kept):
    assert _kept(expr) == kept


@pytest.mark.parametrize(
    "expr",
    [
        pytest.param(~(col("a") == 5), id="not"),
        pytest.param(col("a") == col("f"), id="col_vs_col"),
        pytest.param(col("a").cast(daft.DataType.string()) == "5", id="cast_on_column"),
        pytest.param(col("missing") == 5, id="unknown_column"),
        pytest.param(col("a") == 100.5, id="float_literal_on_int_column"),
        pytest.param(col("f") > 100.0, id="float_max_may_hide_nan"),
        pytest.param(col("f") != 5.0, id="float_ne"),
        pytest.param(col("s") > "zzz", id="string_max_may_be_truncated"),
        pytest.param(col("a") == daft.lit(None), id="null_literal"),
        pytest.param(col("f") == float("nan"), id="nan_literal"),
    ],
)
def test_visitor_keeps_every_file_when_unprovable(expr):
    assert _kept(expr) == [0, 1, 2, 3]


def test_visitor_opaque_branch_of_and_still_prunes_other_branch():
    assert _kept((col("a") == 100) & (col("a").cast(daft.DataType.string()) == "5")) == [2, 3]


def test_visitor_float_min_and_equality_still_prune():
    assert _kept(col("f") < 5.0) == [0, 2, 3]
    assert _kept(col("f") == 15.0) == [1, 2, 3]


def test_visitor_string_min_still_prunes():
    assert _kept(col("s") < "b") == [0, 2, 3]
    assert _kept(col("s") == "e") == [0, 2, 3]


###
# Stats file skipping, end to end on the source
###


@pytest.fixture
def ranged_table(tmp_path):
    """Four files with disjoint `a` ranges; the last one also holds nulls."""
    path = tmp_path / "ranged"
    tables = [
        pa.table({"a": pa.array([0, 5, 9], pa.int64()), "b": ["x", "y", "z"]}),
        pa.table({"a": pa.array([10, 15, 19], pa.int64()), "b": ["x", "y", "z"]}),
        pa.table({"a": pa.array([20, 25, 29], pa.int64()), "b": ["x", "y", "z"]}),
        pa.table({"a": pa.array([30, None, 39], pa.int64()), "b": ["x", "y", "z"]}),
    ]
    _write_files(path, tables)
    return path


@pytest.mark.parametrize(
    "predicate, num_tasks",
    [
        pytest.param(col("a") == 15, 1, id="eq"),
        pytest.param(col("a") > 19, 2, id="gt"),
        pytest.param(col("a") < 0, 0, id="below_all"),
        pytest.param(col("a").is_in([5, 25]), 2, id="is_in"),
        pytest.param(col("a").is_null(), 1, id="is_null"),
        pytest.param((col("a") == 5) | (col("a") == 35), 2, id="or"),
        pytest.param(col("a").between(10, 25), 2, id="between"),
        pytest.param(col("b") == "y", 4, id="string_eq_in_every_file"),
    ],
)
def test_stats_skip_files(ranged_table, predicate, num_tasks):
    assert len(_tasks(ranged_table, filters=predicate)) == num_tasks
    _assert_filter_matches_full_scan(ranged_table, predicate)


def test_stats_skip_shows_in_plan(ranged_table):
    df = daft.read_deltalake(str(ranged_table)).where(col("a") == 15)
    assert "Num Scan Tasks = 1\n" in _plan(df)


def test_stats_skip_with_partition_and_data_filter(tmp_path):
    path = tmp_path / "part"
    _write_files(
        path,
        [
            pa.table({"p": ["x", "x"], "a": pa.array([1, 2], pa.int64())}),
            pa.table({"p": ["x", "x"], "a": pa.array([10, 20], pa.int64())}),
            pa.table({"p": ["y", "y"], "a": pa.array([1, 2], pa.int64())}),
        ],
        partition_by=["p"],
    )
    tasks = _tasks(path, filters=col("a") < 5, partition_filters=col("p") == "x")
    assert len(tasks) == 1
    _assert_filter_matches_full_scan(path, (col("p") == "x") & (col("a") < 5))


def test_stats_skip_keeps_files_without_stats(tmp_path):
    path = tmp_path / "nostats"
    _write_log_table(
        path,
        [_field("a", "long")],
        [
            (
                pa.table({"a": pa.array([1, 2], pa.int64())}),
                {"numRecords": 2, "minValues": {"a": 1}, "maxValues": {"a": 2}},
            ),
            (pa.table({"a": pa.array([100, 200], pa.int64())}), None),
        ],
    )
    assert len(_tasks(path, filters=col("a") > 50)) == 1
    _assert_filter_matches_full_scan(path, col("a") > 50)


def test_stats_skip_keeps_unindexed_columns(tmp_path):
    path = tmp_path / "unindexed"
    _write_files(
        path,
        [
            pa.table({"a": pa.array([1], pa.int64()), "b": pa.array([1], pa.int64())}),
            pa.table({"a": pa.array([2], pa.int64()), "b": pa.array([100], pa.int64())}),
        ],
        configuration={"delta.dataSkippingNumIndexedCols": "1"},
    )
    assert len(_tasks(path, filters=col("b") > 50)) == 2
    assert len(_tasks(path, filters=col("a") > 1)) == 1
    _assert_filter_matches_full_scan(path, col("b") > 50)


def test_stats_skip_tolerates_truncated_string_max(tmp_path):
    path = tmp_path / "strings"
    long_value = "prefix" + "z" * 40
    # A writer truncated the max to a prefix that sorts below the real value.
    stats = {"numRecords": 1, "minValues": {"s": "prefix"}, "maxValues": {"s": "prefix"}, "nullCount": {"s": 0}}
    _write_log_table(path, [_field("s", "string")], [(pa.table({"s": [long_value]}), stats)])
    assert len(_tasks(path, filters=col("s") == long_value)) == 1
    assert len(_tasks(path, filters=col("s") > "prefixa")) == 1
    _assert_filter_matches_full_scan(path, col("s") == long_value)


def test_stats_skip_tolerates_millisecond_timestamp_max(tmp_path):
    path = tmp_path / "ts"
    utc = datetime.timezone.utc
    value = datetime.datetime(2024, 1, 1, 0, 0, 0, 123456, tzinfo=utc)
    # A writer truncated the max to milliseconds, so it sits below the real value.
    truncated = "2024-01-01T00:00:00.123Z"
    stats = {"numRecords": 1, "minValues": {"t": truncated}, "maxValues": {"t": truncated}, "nullCount": {"t": 0}}
    _write_log_table(
        path, [_field("t", "timestamp")], [(pa.table({"t": pa.array([value], pa.timestamp("us", "UTC"))}), stats)]
    )
    predicate = col("t") > datetime.datetime(2024, 1, 1, 0, 0, 0, 123400, tzinfo=utc)
    assert len(_tasks(path, filters=predicate)) == 1
    _assert_filter_matches_full_scan(path, predicate)


def test_stats_skip_column_mapped_table_by_logical_name(tmp_path):
    path = tmp_path / "cm"
    phys_schema = pa.schema([pa.field("col-aaa", pa.int64(), metadata={b"PARQUET:field_id": b"1"})])
    field = {
        "name": "a",
        "type": "long",
        "nullable": True,
        "metadata": {"delta.columnMapping.id": 1, "delta.columnMapping.physicalName": "col-aaa"},
    }
    _write_log_table(
        path,
        [field],
        [
            (
                pa.table({"col-aaa": [1, 2, 3]}, schema=phys_schema),
                {"numRecords": 3, "minValues": {"col-aaa": 1}, "maxValues": {"col-aaa": 3}},
            ),
            (
                pa.table({"col-aaa": [10, 11, 12]}, schema=phys_schema),
                {"numRecords": 3, "minValues": {"col-aaa": 10}, "maxValues": {"col-aaa": 12}},
            ),
        ],
        config={"delta.columnMapping.mode": "name", "delta.columnMapping.maxColumnId": "1"},
        protocol=(2, 5),
    )
    assert len(_tasks(path, filters=col("a") == 11)) == 1
    _assert_filter_matches_full_scan(path, col("a") == 11)


###
# Partition pruning
###


@pytest.mark.parametrize(
    "gen",
    [
        pytest.param(lambda i: i, id="int"),
        pytest.param(lambda i: i * 1.5, id="float"),
        pytest.param(lambda i: f"foo_{i}", id="string"),
        pytest.param(lambda i: datetime.datetime(2024, 2, i + 1), id="timestamp"),
        pytest.param(lambda i: datetime.date(2024, 2, i + 1), id="date"),
        pytest.param(lambda i: decimal.Decimal(str(1000 + i) + ".567"), id="decimal"),
    ],
)
def test_partition_pruning(tmp_path, gen):
    path = tmp_path / "parts"
    values = [gen(i) for i in range(4)]
    table = pa.table({"part": values, "a": pa.array(range(4), pa.int64())})
    if isinstance(values[0], decimal.Decimal):
        table = table.cast(pa.schema([("part", pa.decimal128(7, 3)), ("a", pa.int64())]))
    deltalake.write_deltalake(str(path), table, partition_by=["part"])

    assert len(_tasks(path, partition_filters=col("part") == values[2])) == 1
    assert len(_tasks(path, partition_filters=col("part") < values[2])) == 2
    _assert_filter_matches_full_scan(path, col("part") == values[2])
    _assert_filter_matches_full_scan(path, col("part") < values[2])


###
# Count pushdown
###


@pytest.fixture
def partitioned_table(tmp_path):
    path = tmp_path / "counted"
    _write_files(
        path,
        [
            pa.table({"p": ["x"] * 3, "a": pa.array([1, 2, None], pa.int64())}),
            pa.table({"p": ["y"] * 2, "a": pa.array([3, 4], pa.int64())}),
            pa.table({"p": ["x"] * 4, "a": pa.array([5, 6, 7, 8], pa.int64())}),
        ],
        partition_by=["p"],
    )
    return path


@pytest.fixture
def metadata_counts(monkeypatch) -> list:
    """Records each metadata count the source produced; None means it fell back to scanning."""
    results: list = []
    original = DeltaLakeDataSource._count_from_metadata

    def spy(self, pushdowns):
        result = original(self, pushdowns)
        results.append(result)
        return result

    monkeypatch.setattr(DeltaLakeDataSource, "_count_from_metadata", spy)
    return results


@pytest.mark.parametrize(
    "where, expected",
    [
        pytest.param(None, 9, id="all"),
        pytest.param(col("p") == "x", 7, id="partition_eq"),
        pytest.param(col("p") == "z", 0, id="partition_empty"),
        pytest.param(col("p").is_in(["x", "y"]), 9, id="partition_is_in"),
    ],
)
def test_count_pushdown_from_metadata(partitioned_table, metadata_counts, where, expected):
    df = daft.read_deltalake(str(partitioned_table))
    if where is not None:
        df = df.where(where)
    counted = df.count()
    assert "Aggregation pushdown = count" in _plan(counted)
    assert counted.to_pydict()["count"] == [expected]
    assert metadata_counts and set(metadata_counts) == {expected}


def test_count_pushdown_falls_back_with_data_filter(partitioned_table, metadata_counts):
    df = daft.read_deltalake(str(partitioned_table)).where(col("a") > 2)
    assert df.count().to_pydict()["count"] == [6]
    assert metadata_counts == []


def test_count_pushdown_falls_back_without_num_records(tmp_path, metadata_counts):
    path = tmp_path / "nocount"
    _write_log_table(
        path,
        [_field("a", "long")],
        [
            (pa.table({"a": pa.array([1, 2], pa.int64())}), {"numRecords": 2}),
            (pa.table({"a": pa.array([3, 4, 5], pa.int64())}), None),
        ],
    )
    assert daft.read_deltalake(str(path)).count().to_pydict()["count"] == [5]
    assert metadata_counts and set(metadata_counts) == {None}


def test_count_pushdown_disabled_with_deletion_vectors(tmp_path, metadata_counts):
    path = tmp_path / "dv"
    _write_files(
        path,
        [pa.table({"a": pa.array([1, 2, 3], pa.int64())})],
        configuration={"delta.enableDeletionVectors": "true"},
    )
    assert not _source(path).supports_count_pushdown()
    counted = daft.read_deltalake(str(path), ignore_deletion_vectors=True).count()
    assert "Aggregation pushdown" not in _plan(counted)
    assert counted.to_pydict()["count"] == [3]
    assert metadata_counts == []


###
# Regression guard: pruning never changes results
###


@pytest.mark.parametrize(
    "predicate",
    [
        col("a") == 15,
        col("a") != 15,
        col("a") >= 20,
        col("a") < 10,
        col("a").is_in([5, 25, 100]),
        col("a").between(9, 20),
        col("a").is_null(),
        col("a").not_null(),
        (col("a") < 10) | (col("b") == "z"),
        ~(col("a") > 15),
        col("b") >= "y",
    ],
    ids=str,
)
def test_pruning_matches_full_scan(ranged_table, predicate):
    _assert_filter_matches_full_scan(ranged_table, predicate)
