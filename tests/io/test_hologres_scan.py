from __future__ import annotations

import io
import struct

import pyarrow as pa

import daft
from daft.daft import IOConfig, PyPushdowns, StorageConfig
from daft.io._hologres._schema_loader import HologresColumn, HologresTableSchema
from daft.io._hologres.scan import (
    HologresScanOperator,
    _iter_arrow_blocks_from_copy,
    _string_partition_bounds,
)


def _schema() -> HologresTableSchema:
    return HologresTableSchema(
        schema_name="analytics",
        table_name="Events",
        columns=[
            HologresColumn(name="id", type_name="int4", type_oid=23),
            HologresColumn(name="name", type_name="text", type_oid=25),
            HologresColumn(name="ds", type_name="text", type_oid=25),
        ],
        primary_keys=["id"],
        partition_column="ds",
        shard_count=4,
    )


def _operator(**kwargs) -> HologresScanOperator:
    return HologresScanOperator(
        "postgresql://user:password@host:80/db",
        _schema(),
        StorageConfig(True, IOConfig()),
        **kwargs,
    )


class _UnsupportedFilters:
    def to_sql(self):
        return None


class _FakePushdowns:
    filters = _UnsupportedFilters()
    limit = 10


class TestPartitionBounds:
    def test_string_bounds_exclude_nulls_and_duplicates(self):
        bounds = _string_partition_bounds([None, "b", "a", "b", None], 2)
        assert bounds == ["a", "b"]
        assert None not in bounds

    def test_single_value_or_no_values_does_not_partition(self):
        assert _string_partition_bounds([None], 4) is None
        assert _string_partition_bounds(["a", "a"], 4) is None

    def test_first_bound_task_includes_nulls(self, monkeypatch):
        op = _operator(partition_col="ds", num_partitions=2)
        monkeypatch.setattr(op, "_get_partition_bounds", lambda n: ["a", "b", "c"])
        monkeypatch.setattr(op, "_get_num_rows", lambda p: 6)

        captured = []

        def fake_scan_task(*args, **kwargs):
            captured.append(kwargs["partition_bounds"])
            return object()

        monkeypatch.setattr(op, "_scan_task", fake_scan_task)
        list(op._partition_bound_scan_tasks(PyPushdowns(), use_arrow=True, num_tasks=2))

        assert captured[0] == ("(ds >= 'a' OR ds IS NULL)", "(ds < 'b' OR ds IS NULL)")
        assert "IS NULL" not in captured[1][0]
        assert "IS NULL" not in captured[1][1]


class TestPushdownPlanning:
    def test_shard_restriction_survives_disabled_pushdowns(self):
        op = _operator(disable_pushdowns=True)
        pushdowns = PyPushdowns(filters=(daft.col("id") > 1)._expr, limit=5)

        predicate, filter_applied, limit_applied = op._plan_query_pushdowns(
            pushdowns, use_arrow=True, shard_list=[0, 1]
        )

        assert predicate == "hg_shard_id IN (0, 1)"
        assert filter_applied is False
        assert limit_applied is False

    def test_arrow_never_pushes_limit(self):
        op = _operator()
        pushdowns = PyPushdowns(limit=5)

        _, _, limit_applied = op._plan_query_pushdowns(pushdowns, use_arrow=True)

        assert limit_applied is False

    def test_unsupported_filter_is_not_marked_applied(self):
        op = _operator()

        predicate, filter_applied, _ = op._plan_query_pushdowns(_FakePushdowns(), use_arrow=True)

        assert predicate is None
        assert filter_applied is False


class TestIdentifierQuoting:
    def test_query_quotes_legal_column_names(self):
        op = _operator()

        projection = op._quoted_columns(['a"b', "Order Date"])
        sql = op._compose_query(projection=projection)

        assert '"a""b"' in sql
        assert '"Order Date"' in sql

    def test_base_query_preserves_mixed_case_table(self):
        assert _operator()._base_query() == 'analytics."Events"'


def test_table_read_options_use_documented_disable_pushdowns_name():
    from daft.catalog.__hologres import HologresTable

    assert "disable_pushdowns" in HologresTable._read_options
    assert "disable_pushdowns_to_sql" not in HologresTable._read_options


class _FakeCopy:
    def __init__(self, data: bytes, chunk_size: int = 7) -> None:
        self._data = data
        self._position = 0
        self._chunk_size = chunk_size

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = len(self._data)
        size = min(size, self._chunk_size)
        chunk = self._data[self._position : self._position + size]
        self._position += len(chunk)
        return chunk


def _arrow_stream(table: pa.Table) -> bytes:
    buffer = pa.BufferOutputStream()
    with pa.ipc.new_stream(buffer, table.schema) as writer:
        writer.write_table(table)
    return buffer.getvalue().to_pybytes()


def test_arrow_blocks_are_decoded_from_incremental_copy_chunks():
    table = pa.table({"id": pa.array([1, 2], pa.int32())})
    arrow_data = _arrow_stream(table)
    response = (
        b"PGCOPY\n\xff\r\n\x00"
        + struct.pack("!II", 0, 0)
        + struct.pack("!h", 1)
        + struct.pack("!i", len(arrow_data))
        + arrow_data
        + struct.pack("!h", -1)
    )

    blocks = list(_iter_arrow_blocks_from_copy(_FakeCopy(response, chunk_size=7)))

    assert len(blocks) == 1
    with pa.ipc.open_stream(io.BytesIO(blocks[0])) as reader:
        assert reader.read_all().to_pydict() == {"id": [1, 2]}


def test_bare_arrow_ipc_stream_is_supported():
    table = pa.table({"id": pa.array([1, 2], pa.int32())})
    arrow_data = _arrow_stream(table)

    blocks = list(_iter_arrow_blocks_from_copy(_FakeCopy(arrow_data, chunk_size=5)))

    assert len(blocks) == 1
    with pa.ipc.open_stream(io.BytesIO(blocks[0])) as reader:
        assert reader.read_all().to_pydict() == {"id": [1, 2]}
