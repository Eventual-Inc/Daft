from __future__ import annotations

import pyarrow as pa

from daft.catalog import Identifier
from daft.catalog.__hologres import HologresTable
from daft.io._hologres._schema_loader import HologresColumn, HologresTableSchema
from daft.io._hologres.sink import HologresDataSink, _child_partition_name, _commit_stage_sql, _prepare_stage_table
from daft.io._hologres.types import OnConflictAction


def _schema() -> HologresTableSchema:
    return HologresTableSchema(
        schema_name="public",
        table_name="events",
        columns=[
            HologresColumn(name="id", type_name="int4", type_oid=23),
            HologresColumn(name="name", type_name="text", type_oid=25),
            HologresColumn(name="payload", type_name="jsonb", type_oid=3802),
        ],
        primary_keys=["id"],
        partition_column="ds",
    )


class TestChildPartitionName:
    def test_long_table_name_keeps_suffix(self):
        table_name = "t" * 60
        first = _child_partition_name(table_name, "a")
        second = _child_partition_name(table_name, "b")

        assert len(first) <= 63
        assert len(second) <= 63
        assert first != second
        assert "_p_a_" in first
        assert "_p_b_" in second


class TestPrepareStageTable:
    def test_omitted_columns_are_not_required(self):
        table = pa.table({"id": pa.array([1], pa.int32())})

        staged = _prepare_stage_table(table, _schema())

        assert staged.column_names == ["id"]

    def test_json_columns_are_staged_as_text(self):
        table = pa.table(
            {
                "id": pa.array([1], pa.int32()),
                "payload": pa.array([{"a": 1}]),
            }
        )

        staged = _prepare_stage_table(table, _schema())

        assert staged.column("payload").to_pylist() == ['{"a": 1}']


class TestStageCommitSql:
    def test_commit_only_names_staged_columns(self):
        sink = HologresDataSink.__new__(HologresDataSink)
        sink._holo_schema = _schema()
        sink._stage_columns = [_schema().columns[0]]
        sink._on_conflict = OnConflictAction.NONE
        sink._write_mode = "append"
        sink._schema_name = "public"
        sink._table_name = "events"
        sink._stage_name = "stage"

        sql = _commit_stage_sql(sink)

        assert '"id"' in sql
        assert '"name"' not in sql
        assert '"payload"' not in sql


class _FakeDataFrame:
    def __init__(self, name: str, calls: list) -> None:
        self.name = name
        self.calls = calls

    def collect(self):
        self.calls.append(("collect", self.name))
        return _FakeDataFrame(f"{self.name}_collected", self.calls)

    def write_sink(self, sink) -> None:
        self.calls.append(("write_sink", self.name, sink))


class _FakeSink:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs


class TestOverwrite:
    def _table(self) -> HologresTable:
        return HologresTable._from_catalog(
            "postgresql://user:password@host:80/db",
            Identifier("public", "events"),
        )

    def test_non_stage_overwrite_materializes_before_writing(self, monkeypatch):
        table = self._table()
        monkeypatch.setattr(table, "_load_hologres_schema", _schema)
        monkeypatch.setattr("daft.io._hologres.sink.HologresDataSink", _FakeSink)
        calls: list = []

        table.overwrite(_FakeDataFrame("input", calls))

        assert calls[0] == ("collect", "input")
        assert calls[1][0] == "write_sink"
        assert calls[1][1] == "input_collected"

    def test_stage_overwrite_does_not_materialize(self, monkeypatch):
        table = self._table()
        monkeypatch.setattr(table, "_load_hologres_schema", _schema)
        monkeypatch.setattr("daft.io._hologres.sink.HologresDataSink", _FakeSink)
        calls: list = []

        table.overwrite(_FakeDataFrame("input", calls), copy_mode="stage")

        assert [call[0] for call in calls] == ["write_sink"]
