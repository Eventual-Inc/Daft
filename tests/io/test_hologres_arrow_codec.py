from __future__ import annotations

import io

import pyarrow as pa
import pytest

from daft.io._hologres._arrow_codec import ArrowStageBuffer, build_arrow_schema, columns_for
from daft.io._hologres._schema_loader import HologresColumn, HologresTableSchema


def _columns() -> list[HologresColumn]:
    return [
        HologresColumn(name="id", type_name="int4", type_oid=23),
        HologresColumn(name="name", type_name="text", type_oid=25),
        HologresColumn(name="score", type_name="float4", type_oid=700),
        HologresColumn(name="payload", type_name="jsonb", type_oid=3802),
    ]


class TestBuildArrowSchema:
    def test_basic_schema(self):
        schema = build_arrow_schema(_columns())
        assert schema.names == ["id", "name", "score", "payload"]
        assert schema.field("id").type == pa.int32()
        assert schema.field("name").type == pa.utf8()
        assert schema.field("score").type == pa.float32()
        assert schema.field("payload").type == pa.utf8()  # staged as text

    def test_array_types(self):
        col = HologresColumn(name="v", type_name="float4[]")
        assert build_arrow_schema([col]).field("v").type == pa.list_(pa.float32())

    def test_numeric_types(self):
        col = HologresColumn(name="n", type_name="numeric", precision=10, scale=2)
        assert build_arrow_schema([col]).field("n").type == pa.decimal128(10, 2)

    def test_unsupported_array_raises(self):
        col = HologresColumn(name="v", type_name="weird[]")
        with pytest.raises(ValueError, match="Unsupported array element type"):
            build_arrow_schema([col])


class TestColumnsFor:
    def test_resolve(self):
        holo = HologresTableSchema(
            schema_name="public",
            table_name="t",
            columns=_columns(),
            primary_keys=["id"],
        )
        resolved = columns_for(holo, ["name", "id"])
        assert [c.name for c in resolved] == ["name", "id"]

    def test_unknown_column_raises(self):
        holo = HologresTableSchema(schema_name="public", table_name="t", columns=_columns())
        with pytest.raises(ValueError, match="not found in schema"):
            columns_for(holo, ["nope"])


class TestArrowStageBuffer:
    def test_roundtrip(self):
        cols = _columns()
        buf = ArrowStageBuffer(cols)
        table = pa.table(
            {
                "id": pa.array([1, 2], pa.int32()),
                "name": pa.array(["a", "b"]),
                "score": pa.array([0.5, 1.5], pa.float32()),
                "payload": pa.array(['{"x": 1}', None]),
            }
        )
        buf.put_table(table)
        data = buf.end_and_get_bytes()
        assert len(data) > 0

        with pa.ipc.open_stream(io.BytesIO(data)) as reader:
            out = reader.read_all()
        assert out.schema.names == ["id", "name", "score", "payload"]
        assert out.column("id").to_pylist() == [1, 2]
        assert out.column("payload").to_pylist() == ['{"x": 1}', None]

    def test_projection_and_reorder(self):
        cols = _columns()
        buf = ArrowStageBuffer(cols)
        # extra column + different order tolerated
        table = pa.table(
            {
                "extra": pa.array([10, 20]),
                "score": pa.array([1.0, 2.0], pa.float32()),
                "id": pa.array([1, 2], pa.int32()),
                "name": pa.array(["a", "b"]),
                "payload": pa.array([None, None]),
            }
        )
        buf.put_table(table)
        data = buf.end_and_get_bytes()
        with pa.ipc.open_stream(io.BytesIO(data)) as reader:
            out = reader.read_all()
        assert out.schema.names == ["id", "name", "score", "payload"]

    def test_type_cast(self):
        # int64 input cast down to int32 wire schema
        buf = ArrowStageBuffer([HologresColumn(name="id", type_name="int4")])
        buf.put_table(pa.table({"id": pa.array([1, 2], pa.int64())}))
        data = buf.end_and_get_bytes()
        with pa.ipc.open_stream(io.BytesIO(data)) as reader:
            out = reader.read_all()
        assert out.column("id").type == pa.int32()

    def test_empty_table_produces_no_bytes(self):
        buf = ArrowStageBuffer(_columns())
        buf.put_table(
            pa.table(
                {
                    "id": pa.array([], pa.int32()),
                    "name": pa.array([], pa.string()),
                    "score": pa.array([], pa.float32()),
                    "payload": pa.array([], pa.string()),
                }
            )
        )
        assert buf.end_and_get_bytes() == b""

    def test_data_size_tracking(self):
        buf = ArrowStageBuffer(_columns())
        assert buf.data_size == 0
        buf.put_table(
            pa.table(
                {
                    "id": pa.array([1], pa.int32()),
                    "name": pa.array(["x"]),
                    "score": pa.array([1.0], pa.float32()),
                    "payload": pa.array([None]),
                }
            )
        )
        assert buf.data_size > 0
        buf.end_and_get_bytes()
        assert buf.data_size == 0
