"""Tests for the Arrow PyCapsule Interface (import and export)."""

from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pytest

import daft
from daft import DataType
from daft.recordbatch import MicroPartition


class TestSeriesPyCapsule:
    """Test __arrow_c_schema__ and __arrow_c_array__ on PySeries."""

    @pytest.mark.parametrize(
        "arrow_array",
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array([1.0, 2.5, None], type=pa.float64()),
            pa.array(["a", "b", None], type=pa.large_string()),
            pa.array([True, False, True], type=pa.bool_()),
            pa.array([None, None, None], type=pa.int32()),
        ],
        ids=["int64", "float64", "string", "bool", "all_null"],
    )
    def test_export_array(self, arrow_array):
        series = daft.Series.from_arrow(arrow_array, name="test")
        schema_capsule, array_capsule = series._series.__arrow_c_array__()
        assert type(schema_capsule).__name__ == "PyCapsule"
        assert type(array_capsule).__name__ == "PyCapsule"

    def test_schema_capsule(self):
        series = daft.Series.from_arrow(pa.array([1, 2, 3], type=pa.int64()), name="test")
        capsule = series._series.__arrow_c_schema__()
        assert type(capsule).__name__ == "PyCapsule"

    def test_requested_schema_casts(self):
        series = daft.Series.from_arrow(pa.array([1, 2, 3], type=pa.int64()), name="test")
        target_field = pa.field("test", pa.int32())
        result = pa.chunked_array(series, type=pa.int32())
        assert result.type == target_field.type
        assert result.to_pylist() == [1, 2, 3]


class TestRequestedSchemaColumnOrder:
    """Regression: requested_schema with reordered columns must match by name, not position."""

    def test_record_batch_reorder_by_name(self):
        source = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3], type=pa.int32()), pa.array([1.5, 2.5, 3.5], type=pa.float64())],
            names=["a", "b"],
        )
        mp = MicroPartition.from_arrow_record_batches([source], source.schema)
        rb = mp.to_record_batch()
        target = pa.schema([("b", pa.float32()), ("a", pa.int64())])
        result = pa.record_batch(rb._recordbatch, schema=target)
        assert result.column("a").type == pa.int64()
        assert result.column("b").type == pa.float32()
        assert result.column("a").to_pylist() == [1, 2, 3]
        assert result.column("b").to_pylist() == [1.5, 2.5, 3.5]

    def test_missing_name_errors(self):
        rb = MicroPartition.from_pydict({"a": [1, 2, 3]}).to_record_batch()
        target = pa.schema([("nonexistent", pa.int64())])
        with pytest.raises(Exception, match="nonexistent"):
            pa.record_batch(rb._recordbatch, schema=target)

    def test_requested_schema_rejects_non_capsule(self):
        series = daft.Series.from_arrow(pa.array([1, 2, 3]), name="test")
        with pytest.raises(TypeError):
            series._series.__arrow_c_array__(requested_schema=object())


class TestRecordBatchPyCapsule:
    """Test __arrow_c_schema__ and __arrow_c_array__ on PyRecordBatch."""

    def _make_record_batch(self, data: dict):
        mp = MicroPartition.from_pydict(data)
        return mp.to_record_batch()

    def test_roundtrip_via_pyarrow(self):
        rb = self._make_record_batch({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        pa_rb = pa.record_batch(rb._recordbatch)
        assert pa_rb.column("a").to_pylist() == [1, 2, 3]
        assert pa_rb.column("b").to_pylist() == [4.0, 5.0, 6.0]

    def test_schema_capsule(self):
        rb = self._make_record_batch({"a": [1, 2, 3]})
        capsule = rb._recordbatch.__arrow_c_schema__()
        assert type(capsule).__name__ == "PyCapsule"


class TestMicroPartitionPyCapsule:
    """Test __arrow_c_schema__ and __arrow_c_stream__ on PyMicroPartition."""

    def test_schema_capsule(self):
        mp = MicroPartition.from_pydict({"a": [1, 2, 3]})
        capsule = mp._micropartition.__arrow_c_schema__()
        assert type(capsule).__name__ == "PyCapsule"

    def test_roundtrip_via_pyarrow(self):
        mp = MicroPartition.from_pydict({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        table = pa.RecordBatchReader.from_stream(mp._micropartition).read_all()
        assert table.column("a").to_pylist() == [1, 2, 3]
        assert table.column("b").to_pylist() == [4.0, 5.0, 6.0]

    def test_multi_batch_stream(self):
        mp = MicroPartition.concat(
            [MicroPartition.from_pydict({"a": [1, 2]}), MicroPartition.from_pydict({"a": [3, 4]})]
        )
        batches = list(pa.RecordBatchReader.from_stream(mp._micropartition))
        assert len(batches) == 2
        assert [v for b in batches for v in b.column("a").to_pylist()] == [1, 2, 3, 4]


class TestFromArrowStream:
    """Test importing via __arrow_c_stream__ through daft.from_arrow()."""

    def test_from_record_batch_reader(self):
        schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
        batches = [
            pa.record_batch({"a": [1, 2], "b": ["x", "y"]}, schema=schema),
            pa.record_batch({"a": [3, 4], "b": ["z", "w"]}, schema=schema),
        ]
        reader = pa.RecordBatchReader.from_batches(schema, batches)
        df = daft.from_arrow(reader)
        result = df.to_arrow()
        assert result.column("a").to_pylist() == [1, 2, 3, 4]
        assert result.column("b").to_pylist() == ["x", "y", "z", "w"]

    def test_from_pyarrow_table_via_stream(self):
        table = pa.table({"x": [10, 20, 30], "y": [1.1, 2.2, 3.3]})
        df = daft.from_arrow(table)
        result = df.to_arrow()
        assert result.column("x").to_pylist() == [10, 20, 30]
        assert result.column("y").to_pylist() == [1.1, 2.2, 3.3]

    def test_empty_stream(self):
        schema = pa.schema([("a", pa.int64())])
        reader = pa.RecordBatchReader.from_batches(schema, [])
        df = daft.from_arrow(reader)
        result = df.to_arrow()
        assert len(result) == 0
        assert "a" in result.schema.names

    def test_multiple_types(self):
        schema = pa.schema(
            [
                ("int_col", pa.int32()),
                ("float_col", pa.float64()),
                ("str_col", pa.string()),
                ("bool_col", pa.bool_()),
            ]
        )
        batch = pa.record_batch(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.0, 2.0, 3.0],
                "str_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
            },
            schema=schema,
        )
        reader = pa.RecordBatchReader.from_batches(schema, [batch])
        df = daft.from_arrow(reader)
        result = df.to_arrow()
        assert result.column("int_col").to_pylist() == [1, 2, 3]
        assert result.column("str_col").to_pylist() == ["a", "b", "c"]
        assert result.column("bool_col").to_pylist() == [True, False, True]

    def test_nested_types(self):
        schema = pa.schema(
            [
                ("list_col", pa.list_(pa.int64())),
                ("struct_col", pa.struct([("x", pa.int64()), ("y", pa.string())])),
            ]
        )
        batch = pa.record_batch(
            {
                "list_col": [[1, 2], [3]],
                "struct_col": [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}],
            },
            schema=schema,
        )
        reader = pa.RecordBatchReader.from_batches(schema, [batch])
        df = daft.from_arrow(reader)
        result = df.to_arrow()
        assert result.column("list_col").to_pylist() == [[1, 2], [3]]
        assert result.column("struct_col").to_pylist() == [
            {"x": 1, "y": "a"},
            {"x": 2, "y": "b"},
        ]

    def test_full_roundtrip(self):
        mp = MicroPartition.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        reader = pa.RecordBatchReader.from_stream(mp._micropartition)
        df = daft.from_arrow(reader)
        result = df.to_arrow()
        assert result.column("a").to_pylist() == [1, 2, 3]
        assert result.column("b").to_pylist() == ["x", "y", "z"]

    def test_fixed_shape_tensor_extension(self):
        tensor_type = pa.fixed_shape_tensor(pa.float32(), [2, 3])
        storage = pa.array(
            [[1.0, 2.0, 3.0, 4.0, 5.0, 6.0], [7.0, 8.0, 9.0, 10.0, 11.0, 12.0]], type=pa.list_(pa.float32(), 6)
        )
        tensor_array = pa.ExtensionArray.from_storage(tensor_type, storage)
        schema = pa.schema([("t", tensor_type)])
        reader = pa.RecordBatchReader.from_batches(schema, [pa.record_batch([tensor_array], schema=schema)])
        df = daft.from_arrow(reader)
        assert df.schema()["t"].dtype == DataType.tensor(DataType.float32(), (2, 3))

    def test_daft_embedding_roundtrip(self):
        original = (
            daft.from_pydict({"e": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]})
            .with_column("e", daft.col("e").cast(DataType.embedding(DataType.float32(), 3)))
            .collect()
        )
        assert original.schema()["e"].dtype == DataType.embedding(DataType.float32(), 3)

        # Re-import via pycapsule stream; Daft super-extension metadata preserves dtype.
        restored = daft.from_arrow(pa.RecordBatchReader.from_stream(original))
        assert restored.schema()["e"].dtype == DataType.embedding(DataType.float32(), 3)

    def test_from_pandas_dataframe(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df = daft.from_arrow(pdf)
        result = df.to_arrow()
        assert result.column("a").to_pylist() == [1, 2, 3]
        assert result.column("b").to_pylist() == ["x", "y", "z"]

    def test_null_heavy_roundtrip(self):
        table = pa.table(
            {"a": pa.array([None, None, None], type=pa.int32()), "b": pa.array([1, None, 3], type=pa.int64())}
        )
        df = daft.from_arrow(table)
        result = df.to_arrow()
        assert result.column("a").to_pylist() == [None, None, None]
        assert result.column("b").to_pylist() == [1, None, 3]


class TestPythonLevelInterop:
    """Test that Python-level Series/DataFrame expose PyCapsule methods for zero-copy interop."""

    def test_series_to_pyarrow_via_pycapsule(self):
        series = daft.Series.from_pylist([1, 2, 3], name="a")
        result = pa.chunked_array(series)
        assert result.to_pylist() == [1, 2, 3]

    def test_series_schema_capsule(self):
        series = daft.Series.from_pylist([1, 2, 3], name="a")
        capsule = series.__arrow_c_schema__()
        assert type(capsule).__name__ == "PyCapsule"

    def test_dataframe_to_pyarrow_via_pycapsule(self):
        df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]}).collect()
        result = pa.table(df)
        assert result.column("a").to_pylist() == [1, 2, 3]
        assert result.column("b").to_pylist() == ["x", "y", "z"]

    def test_dataframe_schema_capsule(self):
        df = daft.from_pydict({"a": [1, 2, 3]}).collect()
        capsule = df.__arrow_c_schema__()
        assert type(capsule).__name__ == "PyCapsule"

    def test_dataframe_roundtrip(self):
        original = daft.from_pydict({"x": [10, 20], "y": [1.1, 2.2]}).collect()
        pa_table = pa.table(original)
        restored = daft.from_arrow(pa_table)
        assert restored.to_arrow().column("x").to_pylist() == [10, 20]
        assert restored.to_arrow().column("y").to_pylist() == [1.1, 2.2]
