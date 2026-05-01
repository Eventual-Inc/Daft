from __future__ import annotations

import pyarrow as pa

import daft
from daft import DataType, Series


class TestExtensionTypeCast:
    def test_cast_series_to_extension_utf8(self):
        s = Series.from_pylist(["hello", "world"], pyobj="disallow")
        ext_dtype = DataType.extension("test.label", DataType.string())
        result = s.cast(ext_dtype)
        assert result.datatype() == ext_dtype

    def test_cast_series_to_extension_int64(self):
        s = Series.from_pylist([1, 2, 3], pyobj="disallow")
        ext_dtype = DataType.extension("test.counter", DataType.int64())
        result = s.cast(ext_dtype)
        assert result.datatype() == ext_dtype

    def test_cast_series_to_extension_float64(self):
        s = Series.from_pylist([1.5, 2.5], pyobj="disallow")
        ext_dtype = DataType.extension("test.measurement", DataType.float64())
        result = s.cast(ext_dtype)
        assert result.datatype() == ext_dtype

    def test_cast_series_to_extension_binary(self):
        s = Series.from_pylist([b"abc", b"def"], pyobj="disallow")
        ext_dtype = DataType.extension("test.uuid", DataType.binary())
        result = s.cast(ext_dtype)
        assert result.datatype() == ext_dtype

    def test_cast_series_to_extension_boolean(self):
        s = Series.from_pylist([True, False, None], pyobj="disallow")
        ext_dtype = DataType.extension("test.flag", DataType.bool())
        result = s.cast(ext_dtype)
        assert result.datatype() == ext_dtype

    def test_cast_series_to_extension_fixed_size_list(self):
        s = Series.from_pylist([[1.0, 2.0], [3.0, 4.0]], pyobj="disallow")
        ext_dtype = DataType.extension(
            "geo.point",
            DataType.fixed_size_list(DataType.float64(), 2),
            '{"crs": "WGS84"}',
        )
        result = s.cast(ext_dtype)
        assert result.datatype() == ext_dtype

    def test_cast_series_to_extension_with_metadata(self):
        s = Series.from_pylist([1, 2, 3], pyobj="disallow")
        ext_dtype = DataType.extension("test.typed", DataType.int64(), '{"unit": "meters"}')
        result = s.cast(ext_dtype)
        assert result.datatype() == ext_dtype
        assert result.datatype().is_extension()


class TestExtensionTypeCastBack:
    def test_cast_extension_to_utf8(self):
        s = Series.from_pylist(["a", "b"], pyobj="disallow")
        ext_dtype = DataType.extension("test.label", DataType.string())
        ext_s = s.cast(ext_dtype)
        result = ext_s.cast(DataType.string())
        assert result.datatype() == DataType.string()
        assert result.to_pylist() == ["a", "b"]

    def test_cast_extension_to_int64(self):
        s = Series.from_pylist([10, 20, 30], pyobj="disallow")
        ext_dtype = DataType.extension("test.counter", DataType.int64())
        ext_s = s.cast(ext_dtype)
        result = ext_s.cast(DataType.int64())
        assert result.datatype() == DataType.int64()
        assert result.to_pylist() == [10, 20, 30]

    def test_cast_extension_to_float64(self):
        s = Series.from_pylist([1.5, 2.5], pyobj="disallow")
        ext_dtype = DataType.extension("test.measurement", DataType.float64())
        ext_s = s.cast(ext_dtype)
        result = ext_s.cast(DataType.float64())
        assert result.to_pylist() == [1.5, 2.5]

    def test_cast_extension_to_binary(self):
        s = Series.from_pylist([b"abc", b"def"], pyobj="disallow")
        ext_dtype = DataType.extension("test.uuid", DataType.binary())
        ext_s = s.cast(ext_dtype)
        result = ext_s.cast(DataType.binary())
        assert result.datatype() == DataType.binary()
        assert result.to_pylist() == [b"abc", b"def"]

    def test_cast_extension_to_boolean(self):
        s = Series.from_pylist([True, False, None], pyobj="disallow")
        ext_dtype = DataType.extension("test.flag", DataType.bool())
        ext_s = s.cast(ext_dtype)
        result = ext_s.cast(DataType.bool())
        assert result.to_pylist() == [True, False, None]


class TestExtensionTypeCastWithNulls:
    def test_int64_with_nulls(self):
        s = Series.from_pylist([1, None, 3], pyobj="disallow")
        ext_dtype = DataType.extension("test.nullable", DataType.int64())
        ext_s = s.cast(ext_dtype)
        assert ext_s.datatype() == ext_dtype
        result = ext_s.cast(DataType.int64())
        assert result.to_pylist() == [1, None, 3]

    def test_utf8_with_nulls(self):
        s = Series.from_pylist(["a", None, "c"], pyobj="disallow")
        ext_dtype = DataType.extension("test.nullable_str", DataType.string())
        ext_s = s.cast(ext_dtype)
        result = ext_s.cast(DataType.string())
        assert result.to_pylist() == ["a", None, "c"]


class TestExtensionTypeCastBetweenExtensions:
    def test_cast_between_extensions_same_storage(self):
        s = Series.from_pylist([1, 2, 3], pyobj="disallow")
        ext_a = DataType.extension("type.a", DataType.int64())
        ext_b = DataType.extension("type.b", DataType.int64(), '{"version": 2}')
        series_a = s.cast(ext_a)
        series_b = series_a.cast(ext_b)
        assert series_b.datatype() == ext_b
        result = series_b.cast(DataType.int64())
        assert result.to_pylist() == [1, 2, 3]


class TestExtensionTypeCastStorageCoercion:
    def test_int64_to_extension_int32(self):
        s = Series.from_pylist([1, 2, 3], pyobj="disallow")
        ext_dtype = DataType.extension("test.small", DataType.int32())
        result = s.cast(ext_dtype)
        assert result.datatype() == ext_dtype
        unwrapped = result.cast(DataType.int32())
        assert unwrapped.to_pylist() == [1, 2, 3]


class TestExtensionTypeArrowRoundTrip:
    def test_roundtrip_int64(self):
        s = Series.from_pylist([1, 2, 3], pyobj="disallow")
        ext_dtype = DataType.extension("test.roundtrip", DataType.int64(), '{"key": "val"}')
        ext_s = s.cast(ext_dtype)

        arrow_arr = ext_s.to_arrow()
        assert isinstance(arrow_arr.type, pa.ExtensionType)
        restored = Series.from_arrow(arrow_arr, name="col")
        assert restored.datatype().is_extension()
        assert restored.datatype() == ext_dtype
        assert restored.to_pylist() == [1, 2, 3]

    def test_roundtrip_fixed_size_list(self):
        s = Series.from_pylist([[1.0, 2.0], [3.0, 4.0]], pyobj="disallow")
        ext_dtype = DataType.extension(
            "geo.point",
            DataType.fixed_size_list(DataType.float64(), 2),
            '{"crs": "WGS84"}',
        )
        ext_s = s.cast(ext_dtype)

        arrow_arr = ext_s.to_arrow()
        assert isinstance(arrow_arr.type, pa.ExtensionType)
        restored = Series.from_arrow(arrow_arr, name="col")
        assert restored.datatype().is_extension()
        assert restored.datatype() == ext_dtype

    def test_roundtrip_no_metadata(self):
        s = Series.from_pylist(["a", "b", "c"], pyobj="disallow")
        ext_dtype = DataType.extension("test.label", DataType.string())
        ext_s = s.cast(ext_dtype)

        arrow_arr = ext_s.to_arrow()
        assert isinstance(arrow_arr.type, pa.ExtensionType)
        restored = Series.from_arrow(arrow_arr, name="col")
        assert restored.datatype().is_extension()
        assert restored.datatype() == ext_dtype
        assert restored.to_pylist() == ["a", "b", "c"]


class TestExtensionTypeDataFrameIntegration:
    def test_cast_in_with_column(self):
        ext_dtype = DataType.extension("geo.point", DataType.fixed_size_list(DataType.float64(), 2))
        df = daft.from_pydict({"location": [[1.0, 2.0], [3.0, 4.0]]})
        df = df.with_column("location", df["location"].cast(ext_dtype))
        schema = df.schema()
        assert schema["location"].dtype.is_extension()

    def test_collect_with_extension_type(self):
        ext_dtype = DataType.extension("test.id", DataType.int64())
        df = daft.from_pydict({"id": [1, 2, 3]})
        df = df.with_column("id", df["id"].cast(ext_dtype))
        result = df.collect()
        assert len(result) == 3
        assert result.schema()["id"].dtype.is_extension()

    def test_limit_with_extension_type(self):
        ext_dtype = DataType.extension("test.id", DataType.int64())
        df = daft.from_pydict({"id": [1, 2, 3, 4, 5]})
        df = df.with_column("id", df["id"].cast(ext_dtype))
        result = df.limit(2).collect()
        assert len(result) == 2
        assert result.schema()["id"].dtype.is_extension()

    def test_select_with_extension_type(self):
        ext_dtype = DataType.extension("test.id", DataType.int64())
        df = daft.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        df = df.with_column("id", df["id"].cast(ext_dtype))
        result = df.select("id").collect()
        assert result.schema()["id"].dtype.is_extension()
