from __future__ import annotations

import copy
from typing import Dict, List

import pytest

import daft.datatype as dtypes
from daft.daft import ImageMode
from daft.datatype import (
    BooleanType,
    DataType,
    FixedShapeImageType,
    FixedShapeSparseTensorType,
    FixedShapeTensorType,
    FloatType,
    ImageType,
    IntegerType,
    MapType,
    NumericType,
    SparseTensorType,
    StringType,
    TemporalType,
    TensorType,
    TimestampType,
)

daft_int_types = [
    DataType.int8(),
    DataType.int16(),
    DataType.int32(),
    DataType.int64(),
    DataType.uint8(),
    DataType.uint16(),
    DataType.uint32(),
    DataType.uint64(),
]

daft_numeric_types = daft_int_types + [DataType.float32(), DataType.float64()]
daft_string_types = [DataType.string()]
daft_nonnull_types = (
    daft_numeric_types
    + daft_string_types
    + [DataType.bool(), DataType.binary(), DataType.fixed_size_binary(1), DataType.date()]
)

all_types = [
    DataType.int8(),
    DataType.int16(),
    DataType.int32(),
    DataType.int64(),
    DataType.uint8(),
    DataType.uint16(),
    DataType.uint32(),
    DataType.uint64(),
    DataType.float32(),
    DataType.float64(),
    DataType.bool(),
    DataType.string(),
    DataType.binary(),
    DataType.fixed_size_binary(1),
    DataType.null(),
    DataType.date(),
    DataType.time("us"),
    DataType.timestamp("us"),
    DataType.duration("us"),
    DataType.interval(),
    DataType.list(DataType.int32()),
    DataType.fixed_size_list(DataType.int32(), 10),
    DataType.map(DataType.int32(), DataType.string()),
    DataType.struct({"a": DataType.int32(), "b": DataType.string()}),
    DataType.embedding(DataType.int32(), 10),
    DataType.image("RGB"),
    DataType.tensor(DataType.float32()),
    DataType.sparse_tensor(DataType.float32()),
]


@pytest.mark.parametrize("dtype", daft_nonnull_types)
def test_datatype_pickling(dtype) -> None:
    copy_dtype = copy.deepcopy(dtype)
    assert copy_dtype == dtype


@pytest.mark.parametrize(
    ["source", "expected"],
    [
        (str, DataType.string()),
        (int, DataType.int64()),
        (float, DataType.float64()),
        (bytes, DataType.binary()),
        (object, DataType.python()),
        (
            {"foo": str, "bar": int},
            DataType.struct({"foo": DataType.string(), "bar": DataType.int64()}),
        ),
    ],
)
def test_datatype_parsing(source, expected):
    assert DataType._infer_type(source) == expected


@pytest.mark.parametrize(
    ["source", "expected"],
    [
        # These tests must be run in later version of Python that allow for subscripting of types
        (list[str], DataType.list(DataType.string())),
        (dict[str, int], DataType.map(DataType.string(), DataType.int64())),
        (
            {"foo": list[str], "bar": int},
            DataType.struct({"foo": DataType.list(DataType.string()), "bar": DataType.int64()}),
        ),
        (list[list[str]], DataType.list(DataType.list(DataType.string()))),
    ],
)
def test_subscripted_datatype_parsing(source, expected):
    assert DataType._infer_type(source) == expected


@pytest.mark.parametrize(
    ["source", "expected"],
    [
        # These tests must be run in later version of Python that allow for subscripting of types
        (List[str], DataType.list(DataType.string())),
        (Dict[str, int], DataType.map(DataType.string(), DataType.int64())),
        (
            {"foo": List[str], "bar": int},
            DataType.struct({"foo": DataType.list(DataType.string()), "bar": DataType.int64()}),
        ),
        (List[List[str]], DataType.list(DataType.list(DataType.string()))),
    ],
)
def test_legacy_subscripted_datatype_parsing(source, expected):
    assert DataType._infer_type(source) == expected


@pytest.mark.parametrize(
    ["source", "expected"],
    [
        (DataType.int8(), dtypes.Int8Type),
        (DataType.int16(), dtypes.Int16Type),
        (DataType.int32(), dtypes.Int32Type),
        (DataType.int64(), dtypes.Int64Type),
        (DataType.uint8(), dtypes.UInt8Type),
        (DataType.uint16(), dtypes.UInt16Type),
        (DataType.uint32(), dtypes.UInt32Type),
        (DataType.uint64(), dtypes.UInt64Type),
        (DataType.float32(), dtypes.Float32Type),
        (DataType.float64(), dtypes.Float64Type),
        (DataType.bool(), dtypes.BooleanType),
        (DataType.string(), dtypes.StringType),
        (DataType.binary(), dtypes.BinaryType),
        (DataType.fixed_size_binary(1), dtypes.FixedSizeBinaryType),
        (DataType.null(), dtypes.NullType),
        (DataType.decimal128(10, 2), dtypes.DecimalType),
        (DataType.date(), dtypes.DateType),
        (DataType.time("us"), dtypes.TimeType),
        (DataType.timestamp("us"), dtypes.TimestampType),
        (DataType.duration("us"), dtypes.DurationType),
        (DataType.interval(), dtypes.IntervalType),
        (DataType.list(DataType.int32()), dtypes.ListType),
        (DataType.fixed_size_list(DataType.int32(), 10), dtypes.FixedSizeListType),
        (DataType.map(DataType.int32(), DataType.string()), dtypes.MapType),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), dtypes.StructType),
        (DataType.embedding(DataType.int32(), 10), dtypes.EmbeddingType),
        (DataType.image("RGB"), dtypes.ImageType),
        (DataType.image("RGBA", 2, 2), dtypes.FixedShapeImageType),
        (DataType.tensor(DataType.float32()), dtypes.TensorType),
        (DataType.tensor(DataType.float32(), (2, 2)), dtypes.FixedShapeTensorType),
        (DataType.sparse_tensor(DataType.float32()), dtypes.SparseTensorType),
        (DataType.sparse_tensor(DataType.float32(), (2, 2)), dtypes.FixedShapeSparseTensorType),
    ],
)
def test_dtype_subclass(source, expected):
    assert isinstance(source, expected)


@pytest.mark.parametrize(
    ["source", "expected"],
    [
        (DataType.fixed_size_binary(1).size, 1),
        (DataType.decimal128(10, 2).precision, 10),
        (DataType.decimal128(10, 2).scale, 2),
        (DataType.list(DataType.int32()).inner, DataType.int32()),
        (DataType.fixed_size_list(DataType.int32(), 10).size, 10),
        (DataType.fixed_size_list(DataType.int32(), 10).inner, DataType.int32()),
        (DataType.map(DataType.int32(), DataType.string()).key, DataType.int32()),
        (DataType.map(DataType.int32(), DataType.string()).value, DataType.string()),
        (DataType.embedding(DataType.int32(), 10).size, 10),
        (DataType.embedding(DataType.int32(), 10).dtype, DataType.int32()),
        (DataType.image("RGB").mode, ImageMode.RGB),
        (DataType.image("RGBA", 2, 2).mode, ImageMode.RGBA),
        (DataType.image("RGBA", 2, 2).height, 2),
        (DataType.image("RGBA", 2, 2).width, 2),
        (DataType.tensor(DataType.float32()).dtype, DataType.float32()),
        (
            DataType.struct({"a": DataType.int32(), "b": DataType.string()}).fields,
            {"a": DataType.int32(), "b": DataType.string()},
        ),
    ],
)
def test_nested_properties_subclass(source, expected):
    assert source == expected


def test_timestamp_type():
    ts = DataType.timestamp("us", "UTC")
    assert str(ts.timeunit) == "us"
    assert ts.timezone == "UTC"


def test_duration_type():
    ts = DataType.duration("us")
    assert str(ts.timeunit) == "us"


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), True),
        (DataType.int16(), True),
        (DataType.int32(), True),
        (DataType.int64(), True),
        (DataType.uint8(), True),
        (DataType.uint16(), True),
        (DataType.uint32(), True),
        (DataType.uint64(), True),
        (DataType.float32(), True),
        (DataType.float64(), True),
        (DataType.bool(), False),
        (DataType.string(), False),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), False),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), False),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), False),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), False),
        (DataType.tensor(DataType.float32()), False),
        (DataType.sparse_tensor(DataType.float32()), False),
    ],
)
def test_is_numeric(dtype, expected):
    assert isinstance(dtype, NumericType) == expected


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), False),
        (DataType.int16(), False),
        (DataType.int32(), False),
        (DataType.int64(), False),
        (DataType.uint8(), False),
        (DataType.uint16(), False),
        (DataType.uint32(), False),
        (DataType.uint64(), False),
        (DataType.float32(), True),
        (DataType.float64(), True),
        (DataType.bool(), False),
        (DataType.string(), False),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), False),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), False),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), False),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), False),
        (DataType.tensor(DataType.float32()), False),
        (DataType.sparse_tensor(DataType.float32()), False),
    ],
)
def test_is_float(dtype, expected):
    assert isinstance(dtype, FloatType) == expected


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), True),
        (DataType.int16(), True),
        (DataType.int32(), True),
        (DataType.int64(), True),
        (DataType.uint8(), True),
        (DataType.uint16(), True),
        (DataType.uint32(), True),
        (DataType.uint64(), True),
        (DataType.float32(), False),
        (DataType.float64(), False),
        (DataType.bool(), False),
        (DataType.string(), False),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), False),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), False),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), False),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), False),
        (DataType.tensor(DataType.float32()), False),
        (DataType.sparse_tensor(DataType.float32()), False),
    ],
)
def test_is_integer(dtype, expected):
    assert isinstance(dtype, IntegerType) == expected


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), False),
        (DataType.int16(), False),
        (DataType.int32(), False),
        (DataType.int64(), False),
        (DataType.uint8(), False),
        (DataType.uint16(), False),
        (DataType.uint32(), False),
        (DataType.uint64(), False),
        (DataType.float32(), False),
        (DataType.float64(), False),
        (DataType.bool(), False),
        (DataType.string(), False),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), False),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), False),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), False),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), True),
        (DataType.image("RGBA", 2, 2), False),
        (DataType.tensor(DataType.float32()), False),
        (DataType.sparse_tensor(DataType.float32()), False),
    ],
)
def test_is_image(dtype, expected):
    assert isinstance(dtype, ImageType) == expected


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), False),
        (DataType.int16(), False),
        (DataType.int32(), False),
        (DataType.int64(), False),
        (DataType.uint8(), False),
        (DataType.uint16(), False),
        (DataType.uint32(), False),
        (DataType.uint64(), False),
        (DataType.float32(), False),
        (DataType.float64(), False),
        (DataType.bool(), False),
        (DataType.string(), False),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), False),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), False),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), False),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), False),
        (DataType.image("RGBA", 2, 2), True),
        (DataType.tensor(DataType.float32()), False),
        (DataType.sparse_tensor(DataType.float32()), False),
    ],
)
def test_is_fixed_shape_image(dtype, expected):
    assert isinstance(dtype, FixedShapeImageType) == expected


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), False),
        (DataType.int16(), False),
        (DataType.int32(), False),
        (DataType.int64(), False),
        (DataType.uint8(), False),
        (DataType.uint16(), False),
        (DataType.uint32(), False),
        (DataType.uint64(), False),
        (DataType.float32(), False),
        (DataType.float64(), False),
        (DataType.bool(), False),
        (DataType.string(), False),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), False),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), False),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), True),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), False),
        (DataType.image("RGBA", 2, 2), False),
        (DataType.tensor(DataType.float32()), False),
        (DataType.sparse_tensor(DataType.float32()), False),
    ],
)
def test_is_list(dtype, expected):
    assert isinstance(dtype, dtypes.ListType) == expected


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), False),
        (DataType.int16(), False),
        (DataType.int32(), False),
        (DataType.int64(), False),
        (DataType.uint8(), False),
        (DataType.uint16(), False),
        (DataType.uint32(), False),
        (DataType.uint64(), False),
        (DataType.float32(), False),
        (DataType.float64(), False),
        (DataType.bool(), False),
        (DataType.string(), False),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), False),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), False),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), False),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), False),
        (DataType.image("RGBA", 2, 2), False),
        (DataType.tensor(DataType.float32()), True),
        (DataType.sparse_tensor(DataType.float32()), False),
    ],
)
def test_is_tensor(dtype, expected):
    assert isinstance(dtype, TensorType) == expected


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), False),
        (DataType.int16(), False),
        (DataType.int32(), False),
        (DataType.int64(), False),
        (DataType.uint8(), False),
        (DataType.uint16(), False),
        (DataType.uint32(), False),
        (DataType.uint64(), False),
        (DataType.float32(), False),
        (DataType.float64(), False),
        (DataType.bool(), False),
        (DataType.string(), False),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), False),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), False),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), False),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), False),
        (DataType.image("RGBA", 2, 2), False),
        (DataType.tensor(DataType.float32()), False),
        (DataType.tensor(DataType.float32(), (2, 2)), True),
        (DataType.sparse_tensor(DataType.float32()), False),
    ],
)
def test_is_fixed_tensor(dtype, expected):
    assert isinstance(dtype, FixedShapeTensorType) == expected


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), False),
        (DataType.int16(), False),
        (DataType.int32(), False),
        (DataType.int64(), False),
        (DataType.uint8(), False),
        (DataType.uint16(), False),
        (DataType.uint32(), False),
        (DataType.uint64(), False),
        (DataType.float32(), False),
        (DataType.float64(), False),
        (DataType.bool(), False),
        (DataType.string(), False),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), False),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), False),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), False),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), False),
        (DataType.image("RGBA", 2, 2), False),
        (DataType.tensor(DataType.float32()), False),
        (DataType.tensor(DataType.float32(), (2, 2)), False),
        (DataType.sparse_tensor(DataType.float32()), True),
    ],
)
def test_is_sparse_tensor(dtype, expected):
    assert isinstance(dtype, SparseTensorType) == expected


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), False),
        (DataType.int16(), False),
        (DataType.int32(), False),
        (DataType.int64(), False),
        (DataType.uint8(), False),
        (DataType.uint16(), False),
        (DataType.uint32(), False),
        (DataType.uint64(), False),
        (DataType.float32(), False),
        (DataType.float64(), False),
        (DataType.bool(), False),
        (DataType.string(), False),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), False),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), False),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), False),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), False),
        (DataType.image("RGBA", 2, 2), False),
        (DataType.tensor(DataType.float32()), False),
        (DataType.tensor(DataType.float32(), (2, 2)), False),
        (DataType.sparse_tensor(DataType.float32()), False),
        (DataType.sparse_tensor(DataType.float32(), (2, 2)), True),
    ],
)
def test_is_fixed_sparse_tensor(dtype, expected):
    assert isinstance(dtype, FixedShapeSparseTensorType) == expected


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), False),
        (DataType.int16(), False),
        (DataType.int32(), False),
        (DataType.int64(), False),
        (DataType.uint8(), False),
        (DataType.uint16(), False),
        (DataType.uint32(), False),
        (DataType.uint64(), False),
        (DataType.float32(), False),
        (DataType.float64(), False),
        (DataType.bool(), True),
        (DataType.string(), False),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), False),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), False),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), False),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), False),
        (DataType.image("RGBA", 2, 2), False),
        (DataType.tensor(DataType.float32()), False),
        (DataType.tensor(DataType.float32(), (2, 2)), False),
        (DataType.sparse_tensor(DataType.float32()), False),
        (DataType.sparse_tensor(DataType.float32(), (2, 2)), False),
    ],
)
def test_is_bool(dtype, expected):
    assert isinstance(dtype, BooleanType) == expected


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), False),
        (DataType.int16(), False),
        (DataType.int32(), False),
        (DataType.int64(), False),
        (DataType.uint8(), False),
        (DataType.uint16(), False),
        (DataType.uint32(), False),
        (DataType.uint64(), False),
        (DataType.float32(), False),
        (DataType.float64(), False),
        (DataType.bool(), False),
        (DataType.string(), True),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), False),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), False),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), False),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), False),
        (DataType.image("RGBA", 2, 2), False),
        (DataType.tensor(DataType.float32()), False),
        (DataType.tensor(DataType.float32(), (2, 2)), False),
        (DataType.sparse_tensor(DataType.float32()), False),
        (DataType.sparse_tensor(DataType.float32(), (2, 2)), False),
    ],
)
def test_is_string(dtype, expected):
    assert isinstance(dtype, StringType) == expected


@pytest.mark.parametrize(
    ["dtype", "expected"],
    [
        (DataType.int8(), False),
        (DataType.int16(), False),
        (DataType.int32(), False),
        (DataType.int64(), False),
        (DataType.uint8(), False),
        (DataType.uint16(), False),
        (DataType.uint32(), False),
        (DataType.uint64(), False),
        (DataType.float32(), False),
        (DataType.float64(), False),
        (DataType.bool(), False),
        (DataType.string(), False),
        (DataType.binary(), False),
        (DataType.fixed_size_binary(1), False),
        (DataType.null(), False),
        (DataType.date(), True),
        (DataType.time("us"), False),
        (DataType.timestamp("us"), True),
        (DataType.duration("us"), False),
        (DataType.interval(), False),
        (DataType.list(DataType.int32()), False),
        (DataType.fixed_size_list(DataType.int32(), 10), False),
        (DataType.map(DataType.int32(), DataType.string()), False),
        (DataType.struct({"a": DataType.int32(), "b": DataType.string()}), False),
        (DataType.embedding(DataType.int32(), 10), False),
        (DataType.image("RGB"), False),
        (DataType.image("RGBA", 2, 2), False),
        (DataType.tensor(DataType.float32()), False),
        (DataType.tensor(DataType.float32(), (2, 2)), False),
        (DataType.sparse_tensor(DataType.float32()), False),
        (DataType.sparse_tensor(DataType.float32(), (2, 2)), False),
    ],
)
def test_is_temporal(dtype, expected):
    assert isinstance(dtype, TemporalType) == expected


def test_is_timestamp():
    assert isinstance(DataType.timestamp("us"), TimestampType)
    assert not isinstance(DataType.int32(), TimestampType)


def test_is_logical():
    assert not isinstance(DataType.bool(), dtypes.LogicalType)
    assert isinstance(DataType.embedding(DataType.int32(), 10), dtypes.LogicalType)


def test_is_map():
    assert isinstance(DataType.map(DataType.int32(), DataType.string()), MapType)
    assert not isinstance(DataType.int32(), MapType)
