from __future__ import annotations

import copy
from typing import Dict, List

import pytest

from daft.datatype import DataType

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
    + [DataType.bool(), DataType.binary(), DataType.fixed_size_binary(10), DataType.date()]
)

daft_list_types = [DataType.list(t) for t in daft_nonnull_types] + [
    DataType.fixed_size_list(t, size=10) for t in daft_nonnull_types
]
daft_map_types = [DataType.map(DataType.string(), t) for t in daft_nonnull_types]
daft_struct_types = [DataType.struct({"foo": t, "bar": t}) for t in daft_nonnull_types]

all_daft_types = daft_nonnull_types + daft_list_types + daft_map_types + daft_struct_types


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


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_null(test_type):
    assert test_type.is_null() == (test_type == DataType.null())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_boolean(test_type):
    assert test_type.is_boolean() == (test_type == DataType.bool())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_int8(test_type):
    assert test_type.is_int8() == (test_type == DataType.int8())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_int16(test_type):
    assert test_type.is_int16() == (test_type == DataType.int16())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_int32(test_type):
    assert test_type.is_int32() == (test_type == DataType.int32())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_int64(test_type):
    assert test_type.is_int64() == (test_type == DataType.int64())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_uint8(test_type):
    assert test_type.is_uint8() == (test_type == DataType.uint8())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_uint16(test_type):
    assert test_type.is_uint16() == (test_type == DataType.uint16())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_uint32(test_type):
    assert test_type.is_uint32() == (test_type == DataType.uint32())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_uint64(test_type):
    assert test_type.is_uint64() == (test_type == DataType.uint64())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_float32(test_type):
    assert test_type.is_float32() == (test_type == DataType.float32())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_float64(test_type):
    assert test_type.is_float64() == (test_type == DataType.float64())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_decimal128(test_type):
    assert test_type.is_decimal128() == (test_type == DataType.decimal128(10, 2))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_timestamp(test_type):
    assert test_type.is_timestamp() == (test_type == DataType.timestamp("ns"))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_date(test_type):
    assert test_type.is_date() == (test_type == DataType.date())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_time(test_type):
    assert test_type.is_time() == (test_type == DataType.time("ns"))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_duration(test_type):
    assert test_type.is_duration() == (test_type == DataType.duration("ns"))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_interval(test_type):
    assert test_type.is_interval() == (test_type == DataType.interval())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_binary(test_type):
    assert test_type.is_binary() == (test_type == DataType.binary())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_fixed_size_binary(test_type):
    assert test_type.is_fixed_size_binary() == (test_type == DataType.fixed_size_binary(10))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_string(test_type):
    assert test_type.is_string() == (test_type == DataType.string())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_list(test_type):
    if test_type.is_list():
        assert test_type.is_list() == (test_type == DataType.list(test_type.inner_type))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_fixed_size_list(test_type):
    if test_type.is_fixed_size_list():
        assert test_type.is_fixed_size_list() == (test_type == DataType.fixed_size_list(test_type.inner_type, 10))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_image(test_type):
    assert test_type.is_image() == (test_type == DataType.image())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_fixed_shape_image(test_type):
    assert test_type.is_fixed_shape_image() == (test_type == DataType.image(mode="RGB", height=224, width=224))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_tensor(test_type):
    assert test_type.is_tensor() == (test_type == DataType.tensor(DataType.float32()))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_fixed_shape_tensor(test_type):
    assert test_type.is_fixed_shape_tensor() == (test_type == DataType.tensor(DataType.float32(), shape=(2, 3)))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_sparse_tensor(test_type):
    assert test_type.is_sparse_tensor() == (test_type == DataType.sparse_tensor(DataType.float32()))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_fixed_shape_sparse_tensor(test_type):
    assert test_type.is_fixed_shape_sparse_tensor() == (
        test_type == DataType.sparse_tensor(DataType.float32(), shape=(2, 3))
    )


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_python(test_type):
    assert test_type.is_python() == (test_type == DataType.python())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_numeric(test_type):
    assert test_type.is_numeric() == (test_type in daft_numeric_types)


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_integer(test_type):
    assert test_type.is_integer() == (test_type in daft_int_types)


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_temporal(test_type):
    temporal_types = [
        DataType.timestamp("ns"),
        DataType.date(),
        DataType.time("ns"),
        DataType.duration("ns"),
        DataType.interval(),
    ]
    assert test_type.is_temporal() == (test_type in temporal_types)


@pytest.mark.parametrize("test_type", all_daft_types)
def test_fixed_size_property(test_type):
    if test_type.is_fixed_size_list() or test_type.is_fixed_size_binary():
        assert test_type.fixed_size == 10


@pytest.mark.parametrize("test_type", all_daft_types)
def test_inner_type_property(test_type: DataType):
    if (
        test_type.is_list()
        or test_type.is_fixed_size_list()
        or test_type.is_tensor()
        or test_type.is_sparse_tensor()
        or test_type.is_fixed_shape_tensor()
        or test_type.is_fixed_shape_sparse_tensor()
    ):
        try:
            assert test_type.inner_type is not None
        except AttributeError:
            assert False


@pytest.mark.parametrize("test_type", all_daft_types)
def test_struct_fields_property(test_type):
    if test_type.is_struct():
        assert test_type.fields == {"foo": test_type, "bar": test_type}


@pytest.mark.parametrize("test_type", all_daft_types)
def test_precision_and_scale_properties(test_type):
    if test_type.is_decimal128():
        assert test_type.precision == 10
        assert test_type.scale == 2


@pytest.mark.parametrize("test_type", all_daft_types)
def test_time_unit_property(test_type: DataType):
    if test_type.is_time() or test_type.is_duration() or test_type.is_timestamp():
        assert test_type.timeunit == "ns"


@pytest.mark.parametrize("test_type", all_daft_types)
def test_image_properties(test_type: DataType):
    if test_type.is_image() or test_type.is_fixed_shape_image():
        assert test_type.image_mode == "RGB"
