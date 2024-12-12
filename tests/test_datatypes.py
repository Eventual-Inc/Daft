from __future__ import annotations

import copy

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
    + [DataType.bool(), DataType.binary(), DataType.fixed_size_binary(1), DataType.date()]
)


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
        (list[str], DataType.list(DataType.string())),
        (
            {"foo": list[str], "bar": int},
            DataType.struct({"foo": DataType.list(DataType.string()), "bar": DataType.int64()}),
        ),
        (list[list[str]], DataType.list(DataType.list(DataType.string()))),
    ],
)
def test_datatype_parsing(source, expected):
    assert DataType._infer_type(source) == expected
