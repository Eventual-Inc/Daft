from __future__ import annotations

import copy
from typing import TypedDict

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
daft_embedding_types = [DataType.embedding(t, 512) for t in daft_numeric_types]
daft_tensor_types = [DataType.tensor(t) for t in daft_numeric_types]
daft_sparse_tensor_types = [DataType.sparse_tensor(t) for t in daft_numeric_types]
daft_fixed_shape_tensor_types = [DataType.tensor(t, shape=(2, 3)) for t in daft_numeric_types]
daft_fixed_shape_sparse_tensor_types = [DataType.sparse_tensor(t, shape=(2, 3)) for t in daft_numeric_types]
daft_map_types = [DataType.map(DataType.string(), t) for t in daft_nonnull_types]
daft_struct_types = [DataType.struct({"foo": t, "bar": t}) for t in daft_nonnull_types]

all_daft_types = (
    daft_nonnull_types
    + daft_list_types
    + daft_map_types
    + daft_struct_types
    + daft_embedding_types
    + daft_tensor_types
    + daft_sparse_tensor_types
    + daft_fixed_shape_tensor_types
    + daft_fixed_shape_sparse_tensor_types
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
        (
            TypedDict("Foobar", {"foo": str, "bar": int}),
            DataType.struct({"foo": DataType.string(), "bar": DataType.int64()}),
        ),
    ],
)
def test_datatype_parsing(source, expected):
    assert DataType._infer(source) == expected


@pytest.mark.parametrize(
    ["source", "expected"],
    [
        # These tests must be run in later version of Python that allow for subscripting of types
        (list[str], DataType.list(DataType.string())),
        (dict[str, int], DataType.map(DataType.string(), DataType.int64())),
        (
            TypedDict("Foobar", {"foo": list[str], "bar": int}),
            DataType.struct({"foo": DataType.list(DataType.string()), "bar": DataType.int64()}),
        ),
        (list[list[str]], DataType.list(DataType.list(DataType.string()))),
    ],
)
def test_subscripted_datatype_parsing(source, expected):
    assert DataType._infer(source) == expected


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
        assert test_type.is_list() == (test_type == DataType.list(test_type.dtype))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_fixed_size_list(test_type):
    if test_type.is_fixed_size_list():
        assert test_type.is_fixed_size_list() == (test_type == DataType.fixed_size_list(test_type.dtype, 10))


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_image(test_type):
    assert test_type.is_image() == (test_type == DataType.image())


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_fixed_shape_image(test_type):
    if test_type.is_fixed_shape_image():
        assert test_type == DataType.image(mode="RGB", height=224, width=224)


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_embedding(test_type):
    if test_type.is_embedding():
        assert test_type == DataType.embedding(test_type.dtype, test_type.size)


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_tensor(test_type):
    if test_type.is_tensor():
        assert test_type == DataType.tensor(test_type.dtype)


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_fixed_shape_tensor(test_type):
    if test_type.is_fixed_shape_tensor():
        assert test_type == DataType.tensor(test_type.dtype, test_type.shape)


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_sparse_tensor(test_type):
    if test_type.is_sparse_tensor():
        assert test_type == DataType.sparse_tensor(test_type.dtype)


@pytest.mark.parametrize("test_type", all_daft_types)
def test_is_fixed_shape_sparse_tensor(test_type: DataType):
    if test_type.is_fixed_shape_sparse_tensor():
        test_type == DataType.sparse_tensor(test_type.dtype, shape=test_type.shape)


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
        assert test_type.size == 10
    elif test_type.is_embedding():
        assert test_type.size == 512
    else:
        try:
            test_type.size
            pytest.fail("Expected AttributeError")
        except AttributeError:
            assert True


@pytest.mark.parametrize("test_type", all_daft_types)
def test_inner_type_property(test_type: DataType):
    if (
        test_type.is_list()
        or test_type.is_fixed_size_list()
        or test_type.is_tensor()
        or test_type.is_sparse_tensor()
        or test_type.is_fixed_shape_tensor()
        or test_type.is_fixed_shape_sparse_tensor()
        or test_type.is_embedding()
    ):
        try:
            assert test_type.dtype is not None
        except AttributeError:
            assert False


@pytest.mark.parametrize("test_type", all_daft_types)
def test_struct_fields_property(test_type: DataType):
    if test_type.is_struct():
        assert test_type.fields is not None
    else:
        try:
            test_type.fields
            pytest.fail("Expected AttributeError")
        except AttributeError:
            assert True


@pytest.mark.parametrize("test_type", all_daft_types)
def test_precision_and_scale_properties(test_type):
    if test_type.is_decimal128():
        assert test_type.precision == 10
        assert test_type.scale == 2
    else:
        try:
            test_type.precision
            test_type.scale
            pytest.fail("Expected AttributeError")
        except AttributeError:
            assert True


@pytest.mark.parametrize("test_type", all_daft_types)
def test_time_unit_property(test_type: DataType):
    if test_type.is_time() or test_type.is_duration() or test_type.is_timestamp():
        assert test_type.timeunit == "ns"
    else:
        try:
            test_type.timeunit
            pytest.fail("Expected AttributeError")
        except AttributeError:
            assert True


@pytest.mark.parametrize("test_type", all_daft_types)
def test_image_properties(test_type: DataType):
    if test_type.is_image() or test_type.is_fixed_shape_image():
        assert test_type.image_mode == "RGB"
    else:
        try:
            test_type.image_mode
            pytest.fail("Expected AttributeError")
        except AttributeError:
            assert True


@pytest.mark.parametrize("test_type", all_daft_types)
def test_use_offset_indices_property(test_type: DataType):
    if test_type.is_sparse_tensor() or test_type.is_fixed_shape_sparse_tensor():
        assert test_type.use_offset_indices is False
    else:
        try:
            test_type.use_offset_indices
            pytest.fail("Expected AttributeError")
        except AttributeError:
            assert True


@pytest.mark.parametrize("test_type", all_daft_types)
def test_map_key_property(test_type: DataType):
    if test_type.is_map():
        assert test_type.key_type == DataType.string()
    else:
        try:
            test_type.key_type
            pytest.fail("Expected AttributeError")
        except AttributeError:
            assert True


@pytest.mark.parametrize("test_type", all_daft_types)
def test_map_value_property(test_type: DataType):
    if test_type.is_map():
        assert test_type.value_type is not None
    else:
        try:
            test_type.value_type
            pytest.fail("Expected AttributeError")
        except AttributeError:
            assert True


_SIMPLE_TYPES = [
    ("int8", DataType.int8()),
    ("int16", DataType.int16()),
    ("int32", DataType.int32()),
    ("int64", DataType.int64()),
    ("uint8", DataType.uint8()),
    ("uint16", DataType.uint16()),
    ("uint32", DataType.uint32()),
    ("uint64", DataType.uint64()),
    ("float32", DataType.float32()),
    ("float64", DataType.float64()),
    ("string", DataType.string()),
    ("bool", DataType.bool()),
    ("binary", DataType.binary()),
    ("null", DataType.null()),
    ("date", DataType.date()),
    ("interval", DataType.interval()),
    ("python", DataType.python()),
]


@pytest.mark.parametrize("name,expected", _SIMPLE_TYPES)
def test_datatype_property_access_equals_call(name, expected):
    """Property access and method call should return equal results (issue #2765)."""
    prop = getattr(DataType, name)
    call = getattr(DataType, name)()
    assert prop == expected
    assert call == expected
    assert prop == call


@pytest.mark.parametrize("name,expected", _SIMPLE_TYPES)
def test_datatype_property_hash_consistency(name, expected):
    """Property access and method call should have identical hash values."""
    prop = getattr(DataType, name)
    call = getattr(DataType, name)()
    assert hash(prop) == hash(call)
    assert hash(prop) == hash(expected)


def test_datatype_property_set_dedup():
    """Verify property access and method calls deduplicate correctly in sets."""
    # Same type via property and calls should deduplicate to 1
    s1 = {DataType.int8, DataType.int8(), DataType.int8()}
    assert len(s1) == 1

    # Different types should remain distinct
    s2 = {DataType.int8, DataType.int8(), DataType.uint8, DataType.uint8()}
    assert len(s2) == 2

    # Mixed types should deduplicate per-type
    s3 = {DataType.int8, DataType.int8(), DataType.uint64, DataType.uint64(),
          DataType.string, DataType.string()}
    assert len(s3) == 3
    assert DataType.int8 in s3
    assert DataType.uint64 in s3
    assert DataType.string in s3
    assert DataType.int64 not in s3


def test_datatype_property_dict_key():
    """Verify property access and method calls are interchangeable as dict keys."""
    # Keys created via property access, looked up via method calls
    d1 = {DataType.int64: "signed", DataType.uint64: "unsigned", DataType.string: "text"}
    assert d1[DataType.int64()] == "signed"
    assert d1[DataType.uint64()] == "unsigned"
    assert d1[DataType.string()] == "text"

    # Keys created via method calls, looked up via property access
    d2 = {DataType.float32(): "float32", DataType.float64(): "float64", DataType.bool(): "boolean"}
    assert d2[DataType.float32] == "float32"
    assert d2[DataType.float64] == "float64"
    assert d2[DataType.bool] == "boolean"


def test_datatype_property_repr():
    """Verify repr output for all 17 property-accessible types."""
    repr_checks = [
        ("int8", "Int8"),
        ("int16", "Int16"),
        ("int32", "Int32"),
        ("int64", "Int64"),
        ("uint8", "UInt8"),
        ("uint16", "UInt16"),
        ("uint32", "UInt32"),
        ("uint64", "UInt64"),
        ("float32", "Float32"),
        ("float64", "Float64"),
        ("string", "String"),
        ("bool", "Bool"),
        ("binary", "Binary"),
        ("null", "Null"),
        ("date", "Date"),
        ("interval", "Interval"),
        ("python", "Python"),
    ]
    for name, expected_repr in repr_checks:
        prop = getattr(DataType, name)
        assert expected_repr in repr(prop), f"Expected '{expected_repr}' in repr(DataType.{name})"


def test_datatype_property_call_returns_datatype():
    """Verify calling property results (e.g. DataType.int8()) returns DataType instances."""
    type_names = [
        "int8", "int16", "int32", "int64",
        "uint8", "uint16", "uint32", "uint64",
        "float32", "float64",
        "string", "bool", "binary", "null",
        "date", "interval", "python",
    ]
    for name in type_names:
        result = getattr(DataType, name)()
        assert isinstance(result, DataType), f"DataType.{name}() should return DataType instance"


def test_datatype_property_with_complex_types():
    """Verify property access works as arguments to complex type constructors.

    Tests that DataType.prop and DataType.prop() are interchangeable when passed
    to constructors like list(), struct(), embedding(), tensor(), etc.
    """
    # list
    list_dtype_prop = DataType.list(DataType.int64)
    list_dtype_call = DataType.list(DataType.int64())
    assert list_dtype_prop == list_dtype_call

    # fixed_size_list
    fsl_prop = DataType.fixed_size_list(DataType.float32, 10)
    fsl_call = DataType.fixed_size_list(DataType.float32(), 10)
    assert fsl_prop == fsl_call

    # struct
    struct_prop = DataType.struct({"a": DataType.string, "b": DataType.int64})
    struct_call = DataType.struct({"a": DataType.string(), "b": DataType.int64()})
    assert struct_prop == struct_call

    # embedding
    embedding_prop = DataType.embedding(DataType.float32, 512)
    embedding_call = DataType.embedding(DataType.float32(), 512)
    assert embedding_prop == embedding_call

    # tensor
    tensor_prop = DataType.tensor(DataType.float64)
    tensor_call = DataType.tensor(DataType.float64())
    assert tensor_prop == tensor_call

    # sparse_tensor
    sparse_tensor_prop = DataType.sparse_tensor(DataType.float64)
    sparse_tensor_call = DataType.sparse_tensor(DataType.float64())
    assert sparse_tensor_prop == sparse_tensor_call

    # fixed_size_binary
    fsb_prop = DataType.fixed_size_binary(16)
    fsb_call = DataType.fixed_size_binary(16)
    assert fsb_prop == fsb_call

    # map
    map_prop = DataType.map(DataType.string, DataType.int64)
    map_call = DataType.map(DataType.string(), DataType.int64())
    assert map_prop == map_call

    # image
    image_prop = DataType.image(mode="RGB")
    image_call = DataType.image(mode="RGB")
    assert image_prop == image_call

    # decimal128
    decimal_prop = DataType.decimal128(precision=10, scale=2)
    decimal_call = DataType.decimal128(precision=10, scale=2)
    assert decimal_prop == decimal_call

    # timestamp
    ts_prop = DataType.timestamp(timeunit="ns")
    ts_call = DataType.timestamp(timeunit="ns")
    assert ts_prop == ts_call

    # time
    time_prop = DataType.time(timeunit="us")
    time_call = DataType.time(timeunit="us")
    assert time_prop == time_call

    # duration
    dur_prop = DataType.duration(timeunit="ms")
    dur_call = DataType.duration(timeunit="ms")
    assert dur_prop == dur_call


def test_datatype_property_is_methods():
    """Properties should work correctly with DataType is_* methods."""
    assert DataType.int8.is_int8()
    assert DataType.uint64.is_uint64()
    assert DataType.float64.is_float64()
    assert DataType.string.is_string()
    assert DataType.bool.is_boolean()
    assert DataType.null.is_null()
    assert DataType.date.is_date()
    assert DataType.interval.is_interval()
    assert DataType.python.is_python()


def test_datatype_constructor_names_no_underscore():
    """Verify _DATATYPE_CONSTRUCTOR_SET does not contain underscore-prefixed names.

    This prevents a regression where @datatype_constructor on private methods
    (_int8, _int16, etc.) would pollute the set, causing double-underscore methods
    like Expression.as__int8 to be created.
    """
    from daft.expressions import Expression

    constructor_names = DataType._constructor_names()
    underscore_names = [n for n in constructor_names if n.startswith("_")]
    assert not underscore_names, f"Constructor names should not start with underscore: {underscore_names}"

    # Verify no double-underscore methods on Expression
    bad_methods = [m for m in dir(Expression) if m.startswith("as__")]
    assert not bad_methods, f"Expression should not have double-underscore methods: {bad_methods}"
