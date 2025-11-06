from __future__ import annotations

import datetime
import decimal
import sys
from typing import NamedTuple, TypedDict

import jax
import jaxtyping
import numpy as np
import numpy.typing as npt
import pandas
import PIL.Image
import pytest
import torch
from pydantic import BaseModel, Field, computed_field

from daft import DataType as dt
from daft import Series
from daft.daft import ImageMode
from daft.datatype import MediaType, TimeUnit
from daft.file import File, VideoFile

try:  # pragma: no cover - optional dependency
    import tensorflow  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - optional dependency
    tensorflow = None  # type: ignore[assignment]


# Pydantic test models
class SimplePydanticModel(BaseModel):
    name: str
    age: int


class NestedPydanticModel(BaseModel):
    user: SimplePydanticModel
    active: bool


class PydanticWithAlias(BaseModel):
    model_config = {"serialize_by_alias": True}
    full_name: str = Field(alias="name", serialization_alias="fullName")
    user_age: int = Field(alias="age")


class PydanticWithAliasNoSerializeByAlias(BaseModel):
    full_name: str = Field(alias="name", serialization_alias="fullName")
    user_age: int = Field(alias="age")


class PydanticWithComputedField(BaseModel):
    first_name: str
    last_name: str

    @computed_field
    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"


class PydanticWithMixedTypes(BaseModel):
    numbers: list[int]
    metadata: dict[str, str]


class EmptyPydanticModel(BaseModel):
    pass


class SimpleNamedTuple(NamedTuple):
    foo: str
    bar: int


class PydanticWithNamedTuple(BaseModel):
    values: SimpleNamedTuple


@pytest.mark.parametrize(
    "user_provided_type, expected_datatype",
    [
        (type(None), dt.null()),
        (bool, dt.bool()),
        (str, dt.string()),
        (bytes, dt.binary()),
        (int, dt.int64()),
        (float, dt.float64()),
        (datetime.datetime, dt.timestamp(TimeUnit.us())),
        (datetime.date, dt.date()),
        (datetime.time, dt.time(TimeUnit.us())),
        (datetime.timedelta, dt.duration(TimeUnit.us())),
        (list, dt.list(dt.python())),
        (list[str], dt.list(dt.string())),
        (list[list], dt.list(dt.list(dt.python()))),
        (list[list[str]], dt.list(dt.list(dt.string()))),
        (
            TypedDict("Foobar", {"foo": str, "bar": int}),
            dt.struct({"foo": dt.string(), "bar": dt.int64()}),
        ),
        (dict, dt.map(dt.python(), dt.python())),
        (dict[str, str], dt.map(dt.string(), dt.string())),
        (tuple, dt.list(dt.python())),
        (tuple[str, ...], dt.list(dt.string())),
        (tuple[str, int], dt.struct({"_0": dt.string(), "_1": dt.int64()})),
        (np.ndarray, dt.tensor(dt.python())),
        (torch.Tensor, dt.tensor(dt.python())),
        (torch.FloatTensor, dt.tensor(dt.float32())),
        (torch.DoubleTensor, dt.tensor(dt.float64())),
        (torch.ByteTensor, dt.tensor(dt.uint8())),
        (torch.CharTensor, dt.tensor(dt.int8())),
        (torch.ShortTensor, dt.tensor(dt.int16())),
        (torch.IntTensor, dt.tensor(dt.int32())),
        (torch.LongTensor, dt.tensor(dt.int64())),
        (torch.BoolTensor, dt.tensor(dt.bool())),
        *([] if tensorflow is None else [(tensorflow.Tensor, dt.tensor(dt.python()))]),
        (jax.Array, dt.tensor(dt.python())),
        (npt.NDArray[int], dt.tensor(dt.int64())),
        (np.bool_, dt.bool()),
        (np.int8, dt.int8()),
        (np.uint8, dt.uint8()),
        (np.int16, dt.int16()),
        (np.uint16, dt.uint16()),
        (np.int32, dt.int32()),
        (np.uint32, dt.uint32()),
        (np.int64, dt.int64()),
        (np.uint64, dt.uint64()),
        (np.float32, dt.float32()),
        (np.float64, dt.float64()),
        (np.datetime64, dt.timestamp(TimeUnit.us())),
        (pandas.Series, dt.list(dt.python())),
        (PIL.Image.Image, dt.image()),
        (Series, dt.list(dt.python())),
        (File, dt.file(MediaType.unknown())),
        (VideoFile, dt.file(MediaType.video())),
        (object, dt.python()),
        # Pydantic models
        (SimplePydanticModel, dt.struct({"name": dt.string(), "age": dt.int64()})),
        (
            NestedPydanticModel,
            dt.struct(
                {
                    "user": dt.struct({"name": dt.string(), "age": dt.int64()}),
                    "active": dt.bool(),
                }
            ),
        ),
        # TODO: Uncomment this when we update to pydantic>=2.11 which supports `serialize_by_alias`
        # (PydanticWithAlias, dt.struct({"fullName": dt.string(), "age": dt.int64()})),
        (
            PydanticWithAliasNoSerializeByAlias,
            dt.struct({"full_name": dt.string(), "user_age": dt.int64()}),
        ),
        (
            PydanticWithComputedField,
            dt.struct(
                {
                    "first_name": dt.string(),
                    "last_name": dt.string(),
                    "full_name": dt.string(),
                }
            ),
        ),
        (
            PydanticWithMixedTypes,
            dt.struct(
                {
                    "numbers": dt.list(dt.int64()),
                    "metadata": dt.map(dt.string(), dt.string()),
                }
            ),
        ),
        (EmptyPydanticModel, dt.struct({})),
        # TODO: uncomment once we support named tuples
        # (SimpleNamedTuple, dt.struct({"foo": dt.string(), "bar": dt.int64()})),
        # (PydanticWithNamedTuple, dt.struct({"values": dt.struct({"foo": dt.string(), "bar": dt.int64()})})),
    ],
)
def test_infer_from_type(user_provided_type, expected_datatype):
    actual = dt.infer_from_type(user_provided_type)
    assert actual == expected_datatype


@pytest.mark.parametrize(
    "dtype_class, expected_dtype",
    [
        (jaxtyping.Bool, dt.bool()),
        (jaxtyping.Int8, dt.int8()),
        (jaxtyping.UInt8, dt.uint8()),
        (jaxtyping.Int16, dt.int16()),
        (jaxtyping.UInt16, dt.uint16()),
        (jaxtyping.Int32, dt.int32()),
        (jaxtyping.UInt32, dt.uint32()),
        (jaxtyping.Int64, dt.int64()),
        (jaxtyping.Int, dt.int64()),
        (jaxtyping.Integer, dt.int64()),
        (jaxtyping.UInt64, dt.uint64()),
        (jaxtyping.UInt, dt.uint64()),
        (jaxtyping.Float32, dt.float32()),
        (jaxtyping.Float64, dt.float64()),
        (jaxtyping.Float, dt.float64()),
        (jaxtyping.Real, dt.float64()),
        (jaxtyping.Shaped, dt.python()),
        (jaxtyping.Complex, dt.python()),
    ],
)
@pytest.mark.parametrize(
    "shape_spec, expected_shape",
    [
        ("", ()),
        ("10", (10,)),
        ("10 20", (10, 20)),
        ("3 224 224", (3, 224, 224)),
        ("n", None),
        ("n d", None),
        ("n 512", None),
        ("512 512 _", None),
        ("... 1 2 3", None),
    ],
)
@pytest.mark.parametrize(
    "array_type",
    [
        jax.Array,
        np.ndarray,
        torch.Tensor,
        *([] if tensorflow is None else [tensorflow.Tensor]),
    ],
)
def test_infer_from_jaxtyping(dtype_class, expected_dtype, shape_spec, expected_shape, array_type):
    jaxtyping_annotation = dtype_class[array_type, shape_spec]
    actual_datatype = dt.infer_from_type(jaxtyping_annotation)

    expected_datatype = dt.tensor(expected_dtype, shape=expected_shape)
    assert actual_datatype == expected_datatype


@pytest.mark.parametrize(
    "user_provided_object, expected_datatype",
    [
        (None, dt.null()),
        (False, dt.bool()),
        ("a", dt.string()),
        (b"a", dt.binary()),
        (1, dt.int64()),
        (2**63, dt.uint64()),
        (1.0, dt.float64()),
        (datetime.datetime.now(), dt.timestamp(TimeUnit.us())),
        (datetime.date.today(), dt.date()),
        (datetime.time(microsecond=1), dt.time(TimeUnit.us())),
        (datetime.timedelta(microseconds=1), dt.duration(TimeUnit.us())),
        ([], dt.list(dt.null())),
        (["a", "b", "c"], dt.list(dt.string())),
        ({}, dt.struct({"": dt.null()})),
        ({"foo": "1", "bar": 2}, dt.struct({"foo": dt.string(), "bar": dt.int64()})),
        ({1: 2, 3: 4}, dt.map(dt.int64(), dt.int64())),
        ((), dt.struct({"": dt.null()})),
        (("0", 1), dt.struct({"_0": dt.string(), "_1": dt.int64()})),
        (decimal.Decimal("1.5"), dt.decimal128(38, 1)),
        (decimal.Decimal("4.56e-2"), dt.decimal128(38, 4)),  # 0.0456
        (decimal.Decimal("1.23e2"), dt.decimal128(38, 0)),  # 123
        (decimal.Decimal("7.89E3"), dt.decimal128(38, 0)),  # 7890
        (decimal.Decimal("7.89E+3"), dt.decimal128(38, 0)),  # 7890
        (decimal.Decimal("1.2345e-4"), dt.decimal128(38, 8)),  # 0.00012345
        (np.array([1, 2, 3]), dt.tensor(dt.int64())),
        (torch.tensor([1, 2, 3]), dt.tensor(dt.int64())),
        *([] if tensorflow is None else [(tensorflow.constant([1, 2, 3]), dt.tensor(dt.int32()))]),
        (jax.numpy.array([1, 2, 3]), dt.tensor(dt.int32())),
        (np.bool_(False), dt.bool()),
        (np.int8(1), dt.int8()),
        (np.uint8(1), dt.uint8()),
        (np.int16(1), dt.int16()),
        (np.uint16(1), dt.uint16()),
        (np.int32(1), dt.int32()),
        (np.uint32(1), dt.uint32()),
        (np.int64(1), dt.int64()),
        (np.uint64(1), dt.uint64()),
        (np.float32(1.0), dt.float32()),
        (np.float64(1.0), dt.float64()),
        (np.datetime64(1, "Y"), dt.date()),
        (np.datetime64(1, "M"), dt.date()),
        (np.datetime64(1, "W"), dt.date()),
        (np.datetime64(1, "D"), dt.date()),
        (np.datetime64(1, "h"), dt.timestamp(TimeUnit.s())),
        (np.datetime64(1, "m"), dt.timestamp(TimeUnit.s())),
        (np.datetime64(1, "s"), dt.timestamp(TimeUnit.s())),
        (np.datetime64(1, "ms"), dt.timestamp(TimeUnit.ms())),
        (np.datetime64(1, "us"), dt.timestamp(TimeUnit.us())),
        (np.datetime64(1, "ns"), dt.timestamp(TimeUnit.ns())),
        (np.datetime64(1, "ps"), dt.timestamp(TimeUnit.ns())),
        (pandas.Series([1, 2, 3]), dt.list(dt.int64())),
        (PIL.Image.new("L", (10, 20)), dt.image(ImageMode.L)),
        (PIL.Image.new("LA", (10, 20)), dt.image(ImageMode.LA)),
        (PIL.Image.new("RGB", (10, 20)), dt.image(ImageMode.RGB)),
        (PIL.Image.new("RGBA", (10, 20)), dt.image(ImageMode.RGBA)),
        (Series.from_pylist([1, 2, 3]), dt.list(dt.int64())),
        (File(b"1234"), dt.file(MediaType.unknown())),
        (VideoFile(b"1234"), dt.file(MediaType.video())),
        (object(), dt.python()),
        # Nested lists
        ([[1, 2], [3, 4]], dt.list(dt.list(dt.int64()))),
        ([["a", "b"], ["c", "d"]], dt.list(dt.list(dt.string()))),
        ([[[1]], [[2, 3]]], dt.list(dt.list(dt.list(dt.int64())))),
        # Mixed nested lists
        ([[1, 2], ["a", "b"]], dt.list(dt.list(dt.string()))),
        # Nested structs
        (
            {"outer": {"inner": 1}},
            dt.struct({"outer": dt.struct({"inner": dt.int64()})}),
        ),
        (
            {"a": {"b": {"c": "nested"}}},
            dt.struct({"a": dt.struct({"b": dt.struct({"c": dt.string()})})}),
        ),
        # Mixed nested types - list of structs
        (
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
            dt.list(dt.struct({"name": dt.string(), "age": dt.int64()})),
        ),
        # Mixed nested types - struct with lists
        (
            {"numbers": [1, 2, 3], "strings": ["a", "b"]},
            dt.struct({"numbers": dt.list(dt.int64()), "strings": dt.list(dt.string())}),
        ),
        # Complex nested structures
        (
            {"users": [{"profile": {"name": "Alice"}, "scores": [1, 2, 3]}]},
            dt.struct(
                {
                    "users": dt.list(
                        dt.struct(
                            {
                                "profile": dt.struct({"name": dt.string()}),
                                "scores": dt.list(dt.int64()),
                            }
                        )
                    )
                }
            ),
        ),
        # Nested tuples as structs
        (
            (1, (2, 3)),
            dt.struct(
                {
                    "_0": dt.int64(),
                    "_1": dt.struct({"_0": dt.int64(), "_1": dt.int64()}),
                }
            ),
        ),
        # Empty nested structures
        ([[], []], dt.list(dt.list(dt.null()))),
        ([{}, {}], dt.list(dt.struct({"": dt.null()}))),
        # Pydantic model instances
        (
            SimplePydanticModel(name="Alice", age=30),
            dt.struct({"name": dt.string(), "age": dt.int64()}),
        ),
        (
            NestedPydanticModel(user=SimplePydanticModel(name="Bob", age=25), active=True),
            dt.struct(
                {
                    "user": dt.struct({"name": dt.string(), "age": dt.int64()}),
                    "active": dt.bool(),
                }
            ),
        ),
        # TODO: Uncomment this when we update to pydantic>=2.11 which supports `serialize_by_alias`
        # (PydanticWithAlias(name="Jane Doe", age=28), dt.struct({"fullName": dt.string(), "age": dt.int64()})),
        (
            PydanticWithAliasNoSerializeByAlias(name="Jane Doe", age=28),
            dt.struct({"full_name": dt.string(), "user_age": dt.int64()}),
        ),
        (
            PydanticWithComputedField(first_name="John", last_name="Smith"),
            dt.struct(
                {
                    "first_name": dt.string(),
                    "last_name": dt.string(),
                    "full_name": dt.string(),
                }
            ),
        ),
        (
            PydanticWithMixedTypes(numbers=[1, 2, 3], metadata={"key": "value"}),
            dt.struct(
                {
                    "numbers": dt.list(dt.int64()),
                    "metadata": dt.map(dt.string(), dt.string()),
                }
            ),
        ),
        (
            PydanticWithMixedTypes(numbers=[1, 2, 3], metadata={"key": "value"}),
            dt.struct(
                {
                    "numbers": dt.list(dt.int64()),
                    "metadata": dt.map(dt.string(), dt.string()),
                }
            ),
        ),
        (EmptyPydanticModel(), dt.struct({"": dt.null()})),
        # TODO: uncomment once we support named tuples
        # (SimpleNamedTuple(foo="1", bar=2), dt.struct({"foo": dt.string(), "bar": dt.int64()})),
        # (PydanticWithNamedTuple(values=SimpleNamedTuple(foo="1", bar=2)), dt.struct({"values": dt.struct({"foo": dt.string(), "bar": dt.int64()})})),
    ],
)
def test_infer_from_object(user_provided_object, expected_datatype):
    actual = dt.infer_from_object(user_provided_object)
    assert actual == expected_datatype


@pytest.mark.parametrize(
    "obj",
    [
        None,
        False,
        "a",
        b"a",
        1,
        2**63,
        1.0,
        datetime.datetime.now(),
        datetime.date.today(),
        datetime.time(microsecond=1),
        datetime.timedelta(microseconds=1),
        [],
        ["a", "b", "c"],
        {},
        {"foo": "1", "bar": 2},
        decimal.Decimal("1.5"),
        object(),
        # Nested structures for roundtrip testing
        [[1, 2], [3, 4]],
        [["a", "b"], ["c", "d"]],
        {"outer": {"inner": 1}},
        [{"name": "Alice", "age": 30}],
        {"numbers": [1, 2, 3]},
    ],
)
def test_roundtrippable(obj):
    input = [None, obj, None]
    s = Series.from_pylist(input)
    assert input == s.to_pylist()


@pytest.mark.parametrize(
    "arr",
    [
        np.array([1, 2, 3], dtype=np.int64),
        np.array([1.0, 2.0, 3.0], dtype=np.float64),
        np.array([[1, 2], [3, 4]], dtype=np.int64),
        np.array([["a", "b"], ["c", "d"]], dtype=object),
        np.array([], dtype=np.float64),
        np.array([True, False, True], dtype=bool),
        np.array([1, 2, 3], dtype=np.uint8),
        np.array([1, 2, 3], dtype=np.int8),
        np.array([1, 2, 3], dtype=np.uint16),
        np.array([1, 2, 3], dtype=np.int16),
        np.array([1, 2, 3], dtype=np.uint32),
        np.array([1, 2, 3], dtype=np.int32),
        np.array([1, 2, 3], dtype=np.uint64),
        np.array([1, 2, 3], dtype=np.float32),
    ],
)
def test_roundtrippable_numpy(arr):
    input = [None, arr, None]
    s = Series.from_pylist(input)
    pre, out_arr, post = s.to_pylist()
    assert pre is None
    assert post is None
    assert (out_arr == arr).all()


def test_decimals_with_scientific_notation():
    """Test roundtripping decimals in (-1, 1) with exponents 0 to -18.

    See: https://github.com/Eventual-Inc/Daft/issues/5302
    """
    import daft

    decimals = [
        decimal.Decimal("-1E0"),
        decimal.Decimal("-1E-1"),
        decimal.Decimal("-1E-2"),
        decimal.Decimal("-1E-3"),
        decimal.Decimal("-1E-4"),
        decimal.Decimal("-1E-5"),
        decimal.Decimal("-1E-6"),
        decimal.Decimal("-1E-7"),
        decimal.Decimal("-1E-8"),
        decimal.Decimal("-1E-9"),
        decimal.Decimal("-1E-10"),
        decimal.Decimal("-1E-11"),
        decimal.Decimal("-1E-12"),
        decimal.Decimal("-1E-13"),
        decimal.Decimal("-1E-14"),
        decimal.Decimal("-1E-15"),
        decimal.Decimal("-1E-16"),
        decimal.Decimal("-1E-17"),
        decimal.Decimal("-1E-18"),
        decimal.Decimal("0E0"),
        decimal.Decimal("1E-18"),
        decimal.Decimal("1E-17"),
        decimal.Decimal("1E-16"),
        decimal.Decimal("1E-15"),
        decimal.Decimal("1E-14"),
        decimal.Decimal("1E-13"),
        decimal.Decimal("1E-12"),
        decimal.Decimal("1E-11"),
        decimal.Decimal("1E-10"),
        decimal.Decimal("1E-9"),
        decimal.Decimal("1E-8"),
        decimal.Decimal("1E-7"),
        decimal.Decimal("1E-6"),
        decimal.Decimal("1E-5"),
        decimal.Decimal("1E-4"),
        decimal.Decimal("1E-3"),
        decimal.Decimal("1E-2"),
        decimal.Decimal("1E-1"),
        decimal.Decimal("1E0"),
    ]

    # assert roundtrip equality
    assert daft.from_pydict({"col": decimals}).to_pydict()["col"] == decimals


@pytest.mark.skipif(
    sys.version_info >= (3, 13),
    reason="Skipping cupy test due to: cupy_backends.cuda.api.runtime.CUDARuntimeError: cudaErrorInsufficientDriver: CUDA driver version is insufficient for CUDA runtime version and incompatibility with Python 3.13",
)
def test_cupy():
    cupy = pytest.importorskip("cupy")
    if cupy.cuda.is_available():
        assert dt.infer_from_type(cupy.ndarray) == dt.tensor(dt.python())

        arr = cupy.array([1, 2, 3])
        assert dt.infer_from_object(arr) == dt.tensor(dt.int64())
    else:
        pytest.skip("CUDA is not available")
