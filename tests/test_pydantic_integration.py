from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, NamedTuple, Optional, TypeVar

import pytest
from pydantic import BaseModel

from daft import DataType
from daft.pydantic_integration import daft_dataype_for

T = TypeVar("T")


def _test_logic(in_type: type, expected: type) -> None:
    try:
        actual = daft_dataype_for(in_type)
    except Exception as err:  # type: ignore
        raise ValueError(f"Failed to convert {in_type} into {expected} due to: {err}") from err
    assert actual == expected, f"Expecting {expected} from {in_type} but got {actual}"


CHECK: list[tuple[type, DataType]] = [
    (int, DataType.int64()),
    (float, DataType.float64()),
    (str, DataType.string()),
    (bool, DataType.bool()),
    (bytes, DataType.binary()),
]


@pytest.mark.parametrize("in_type, expected", CHECK)
def test_builtins(in_type, expected):
    _test_logic(in_type, expected)


class Simple(BaseModel):
    name: str
    age: int


class SimplePlus(Simple, Generic[T]):
    data: T


@dataclass(frozen=True)
class SomeDataclass:
    name: str
    age: int


@dataclass(frozen=True)
class SomeGenericDataclass(SomeDataclass, Generic[T]):
    data: T


SIMPLE_ARROW_DAFT_TYPE: DataType = DataType.struct(
    {
        "name": DataType.string(),
        "age": DataType.int64(),
    }
)


GENERIC_SIMPLE_ARROW_DAFT_TYPE = DataType.struct(
    {
        "name": DataType.string(),
        "age": DataType.int64(),
        "data": DataType.float64(),
    }
)


@pytest.mark.parametrize(
    "inner_type,expected",
    [
        (Simple, SIMPLE_ARROW_DAFT_TYPE),
        (SimplePlus[float], GENERIC_SIMPLE_ARROW_DAFT_TYPE),
    ],
)
def test_pydantic(inner_type, expected):
    _test_logic(inner_type, expected)


@pytest.mark.parametrize(
    "inner_type,expected",
    [
        (SomeDataclass, SIMPLE_ARROW_DAFT_TYPE),
        (SomeGenericDataclass[float], GENERIC_SIMPLE_ARROW_DAFT_TYPE),
    ],
)
def test_dataclass(inner_type, expected):
    _test_logic(inner_type, expected)


@pytest.mark.parametrize(
    "item_type, expected_inner",
    [
        (int, DataType.int64()),
        (float, DataType.float64()),
        (str, DataType.string()),
        (bool, DataType.bool()),
        (bytes, DataType.binary()),
        (Simple, SIMPLE_ARROW_DAFT_TYPE),
        (SomeDataclass, SIMPLE_ARROW_DAFT_TYPE),
        (SimplePlus[float], GENERIC_SIMPLE_ARROW_DAFT_TYPE),
        (SomeGenericDataclass[float], GENERIC_SIMPLE_ARROW_DAFT_TYPE),
    ],
)
def test_list(item_type, expected_inner):
    in_type = list[item_type]
    expected = DataType.list(expected_inner)
    _test_logic(in_type, expected)


def _dict_types() -> Iterator[tuple[type, DataType, type, DataType]]:
    check: list[tuple[type, DataType]] = CHECK + [
        (Simple, SIMPLE_ARROW_DAFT_TYPE),
        (SomeDataclass, SIMPLE_ARROW_DAFT_TYPE),
        (SimplePlus[float], GENERIC_SIMPLE_ARROW_DAFT_TYPE),
        (SomeGenericDataclass[float], GENERIC_SIMPLE_ARROW_DAFT_TYPE),
    ]
    for key_type, expected_key_type in check:
        for value_type, expected_value_type in check:
            yield (key_type, expected_key_type, value_type, expected_value_type)


@pytest.mark.parametrize(
    "key_type,expected_key_type,value_type,expected_value_type",
    list(_dict_types()),
)
def test_dict(key_type, expected_key_type, value_type, expected_value_type):
    in_type = dict[key_type, value_type]
    expected = DataType.map(expected_key_type, expected_value_type)
    _test_logic(in_type, expected)


def test_date_and_time():
    _test_logic(datetime, DataType.date())


def test_optional():
    _test_logic(Optional[int], DataType.int64())
    _test_logic(int | None, DataType.int64())


class Something1(BaseModel):
    score: float


class Something2(Something1):
    testing_date: datetime


class Complex(BaseModel):
    simples: list[Simple]
    some: Something1
    thing: Something2
    fun: bytes | None


class Contains(BaseModel):
    name: str
    complex: Complex
    a_dataclass: dict[str, SomeDataclass]


class ComplexPlus(BaseModel, Generic[T]):
    simples: list[SimplePlus[T]]
    some: Something1
    thing: Something2
    fun: bytes | None


class ContainsPlus(BaseModel, Generic[T]):
    name: str
    complex: ComplexPlus[T]
    a_dataclass: dict[str, SomeGenericDataclass[T]]


def _expected_contains(*, is_generic: bool) -> DataType:
    fun = DataType.binary()
    fun.nullable = True
    if is_generic:
        return DataType.struct(
            {
                "name": DataType.string(),
                "complex": DataType.struct(
                    {
                        "simples": DataType.list(GENERIC_SIMPLE_ARROW_DAFT_TYPE),
                        "some": DataType.struct({"score": DataType.float64()}),
                        "thing": DataType.struct({"score": DataType.float64(), "testing_date": DataType.date()}),
                        "fun": fun,
                    }
                ),
                "a_dataclass": DataType.map(DataType.string(), GENERIC_SIMPLE_ARROW_DAFT_TYPE),
            }
        )
    else:
        return DataType.struct(
            {
                "name": DataType.string(),
                "complex": DataType.struct(
                    {
                        "simples": DataType.list(SIMPLE_ARROW_DAFT_TYPE),
                        "some": DataType.struct({"score": DataType.float64()}),
                        "thing": DataType.struct({"score": DataType.float64(), "testing_date": DataType.date()}),
                        "fun": fun,
                    }
                ),
                "a_dataclass": DataType.map(DataType.string(), SIMPLE_ARROW_DAFT_TYPE),
            }
        )


def test_complex_pydantic_and_nested():
    in_type = Contains
    expected = _expected_contains(is_generic=False)

    _test_logic(in_type, expected)
    _test_logic(list[in_type], DataType.list(expected))
    _test_logic(dict[str, in_type], DataType.map(DataType.string(), expected))


def test_complex_pydantic_and_nested_generic():
    in_type = ContainsPlus[float]
    expected = _expected_contains(is_generic=True)

    _test_logic(in_type, expected)
    _test_logic(list[in_type], DataType.list(expected))
    _test_logic(dict[str, in_type], DataType.map(DataType.string(), expected))


class SomeNamedTuple(NamedTuple):
    name: str
    age: int


@pytest.mark.parametrize(
    "in_type",
    [
        SomeNamedTuple,
        tuple[int, str, float, bool, bytes],
        int | str,
    ],
)
def test_known_unsuported(in_type):
    with pytest.raises(TypeError):
        daft_dataype_for(in_type)
