from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import NamedTuple, Optional

import pytest
from pydantic import BaseModel

from daft import DataType
from daft.pydantic_integration import daft_pyarrow_datatype


def _test_logic(in_type: type, expected: type) -> None:
    try:
        actual = daft_pyarrow_datatype(in_type)
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


SIMPLE_ARROW_DAFT_TYPE: DataType = DataType.struct(
    {
        "name": DataType.string(),
        "age": DataType.int64(),
    }
)


def test_simple_pydantic():
    _test_logic(Simple, SIMPLE_ARROW_DAFT_TYPE)


@pytest.mark.parametrize(
    "item_type, expected_inner",
    [
        (int, DataType.int64()),
        (float, DataType.float64()),
        (str, DataType.string()),
        (bool, DataType.bool()),
        (bytes, DataType.binary()),
        (Simple, SIMPLE_ARROW_DAFT_TYPE),
    ],
)
def test_list(item_type, expected_inner):
    in_type = list[item_type]
    expected = DataType.list(expected_inner)
    _test_logic(in_type, expected)


def _dict_types() -> Iterator[tuple[type, DataType, type, DataType]]:
    check: list[tuple[type, DataType]] = CHECK + [(Simple, SIMPLE_ARROW_DAFT_TYPE)]
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


def test_complex_pydantic_and_nested():
    fun = DataType.binary()
    fun.nullable = True

    in_type = Contains
    expected = DataType.struct(
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
        }
    )

    _test_logic(in_type, expected)
    _test_logic(list[Contains], DataType.list(expected))
    _test_logic(dict[str, Contains], DataType.map(DataType.string(), expected))


@dataclass(frozen=True)
class SomeDataclass:
    name: str
    age: int

# @dataclass(frozen=True)



@pytest.mark.parametrize(
    "inner_type,expected",
    [
        (SomeDataclass, SIMPLE_ARROW_DAFT_TYPE),
    ]
)
def test_dataclass(inner_type, expected):
    _test_logic(inner_type, expected)



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
        daft_pyarrow_datatype(in_type)
