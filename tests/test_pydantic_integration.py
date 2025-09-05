from __future__ import annotations

from collections.abc import Iterator

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


@pytest.mark.parameterize("in_type, expected", CHECK)
def test_builtins(in_type, expected):
    _test_logic(in_type, expected)


class Simple(BaseModel):
    name: str
    age: int


SIMPLE_ARROW_DAFT_TYPE = DataType.struct(
    {
        "name": DataType.string(),
        "age": DataType.int64(),
    }
)


def test_simple_pydantic():
    _test_logic(Simple, SIMPLE_ARROW_DAFT_TYPE)


@pytest.mark.parameterize(
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


@pytest.mark.parameterize(
    "key_type,expected_key_type,value_type,expected_value_type",
    list(_dict_types()),
)
def test_dict(key_type, expected_key_type, value_type, expected_value_type):
    in_type = dict[key_type, value_type]
    expected = DataType.map(expected_key_type, expected_value_type)
    _test_logic(in_type, expected)
