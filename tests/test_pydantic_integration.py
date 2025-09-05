from __future__ import annotations

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


@pytest.mark.parameterize(
    "in_type, expected",
    [
        (int, DataType.int64()),
        (float, DataType.float64()),
        (str, DataType.string()),
        (bool, DataType.bool()),
        (bytes, DataType.binary()),
    ],
)
def test_builtins(in_type, expected):
    _test_logic(in_type, expected)


class Simple(BaseModel):
    name: str
    age: int


def test_simple_pydantic():
    _test_logic(
        Simple,
        DataType.struct(
            {
                "name": DataType.string(),
                "age": DataType.int64(),
            }
        ),
    )


@pytest.mark.parameterize(
    "in_type, expected",
    [
        (list[float], DataType.list(DataType.float64())),
        (list[int], DataType.list(DataType.int64())),
        (list[str], DataType.list(DataType.string())),
        (list[bool], DataType.list(DataType.bool())),
        (list[bytes], DataType.list(DataType.binary())),
        (
            list[Simple],
            DataType.list(
                DataType.struct(
                    {
                        "name": DataType.string(),
                        "age": DataType.int64(),
                    }
                )
            ),
        ),
    ],
)
def test_list(in_type, expected):
    _test_logic(in_type, expected)


def test_dict():
    pass
