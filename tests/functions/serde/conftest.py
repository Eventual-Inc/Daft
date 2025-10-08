from __future__ import annotations

from typing import Any

import pytest

import daft
from daft import col
from daft.datatype import DataTypeLike


@pytest.fixture(scope="function")
def deserialize():
    def _deserialize(items: list[str], format: str, dtype: DataTypeLike) -> list[Any]:
        field_name = "field_to_deserialize"
        df = daft.from_pydict({field_name: items})
        df = df.select(col(field_name).deserialize(format, dtype))  # type: ignore
        return df.to_pydict()[field_name]

    yield _deserialize


@pytest.fixture(scope="function")
def try_deserialize():
    def _try_deserialize(items: list[str], format: str, dtype: DataTypeLike) -> list[Any]:
        field_name = "field_to_deserialize"
        df = daft.from_pydict({field_name: items})
        df = df.select(col(field_name).deserialize(format, dtype))  # type: ignore
        return df.to_pydict()[field_name]

    yield _try_deserialize
