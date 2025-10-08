from __future__ import annotations

from typing import Any, Literal

import pytest

import daft
from daft import col

API = Literal["Expression", "SQL", "Series"]


@pytest.fixture(scope="function")
def series(request):
    """Returns a series-like fixture backed by the respective API's function provider for batch function testing."""

    def _series(input: list[Any]):
        return {
            "Expression": _ExpressionFunctions,
            "SQL": _SqlFunctions,
            "Series": _SeriesFunctions,
        }[request.param](input=input)

    return _series


class _ExpressionFunctions:
    """Expression API function helper."""

    def __init__(self, input: list[Any]):
        self.input = input

    def __getattribute__(self, name):
        def wrapper(*args, **kwargs):
            expr = col("input").__getattribute__(name)(*args, **kwargs)
            df = daft.from_pydict({"input": object.__getattribute__(self, "input")})
            df = df.select(expr.alias("output")).to_pydict()
            return df["output"]

        return wrapper


class _SqlFunctions:
    """SQL API function helper."""

    def __init__(self, input: list[Any]):
        self.input = input

    def __getattribute__(self, name):
        def wrapper(*args, **kwargs):
            expr = f"{name}({', '.join(to_sql_args(*args, **kwargs))})"
            df = daft.from_pydict({"input": object.__getattribute__(self, "input")})
            df = daft.sql(f"SELECT {expr} AS output FROM df", df=df).to_pydict()
            return df["output"]

        return wrapper


class _SeriesFunctions:
    """Series API function helper."""

    def __init__(self, input: list[Any]):
        self.input = input

    def __getattribute__(self, name):
        raise ValueError("not supported")


def to_sql_args(*args, **kwargs) -> list[str]:
    sql_args = ["input"]
    sql_args.extend([to_sql_arg(arg) for arg in args])
    sql_args.extend([f"{name}:={to_sql_arg(arg)}" for (name, arg) in kwargs.items()])
    return sql_args


def to_sql_arg(arg) -> str:
    if arg is None:
        return "NULL"
    elif isinstance(arg, str):
        return f"'{arg}'"
    elif isinstance(arg, list):
        items = [to_sql_arg(arg) for item in arg]
        return f"[{', '.join(items)}]"
    else:
        raise ValueError("Unsupported SQL argument!")
