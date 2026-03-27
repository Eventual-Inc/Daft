"""Tests for UDF result column naming behavior.

After the fix, UDF result columns should be named after the UDF function name,
not the first input argument's column name. This prevents column name conflicts
when using `select *, udf(col)` patterns.
"""

from __future__ import annotations

import daft
from daft import DataType


def test_udf_result_column_uses_func_name():
    @daft.func(return_dtype=DataType.string())
    def my_func(col: int) -> str:
        return f"my: {col}"

    df = daft.from_pydict({"x": [1, 2, 3]})
    daft.attach_function(my_func, "my_func")
    bindings = {"test": df}
    df = daft.sql("select *, my_func(x) from test", **bindings)
    result = df.collect()
    result_dict = result.to_pydict()

    assert "x" in result_dict, f"Expected column 'x', got columns: {list(result_dict.keys())}"
    assert "my_func" in result_dict, f"Expected column 'my_func', got columns: {list(result_dict.keys())}"
    assert result_dict["x"] == [1, 2, 3]
    assert result_dict["my_func"] == ["my: 1", "my: 2", "my: 3"]


def test_udf_no_column_name_conflict_with_star():
    """Select *, udf(col) should not produce duplicate column names."""

    @daft.func(return_dtype=DataType.int64())
    def double_value(a) -> int:
        return a * 2

    daft.attach_function(double_value, "double_value")
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    bindings = {"test": df}
    df = daft.sql("select *, double_value(a) from test", **bindings)

    result_dict = df.to_pydict()
    assert "a" in result_dict
    assert "b" in result_dict
    assert "double_value" in result_dict
    assert result_dict["a"] == [1, 2, 3]
    assert result_dict["b"] == [4, 5, 6]
    assert result_dict["double_value"] == [2, 4, 6]


def test_sql_udf_explicit_alias_overrides_function_name():
    """SQL AS alias should take precedence over the auto-generated function name alias."""

    @daft.func(return_dtype=DataType.int64())
    def add_ten(val: int) -> int:
        return val + 10

    df = daft.from_pydict({"x": [1, 2, 3]})
    daft.attach_function(add_ten, "add_ten")
    result = daft.sql("select add_ten(x) as plus_ten from test", test=df).collect()
    result_dict = result.to_pydict()

    assert "plus_ten" in result_dict, f"Expected column 'plus_ten', got columns: {list(result_dict.keys())}"
    assert result_dict["plus_ten"] == [11, 12, 13]


def test_udf_result_column_uses_func_name_without_star():
    """SELECT udf(col) FROM t should produce a column named after the UDF, not the argument."""

    @daft.func(return_dtype=DataType.string())
    def my_func(col: int) -> str:
        return f"my: {col}"

    df = daft.from_pydict({"x": [1, 2, 3]})
    daft.attach_function(my_func, "my_func")
    result_dict = daft.sql("select my_func(x) from test", test=df).collect().to_pydict()
    assert list(result_dict.keys()) == ["my_func"], f"Got: {list(result_dict.keys())}"
    assert result_dict["my_func"] == ["my: 1", "my: 2", "my: 3"]
