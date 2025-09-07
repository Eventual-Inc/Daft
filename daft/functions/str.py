"""String Functions."""

from __future__ import annotations

from typing import Any, Literal

from daft.daft import sql_datatype
from daft.datatype import DataType, DataTypeLike
from daft.expressions import Expression, col, lit


def deserialize(expr: Expression | str, format: Literal["json"], dtype: DataTypeLike) -> Expression:
    """Deserializes a string using the specified format and data type.

    Args:
        expr: The expression or string to deserialize.
        format (Literal["json"]): The serialization format.
        dtype: The target data type to deserialize into.

    Returns:
        Expression: A new expression with the deserialized value.
    """
    if isinstance(dtype, str):
        dtype = DataType._from_pydatatype(sql_datatype(dtype))
    else:
        assert isinstance(dtype, (DataType, type))
        dtype = DataType._infer_type(dtype)
    return Expression._call_builtin_scalar_fn("deserialize", expr, format=format, dtype=dtype._dtype)


def try_deserialize(expr: Expression | str, format: Literal["json"], dtype: DataTypeLike) -> Expression:
    """Deserializes a string using the specified format and data type, inserting nulls on failures.

    Args:
        expr: The expression or string to deserialize.
        format (Literal["json"]): The serialization format.
        dtype: The target data type to deserialize into.

    Returns:
        Expression: A new expression with the deserialized value (or null).
    """
    if isinstance(dtype, str):
        dtype = DataType._from_pydatatype(sql_datatype(dtype))
    else:
        assert isinstance(dtype, (DataType, type))
        dtype = DataType._infer_type(dtype)
    return Expression._call_builtin_scalar_fn("try_deserialize", expr, format=format, dtype=dtype._dtype)


def serialize(expr: Expression | Any, format: Literal["json"]) -> Expression:
    """Serializes a value to a string using the specified format.

    Args:
        expr: The expression or value to serialize.
        format (Literal["json"]): The serialization format.

    Returns:
        Expression: A new expression with the serialized string.
    """
    return Expression._call_builtin_scalar_fn("serialize", expr, format=format)


def jq(expr: Expression | str, filter: str) -> Expression:
    """Applies a [jq](https://jqlang.github.io/jq/manual/) filter to a string, returning the results as a string.

    Args:
        expr: The expression or string to apply the jq filter to.
        filter (str): The jq filter to apply.

    Returns:
        Expression: Expression representing the result of the jq filter as a column of JSON-compatible strings.

    Warning:
        This expression uses [jaq](https://github.com/01mf02/jaq) as its filter executor which can differ from the
        [jq](https://jqlang.org/) command-line tool. Please consult [jq vs. jaq](https://github.com/01mf02/jaq?tab=readme-ov-file#differences-between-jq-and-jaq)
        for a detailed look into possible differences.

    Examples:
        >>> import daft
        >>> from daft.functions import jq
        >>>
        >>> df = daft.from_pydict({"col": ['{"a": 1}', '{"a": 2}', '{"a": 3}']})
        >>> df.with_column("res", jq(df["col"], ".a")).collect()
        ╭──────────┬──────╮
        │ col      ┆ res  │
        │ ---      ┆ ---  │
        │ Utf8     ┆ Utf8 │
        ╞══════════╪══════╡
        │ {"a": 1} ┆ 1    │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        │ {"a": 2} ┆ 2    │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        │ {"a": 3} ┆ 3    │
        ╰──────────┴──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    return Expression._call_builtin_scalar_fn("jq", expr, filter=filter)


def format(f_string: str, *args: Expression | str) -> Expression:
    """Format a string using the given arguments.

    Args:
        f_string: The format string.
        *args: The arguments to format the string with.

    Returns:
        Expression: A string expression with the formatted result.

    Examples:
        >>> import daft
        >>> from daft.functions import format
        >>> from daft import col
        >>> df = daft.from_pydict({"first_name": ["Alice", "Bob"], "last_name": ["Smith", "Jones"]})
        >>> df = df.with_column("greeting", format("Hello {} {}", col("first_name"), "last_name"))
        >>> df.show()
        ╭────────────┬───────────┬───────────────────╮
        │ first_name ┆ last_name ┆ greeting          │
        │ ---        ┆ ---       ┆ ---               │
        │ Utf8       ┆ Utf8      ┆ Utf8              │
        ╞════════════╪═══════════╪═══════════════════╡
        │ Alice      ┆ Smith     ┆ Hello Alice Smith │
        ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ Bob        ┆ Jones     ┆ Hello Bob Jones   │
        ╰────────────┴───────────┴───────────────────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)
    """
    if f_string.count("{}") != len(args):
        raise ValueError(
            f"Format string {f_string} has {f_string.count('{}')} placeholders but {len(args)} arguments were provided"
        )

    parts = f_string.split("{}")
    exprs = []

    for part, arg in zip(parts, args):
        if part:
            exprs.append(lit(part))

        if isinstance(arg, str):
            exprs.append(col(arg))
        else:
            exprs.append(arg)

    if parts[-1]:
        exprs.append(lit(parts[-1]))

    if not exprs:
        return lit("")

    result = exprs[0]
    for expr in exprs[1:]:
        result = result + expr

    return result
