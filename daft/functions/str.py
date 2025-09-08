"""String Functions."""

from __future__ import annotations

from typing import Any, Literal

from daft.daft import IOConfig, list_lit, sql_datatype
from daft.datatype import DataType, DataTypeLike
from daft.expressions import Expression, col, lit
from daft.series import item_to_series


def deserialize(expr: Expression, format: Literal["json"], dtype: DataTypeLike) -> Expression:
    """Deserializes a string using the specified format and data type.

    Args:
        expr: The expression to deserialize.
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


def try_deserialize(expr: Expression, format: Literal["json"], dtype: DataTypeLike) -> Expression:
    """Deserializes a string using the specified format and data type, inserting nulls on failures.

    Args:
        expr: The expression to deserialize.
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


def serialize(expr: Expression, format: Literal["json"]) -> Expression:
    """Serializes a value to a string using the specified format.

    Args:
        expr: The expression to serialize.
        format (Literal["json"]): The serialization format.

    Returns:
        Expression: A new expression with the serialized string.
    """
    return Expression._call_builtin_scalar_fn("serialize", expr, format=format)


def jq(expr: Expression, filter: str) -> Expression:
    """Applies a [jq](https://jqlang.github.io/jq/manual/) filter to a string, returning the results as a string.

    Args:
        expr: The expression to apply the jq filter to.
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


def contains(expr: Expression, substr: str | Expression) -> Expression:
    """Checks whether each string contains the given pattern in a string column.

    Args:
        expr: The expression to check.
        pattern: pattern to search for as a literal string, or as a column to pick values from

    Returns:
        Expression: a Boolean expression indicating whether each value contains the provided pattern

    Examples:
        >>> import daft
        >>> from daft.functions import contains
        >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"]})
        >>> df = df.select(contains(df["x"], "o"))
        >>> df.show()
        ╭─────────╮
        │ x       │
        │ ---     │
        │ Boolean │
        ╞═════════╡
        │ true    │
        ├╌╌╌╌╌╌╌╌╌┤
        │ false   │
        ├╌╌╌╌╌╌╌╌╌┤
        │ false   │
        ╰─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("utf8_contains", expr, substr)


def split(expr: Expression, pattern: str | Expression, regex: bool = False) -> Expression:
    r"""Splits each string on the given literal or regex pattern, into a list of strings.

    Args:
        expr: The expression to split.
        pattern: The pattern on which each string should be split, or a column to pick such patterns from.
        regex: Whether the pattern is a regular expression. Defaults to False.

    Returns:
        Expression: A List[Utf8] expression containing the string splits for each string in the column.

    Examples:
        >>> import daft
        >>> from daft.functions import split
        >>> df = daft.from_pydict({"data": ["daft.distributed.query", "a.b.c", "1.2.3"]})
        >>> df.with_column("split", split(df["data"], ".")).collect()
        ╭────────────────────────┬────────────────────────────╮
        │ data                   ┆ split                      │
        │ ---                    ┆ ---                        │
        │ Utf8                   ┆ List[Utf8]                 │
        ╞════════════════════════╪════════════════════════════╡
        │ daft.distributed.query ┆ [daft, distributed, query] │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ a.b.c                  ┆ [a, b, c]                  │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1.2.3                  ┆ [1, 2, 3]                  │
        ╰────────────────────────┴────────────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        Split on a regex pattern

        >>> df = daft.from_pydict({"data": ["daft.distributed...query", "a.....b.c", "1.2...3.."]})
        >>> df.with_column("split", split(df["data"], r"\.+", regex=True)).collect()
        ╭──────────────────────────┬────────────────────────────╮
        │ data                     ┆ split                      │
        │ ---                      ┆ ---                        │
        │ Utf8                     ┆ List[Utf8]                 │
        ╞══════════════════════════╪════════════════════════════╡
        │ daft.distributed...query ┆ [daft, distributed, query] │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ a.....b.c                ┆ [a, b, c]                  │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1.2...3..                ┆ [1, 2, 3, ]                │
        ╰──────────────────────────┴────────────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)


    """
    f_name = "regexp_split" if regex else "split"
    return Expression._call_builtin_scalar_fn(f_name, expr, pattern)


def lower(expr: Expression) -> Expression:
    """Convert UTF-8 string to all lowercase.

    Returns:
        Expression: a String expression which is `self` lowercased

    Examples:
        >>> import daft
        >>> from daft.functions import lower
        >>> df = daft.from_pydict({"x": ["FOO", "BAR", "BAZ"]})
        >>> df = df.select(lower(df["x"]))
        >>> df.show()
        ╭──────╮
        │ x    │
        │ ---  │
        │ Utf8 │
        ╞══════╡
        │ foo  │
        ├╌╌╌╌╌╌┤
        │ bar  │
        ├╌╌╌╌╌╌┤
        │ baz  │
        ╰──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("lower", expr)


def upper(expr: Expression) -> Expression:
    """Convert UTF-8 string to all upper.

    Returns:
        Expression: a String expression which is `self` uppercased

    Examples:
        >>> import daft
        >>> from daft.functions import upper
        >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"]})
        >>> df = df.select(upper(df["x"]))
        >>> df.show()
        ╭──────╮
        │ x    │
        │ ---  │
        │ Utf8 │
        ╞══════╡
        │ FOO  │
        ├╌╌╌╌╌╌┤
        │ BAR  │
        ├╌╌╌╌╌╌┤
        │ BAZ  │
        ╰──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("upper", expr)


def lstrip(expr: Expression) -> Expression:
    """Strip whitespace from the left side of a UTF-8 string.

    Returns:
        Expression: a String expression which is `self` with leading whitespace stripped

    Examples:
        >>> import daft
        >>> from daft.functions import lstrip
        >>> df = daft.from_pydict({"x": ["foo", "bar", "  baz"]})
        >>> df = df.select(lstrip(df["x"]))
        >>> df.show()
        ╭──────╮
        │ x    │
        │ ---  │
        │ Utf8 │
        ╞══════╡
        │ foo  │
        ├╌╌╌╌╌╌┤
        │ bar  │
        ├╌╌╌╌╌╌┤
        │ baz  │
        ╰──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("lstrip", expr)


def rstrip(expr: Expression) -> Expression:
    """Strip whitespace from the right side of a UTF-8 string.

    Returns:
        Expression: a String expression which is `self` with trailing whitespace stripped

    Examples:
        >>> import daft
        >>> from daft.functions import rstrip
        >>> df = daft.from_pydict({"x": ["foo", "bar", "baz   "]})
        >>> df = df.select(rstrip(df["x"]))
        >>> df.show()
        ╭──────╮
        │ x    │
        │ ---  │
        │ Utf8 │
        ╞══════╡
        │ foo  │
        ├╌╌╌╌╌╌┤
        │ bar  │
        ├╌╌╌╌╌╌┤
        │ baz  │
        ╰──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("rstrip", expr)


def reverse(expr: Expression) -> Expression:
    """Reverse a UTF-8 string.

    Returns:
        Expression: a String expression which is `self` reversed

    Examples:
        >>> import daft
        >>> from daft.functions import reverse
        >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"]})
        >>> df = df.select(reverse(df["x"]))
        >>> df.show()
        ╭──────╮
        │ x    │
        │ ---  │
        │ Utf8 │
        ╞══════╡
        │ oof  │
        ├╌╌╌╌╌╌┤
        │ rab  │
        ├╌╌╌╌╌╌┤
        │ zab  │
        ╰──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("reverse", expr)


def capitalize(expr: Expression) -> Expression:
    """Capitalize a UTF-8 string.

    Returns:
        Expression: a String expression which is `self` uppercased with the first character and lowercased the rest

    Examples:
        >>> import daft
        >>> from daft.functions import capitalize
        >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"]})
        >>> df = df.select(capitalize(df["x"]))
        >>> df.show()
        ╭──────╮
        │ x    │
        │ ---  │
        │ Utf8 │
        ╞══════╡
        │ Foo  │
        ├╌╌╌╌╌╌┤
        │ Bar  │
        ├╌╌╌╌╌╌┤
        │ Baz  │
        ╰──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("capitalize", expr)


def left(expr: Expression, nchars: int | Expression) -> Expression:
    """Gets the n (from nchars) left-most characters of each string.

    Returns:
        Expression: a String expression which is the `n` left-most characters of `self`

    Examples:
        >>> import daft
        >>> from daft.functions import left
        >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
        >>> df = df.select(left(df["x"], 4))
        >>> df.show()
        ╭──────╮
        │ x    │
        │ ---  │
        │ Utf8 │
        ╞══════╡
        │ daft │
        ├╌╌╌╌╌╌┤
        │ quer │
        ├╌╌╌╌╌╌┤
        │ engi │
        ╰──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("left", expr, nchars)


def right(expr: Expression, nchars: int | Expression) -> Expression:
    """Gets the n (from nchars) right-most characters of each string.

    Returns:
        Expression: a String expression which is the `n` right-most characters of `self`

    Examples:
        >>> import daft
        >>> from daft.functions import right
        >>> df = daft.from_pydict({"x": ["daft", "distributed", "engine"]})
        >>> df = df.select(right(df["x"], 4))
        >>> df.show()
        ╭──────╮
        │ x    │
        │ ---  │
        │ Utf8 │
        ╞══════╡
        │ daft │
        ├╌╌╌╌╌╌┤
        │ uted │
        ├╌╌╌╌╌╌┤
        │ gine │
        ╰──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("right", expr, nchars)


def rpad(expr: Expression, length: int | Expression, pad: str | Expression) -> Expression:
    """Right-pads each string by truncating or padding with the character.

    Returns:
        Expression: a String expression which is `self` truncated or right-padded with the pad character

    Note:
        If the string is longer than the specified length, it will be truncated.
        The pad character must be a single character.

    Examples:
        >>> import daft
        >>> from daft.functions import rpad
        >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
        >>> df = df.select(rpad(df["x"], 6, "0"))
        >>> df.show()
        ╭────────╮
        │ x      │
        │ ---    │
        │ Utf8   │
        ╞════════╡
        │ daft00 │
        ├╌╌╌╌╌╌╌╌┤
        │ query0 │
        ├╌╌╌╌╌╌╌╌┤
        │ engine │
        ╰────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("rpad", expr, length, pad)


def lpad(expr: Expression, length: int | Expression, pad: str | Expression) -> Expression:
    """Left-pads each string by truncating on the right or padding with the character.

    Returns:
        Expression: a String expression which is `self` truncated or left-padded with the pad character

    Note:
        If the string is longer than the specified length, it will be truncated on the right.
        The pad character must be a single character.

    Examples:
        >>> import daft
        >>> from daft.functions import lpad
        >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
        >>> df = df.select(lpad(df["x"], 6, "0"))
        >>> df.show()
        ╭────────╮
        │ x      │
        │ ---    │
        │ Utf8   │
        ╞════════╡
        │ 00daft │
        ├╌╌╌╌╌╌╌╌┤
        │ 0query │
        ├╌╌╌╌╌╌╌╌┤
        │ engine │
        ╰────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("lpad", expr, length, pad)


def repeat(expr: Expression, n: int | Expression) -> Expression:
    """Repeats each string n times.

    Returns:
        Expression: a String expression which is `self` repeated `n` times

    Examples:
        >>> import daft
        >>> from daft.functions import repeat
        >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
        >>> df = df.select(repeat(df["x"], 5))
        >>> df.show()
        ╭────────────────────────────────╮
        │ x                              │
        │ ---                            │
        │ Utf8                           │
        ╞════════════════════════════════╡
        │ daftdaftdaftdaftdaft           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ queryqueryqueryqueryquery      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ engineengineengineengineengin… │
        ╰────────────────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("repeat", expr, n)


def like(expr: Expression, pattern: str | Expression) -> Expression:
    """Checks whether each string matches the given SQL LIKE pattern, case sensitive.

    Returns:
        Expression: a Boolean expression indicating whether each value matches the provided pattern

    Note:
        Use % as a multiple-character wildcard or _ as a single-character wildcard.

    Examples:
        >>> import daft
        >>> from daft.functions import like
        >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
        >>> df = df.select(like(df["x"], "daf%"))
        >>> df.show()
        ╭─────────╮
        │ x       │
        │ ---     │
        │ Boolean │
        ╞═════════╡
        │ true    │
        ├╌╌╌╌╌╌╌╌╌┤
        │ false   │
        ├╌╌╌╌╌╌╌╌╌┤
        │ false   │
        ╰─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("like", expr, pattern)


def ilike(expr: Expression, pattern: str | Expression) -> Expression:
    """Checks whether each string matches the given SQL ILIKE pattern, case insensitive.

    Returns:
        Expression: a Boolean expression indicating whether each value matches the provided pattern

    Note:
        Use % as a multiple-character wildcard or _ as a single-character wildcard.

    Examples:
        >>> import daft
        >>> from daft.functions import ilike
        >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
        >>> df = df.select(ilike(df["x"], "%ft%"))
        >>> df.show()
        ╭─────────╮
        │ x       │
        │ ---     │
        │ Boolean │
        ╞═════════╡
        │ true    │
        ├╌╌╌╌╌╌╌╌╌┤
        │ false   │
        ├╌╌╌╌╌╌╌╌╌┤
        │ false   │
        ╰─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("ilike", expr, pattern)


def substr(expr: Expression, start: int | Expression, length: int | Expression | None = None) -> Expression:
    """Extract a substring from a string, starting at a specified index and extending for a given length.

    Returns:
        Expression: A String expression representing the extracted substring.

    Note:
        If `length` is not provided, the substring will include all characters from `start` to the end of the string.

    Examples:
        >>> import daft
        >>> from daft.functions import substr
        >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
        >>> df = df.select(substr(df["x"], 2, 4))
        >>> df.show()
        ╭──────╮
        │ x    │
        │ ---  │
        │ Utf8 │
        ╞══════╡
        │ ft   │
        ├╌╌╌╌╌╌┤
        │ ery  │
        ├╌╌╌╌╌╌┤
        │ gine │
        ╰──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("substr", expr, start, length)


def endswith(expr: Expression, suffix: str | Expression) -> Expression:
    """Checks whether each string ends with the given pattern in a string column.

    Args:
        expr: The expression to check.
        pattern: pattern to search for as a literal string, or as a column to pick values from

    Returns:
        Expression: a Boolean expression indicating whether each value ends with the provided pattern

    Examples:
        >>> import daft
        >>> from daft.functions import endswith
        >>> df = daft.from_pydict({"x": ["geftdaft", "lazy", "daft.io"]})
        >>> df.with_column("match", endswith(df["x"], "daft")).collect()
        ╭──────────┬─────────╮
        │ x        ┆ match   │
        │ ---      ┆ ---     │
        │ Utf8     ┆ Boolean │
        ╞══════════╪═════════╡
        │ geftdaft ┆ true    │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ lazy     ┆ false   │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ daft.io  ┆ false   │
        ╰──────────┴─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("ends_with", expr, suffix)


def startswith(expr: Expression, prefix: str | Expression) -> Expression:
    """Checks whether each string starts with the given pattern in a string column.

    Args:
        expr: The expression to check.
        pattern: pattern to search for as a literal string, or as a column to pick values from

    Returns:
        Expression: a Boolean expression indicating whether each value starts with the provided pattern

    Examples:
        >>> import daft
        >>> from daft.functions import startswith
        >>> df = daft.from_pydict({"x": ["geftdaft", "lazy", "daft.io"]})
        >>> df.with_column("match", startswith(df["x"], "daft")).collect()
        ╭──────────┬─────────╮
        │ x        ┆ match   │
        │ ---      ┆ ---     │
        │ Utf8     ┆ Boolean │
        ╞══════════╪═════════╡
        │ geftdaft ┆ false   │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ lazy     ┆ false   │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ daft.io  ┆ true    │
        ╰──────────┴─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("starts_with", expr, prefix)


def normalize(
    expr: Expression,
    *,
    remove_punct: bool = False,
    lowercase: bool = False,
    nfd_unicode: bool = False,
    white_space: bool = False,
) -> Expression:
    r"""Normalizes a string for more useful deduplication.

    Args:
        expr: The expression to normalize.
        remove_punct: Whether to remove all punctuation (ASCII).
        lowercase: Whether to convert the string to lowercase.
        nfd_unicode: Whether to normalize and decompose Unicode characters according to NFD.
        white_space: Whether to normalize whitespace, replacing newlines etc with spaces and removing double spaces.

    Returns:
        Expression: a String expression which is normalized.

    Note:
        All processing options are off by default.

    Examples:
        >>> import daft
        >>> from daft.functions import normalize
        >>> df = daft.from_pydict({"x": ["hello world", "Hello, world!", "HELLO,   \nWORLD!!!!"]})
        >>> df = df.with_column("normalized", normalize(df["x"], remove_punct=True, lowercase=True, white_space=True))
        >>> df.show()
        ╭───────────────┬─────────────╮
        │ x             ┆ normalized  │
        │ ---           ┆ ---         │
        │ Utf8          ┆ Utf8        │
        ╞═══════════════╪═════════════╡
        │ hello world   ┆ hello world │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ Hello, world! ┆ hello world │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ HELLO,        ┆ hello world │
        │ WORLD!!!!     ┆             │
        ╰───────────────┴─────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn(
        "normalize",
        expr,
        remove_punct=remove_punct,
        lowercase=lowercase,
        nfd_unicode=nfd_unicode,
        white_space=white_space,
    )


def tokenize_encode(
    expr: Expression,
    tokens_path: str,
    *,
    io_config: IOConfig | None = None,
    pattern: str | None = None,
    special_tokens: str | None = None,
    use_special_tokens: bool | None = None,
) -> Expression:
    """Encodes each string as a list of integer tokens using a tokenizer.

    Uses https://github.com/openai/tiktoken for tokenization.

    Supported built-in tokenizers: `cl100k_base`, `o200k_base`, `p50k_base`, `p50k_edit`, `r50k_base`. Also supports
    loading tokens from a file in tiktoken format.

    Args:
        expr: The expression to encode.
        tokens_path: The name of a built-in tokenizer, or the path to a token file (supports downloading).
        io_config (optional): IOConfig to use when accessing remote storage.
        pattern (optional): Regex pattern to use to split strings in tokenization step. Necessary if loading from a file.
        special_tokens (optional): Name of the set of special tokens to use. Currently only "llama3" supported. Necessary if loading from a file.
        use_special_tokens (optional): Whether or not to parse special tokens included in input. Disabled by default. Automatically enabled if `special_tokens` is provided.

    Returns:
        Expression: An expression with the encodings of the strings as lists of unsigned 32-bit integers.

    Note:
        If using this expression with Llama 3 tokens, note that Llama 3 does some extra preprocessing on
        strings in certain edge cases. This may result in slightly different encodings in these cases.

    """
    return Expression._call_builtin_scalar_fn(
        "tokenize_encode",
        expr,
        tokens_path=tokens_path,
        io_config=io_config,
        pattern=pattern,
        special_tokens=special_tokens,
        use_special_tokens=use_special_tokens,
    )


def tokenize_decode(
    expr: Expression,
    tokens_path: str,
    *,
    io_config: IOConfig | None = None,
    pattern: str | None = None,
    special_tokens: str | None = None,
) -> Expression:
    """Decodes each list of integer tokens into a string using a tokenizer.

    Uses [https://github.com/openai/tiktoken](https://github.com/openai/tiktoken) for tokenization.

    Supported built-in tokenizers: `cl100k_base`, `o200k_base`, `p50k_base`, `p50k_edit`, `r50k_base`. Also supports
    loading tokens from a file in tiktoken format.

    Args:
        expr: The expression to decode.
        tokens_path: The name of a built-in tokenizer, or the path to a token file (supports downloading).
        io_config (optional): IOConfig to use when accessing remote storage.
        pattern (optional): Regex pattern to use to split strings in tokenization step. Necessary if loading from a file.
        special_tokens (optional): Name of the set of special tokens to use. Currently only "llama3" supported. Necessary if loading from a file.

    Returns:
        Expression: An expression with decoded strings.
    """
    return Expression._call_builtin_scalar_fn(
        "tokenize_decode",
        expr,
        tokens_path=tokens_path,
        io_config=io_config,
        pattern=pattern,
        special_tokens=special_tokens,
    )


def count_matches(
    expr: Expression,
    patterns: Any,
    *,
    whole_words: bool = False,
    case_sensitive: bool = True,
) -> Expression:
    """Counts the number of times a pattern, or multiple patterns, appear in a string.

    If whole_words is true, then matches are only counted if they are whole words. This
    also applies to multi-word strings. For example, on the string "abc def", the strings
    "def" and "abc def" would be matched, but "bc de", "abc d", and "abc " (with the space)
    would not.

    If case_sensitive is false, then case will be ignored. This only applies to ASCII
    characters; unicode uppercase/lowercase will still be considered distinct.

    Args:
        expr: The expression to check.
        patterns: A pattern or a list of patterns.
        whole_words: Whether to only match whole word(s). Defaults to false.
        case_sensitive: Whether the matching should be case sensitive. Defaults to true.

    Note:
        If a pattern is a substring of another pattern, the longest pattern is matched first.
        For example, in the string "hello world", with patterns "hello", "world", and "hello world",
        one match is counted for "hello world".
    """
    if isinstance(patterns, str):
        patterns = [patterns]
    if not isinstance(patterns, Expression):
        series = item_to_series("items", patterns)
        patterns = Expression._from_pyexpr(list_lit(series._series))

    return Expression._call_builtin_scalar_fn(
        "count_matches", expr, patterns, whole_words=whole_words, case_sensitive=case_sensitive
    )


def length_bytes(expr: Expression) -> Expression:
    """Retrieves the length for a UTF-8 string column in bytes.

    Returns:
        Expression: an UInt64 expression with the length of each string

    Examples:
        >>> import daft
        >>> from daft.functions import length_bytes
        >>> df = daft.from_pydict({"x": ["😉test", "hey̆", "baz"]})
        >>> df = df.select(length_bytes(df["x"]))
        >>> df.show()
        ╭────────╮
        │ x      │
        │ ---    │
        │ UInt64 │
        ╞════════╡
        │ 8      │
        ├╌╌╌╌╌╌╌╌┤
        │ 5      │
        ├╌╌╌╌╌╌╌╌┤
        │ 3      │
        ╰────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("length_bytes", expr)
