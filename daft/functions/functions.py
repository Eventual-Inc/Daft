from __future__ import annotations

import daft.daft as native
from daft.expressions import Expression, col, list_, lit


def monotonically_increasing_id() -> Expression:
    """Generates a column of monotonically increasing unique ids.

    The implementation puts the partition number in the upper 28 bits, and the row number in each partition
    in the lower 36 bits. This allows for 2^28 ≈ 268 million partitions and 2^36 ≈ 68 billion rows per partition.

    Returns:
        Expression: An expression that generates monotonically increasing IDs

    Examples:
        >>> import daft
        >>> from daft.functions import monotonically_increasing_id
        >>> daft.context.set_runner_ray()  # doctest: +SKIP
        >>>
        >>> df = daft.from_pydict({"a": [1, 2, 3, 4]}).into_partitions(2)
        >>> df = df.with_column("id", monotonically_increasing_id())
        >>> df.show()  # doctest: +SKIP
        ╭───────┬─────────────╮
        │ a     ┆ id          │
        │ ---   ┆ ---         │
        │ Int64 ┆ UInt64      │
        ╞═══════╪═════════════╡
        │ 1     ┆ 0           │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2     ┆ 1           │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 3     ┆ 68719476736 │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 4     ┆ 68719476737 │
        ╰───────┴─────────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)

    """
    f = native.get_function_from_registry("monotonically_increasing_id")
    return Expression._from_pyexpr(f())


def columns_sum(*exprs: Expression | str) -> Expression:
    """Sum values across columns.

    Args:
        exprs: The columns to sum.

    Examples:
        >>> import daft
        >>> from daft.functions import columns_sum
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("sum", columns_sum("a", "b"))
        >>> df.show()
        ╭───────┬───────┬───────╮
        │ a     ┆ b     ┆ sum   │
        │ ---   ┆ ---   ┆ ---   │
        │ Int64 ┆ Int64 ┆ Int64 │
        ╞═══════╪═══════╪═══════╡
        │ 1     ┆ 4     ┆ 5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ 5     ┆ 7     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 6     ┆ 9     │
        ╰───────┴───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    if not exprs:
        raise ValueError("columns_sum requires at least one expression")
    return list_(*exprs).list.sum().alias("columns_sum")


def columns_mean(*exprs: Expression | str) -> Expression:
    """Average values across columns. Akin to `columns_avg`.

    Args:
        exprs: The columns to average.

    Examples:
        >>> import daft
        >>> from daft.functions import columns_mean
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("mean", columns_mean("a", "b"))
        >>> df.show()
        ╭───────┬───────┬─────────╮
        │ a     ┆ b     ┆ mean    │
        │ ---   ┆ ---   ┆ ---     │
        │ Int64 ┆ Int64 ┆ Float64 │
        ╞═══════╪═══════╪═════════╡
        │ 1     ┆ 4     ┆ 2.5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 2     ┆ 5     ┆ 3.5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 3     ┆ 6     ┆ 4.5     │
        ╰───────┴───────┴─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    if not exprs:
        raise ValueError("columns_mean requires at least one expression")
    return list_(*exprs).list.mean().alias("columns_mean")


def columns_avg(*exprs: Expression | str) -> Expression:
    """Average values across columns. Akin to `columns_mean`.

    Args:
        exprs: The columns to average across.

    Examples:
        >>> import daft
        >>> from daft.functions import columns_avg
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("avg", columns_avg("a", "b"))
        >>> df.show()
        ╭───────┬───────┬─────────╮
        │ a     ┆ b     ┆ avg     │
        │ ---   ┆ ---   ┆ ---     │
        │ Int64 ┆ Int64 ┆ Float64 │
        ╞═══════╪═══════╪═════════╡
        │ 1     ┆ 4     ┆ 2.5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 2     ┆ 5     ┆ 3.5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 3     ┆ 6     ┆ 4.5     │
        ╰───────┴───────┴─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    if not exprs:
        raise ValueError("columns_avg requires at least one expression")
    return list_(*exprs).list.mean().alias("columns_avg")


def columns_min(*exprs: Expression | str) -> Expression:
    """Find the minimum value across columns.

    Args:
        exprs: The columns to find the minimum of.

    Examples:
        >>> import daft
        >>> from daft.functions import columns_min
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("min", columns_min("a", "b"))
        >>> df.show()
        ╭───────┬───────┬───────╮
        │ a     ┆ b     ┆ min   │
        │ ---   ┆ ---   ┆ ---   │
        │ Int64 ┆ Int64 ┆ Int64 │
        ╞═══════╪═══════╪═══════╡
        │ 1     ┆ 4     ┆ 1     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ 5     ┆ 2     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 6     ┆ 3     │
        ╰───────┴───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    if not exprs:
        raise ValueError("columns_min requires at least one expression")
    return list_(*exprs).list.min().alias("columns_min")


def columns_max(*exprs: Expression | str) -> Expression:
    """Find the maximum value across columns.

    Args:
        exprs: The columns to find the maximum of.

    Examples:
        >>> import daft
        >>> from daft.functions import columns_max
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("max", columns_max("a", "b"))
        >>> df.show()
        ╭───────┬───────┬───────╮
        │ a     ┆ b     ┆ max   │
        │ ---   ┆ ---   ┆ ---   │
        │ Int64 ┆ Int64 ┆ Int64 │
        ╞═══════╪═══════╪═══════╡
        │ 1     ┆ 4     ┆ 4     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ 5     ┆ 5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 6     ┆ 6     │
        ╰───────┴───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    if not exprs:
        raise ValueError("columns_max requires at least one expression")
    return list_(*exprs).list.max().alias("columns_max")


def row_number() -> Expression:
    """Return the row number of the current row (used for window functions).

    Examples:
        >>> import daft
        >>> from daft.window import Window
        >>> from daft.functions import row_number
        >>> df = daft.from_pydict({"category": ["A", "A", "A", "A", "B", "B", "B", "B"], "value": [1, 7, 2, 9, 1, 3, 3, 7]})
        >>>
        >>> # Ascending order
        >>> window = Window().partition_by("category").order_by("value")
        >>> df = df.with_column("row", row_number().over(window))
        >>> df = df.sort("category")
        >>> df.show()
        ╭──────────┬───────┬────────╮
        │ category ┆ value ┆ row    │
        │ ---      ┆ ---   ┆ ---    │
        │ Utf8     ┆ Int64 ┆ UInt64 │
        ╞══════════╪═══════╪════════╡
        │ A        ┆ 1     ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ A        ┆ 2     ┆ 2      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ A        ┆ 7     ┆ 3      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ A        ┆ 9     ┆ 4      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 1     ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 3     ┆ 2      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 3     ┆ 3      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 7     ┆ 4      │
        ╰──────────┴───────┴────────╯
        <BLANKLINE>
        (Showing first 8 rows)

    Returns:
        Expression: An expression that returns the row number of the current row.
    """
    return Expression._from_pyexpr(native.row_number())


def rank() -> Expression:
    """Return the rank of the current row (used for window functions).

    Examples:
        >>> import daft
        >>> from daft.window import Window
        >>> from daft.functions import rank
        >>> df = daft.from_pydict({"category": ["A", "A", "A", "A", "B", "B", "B", "B"], "value": [1, 3, 3, 7, 7, 7, 4, 4]})
        >>>
        >>> window = Window().partition_by("category").order_by("value", desc=True)
        >>> df = df.with_column("rank", rank().over(window))
        >>> df = df.sort("category")
        >>> df.show()
        ╭──────────┬───────┬────────╮
        │ category ┆ value ┆ rank   │
        │ ---      ┆ ---   ┆ ---    │
        │ Utf8     ┆ Int64 ┆ UInt64 │
        ╞══════════╪═══════╪════════╡
        │ A        ┆ 7     ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ A        ┆ 3     ┆ 2      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ A        ┆ 3     ┆ 2      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ A        ┆ 1     ┆ 4      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 7     ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 7     ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 4     ┆ 3      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 4     ┆ 3      │
        ╰──────────┴───────┴────────╯
        <BLANKLINE>
        (Showing first 8 rows)

    Returns:
        Expression: An expression that returns the rank of the current row.
    """
    return Expression._from_pyexpr(native.rank())


def dense_rank() -> Expression:
    """Return the dense rank of the current row (used for window functions).

    The dense rank is the rank of the current row without gaps.

    Examples:
        >>> import daft
        >>> from daft.window import Window
        >>> from daft.functions import dense_rank
        >>> df = daft.from_pydict({"category": ["A", "A", "A", "A", "B", "B", "B", "B"], "value": [1, 3, 3, 7, 7, 7, 4, 4]})
        >>>
        >>> window = Window().partition_by("category").order_by("value", desc=True)
        >>> df = df.with_column("dense_rank", dense_rank().over(window))
        >>> df = df.sort("category")
        >>> df.show()
        ╭──────────┬───────┬────────────╮
        │ category ┆ value ┆ dense_rank │
        │ ---      ┆ ---   ┆ ---        │
        │ Utf8     ┆ Int64 ┆ UInt64     │
        ╞══════════╪═══════╪════════════╡
        │ A        ┆ 7     ┆ 1          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ A        ┆ 3     ┆ 2          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ A        ┆ 3     ┆ 2          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ A        ┆ 1     ┆ 3          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 7     ┆ 1          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 7     ┆ 1          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 4     ┆ 2          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 4     ┆ 2          │
        ╰──────────┴───────┴────────────╯
        <BLANKLINE>
        (Showing first 8 rows)

    Returns:
        Expression: An expression that returns the dense rank of the current row.
    """
    return Expression._from_pyexpr(native.dense_rank())


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
