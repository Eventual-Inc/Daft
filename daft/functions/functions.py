from __future__ import annotations

import daft.daft as native
from daft.expressions import Expression, list_


def monotonically_increasing_id() -> Expression:
    """Generates a column of monotonically increasing unique ids.

    The implementation puts the partition number in the upper 28 bits, and the row number in each partition
    in the lower 36 bits. This allows for 2^28 ≈ 268 million partitions and 2^40 ≈ 68 billion rows per partition.

    Example:
        >>> import daft
        >>> from daft.functions import monotonically_increasing_id
        >>> df = daft.from_pydict({"a": [1, 2, 3, 4]}).into_partitions(2)
        >>> df = df.with_column("id", monotonically_increasing_id())
        >>> df.show()
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

    Returns:
        Expression: An expression that generates monotonically increasing IDs
    """
    return Expression._from_pyexpr(native.monotonically_increasing_id())


def sum_horizontal(*exprs: Expression | str) -> Expression:
    """Sum values horizontally across columns.

    Args:
        exprs: The columns to sum.

    Example:
        >>> import daft
        >>> from daft.functions import sum_horizontal
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("sum", sum_horizontal("a", "b"))
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
        raise ValueError("sum_horizontal requires at least one expression")
    return list_(*exprs).list.sum()


def mean_horizontal(*exprs: Expression | str) -> Expression:
    """Average values horizontally across columns.

    Args:
        exprs: The columns to average.

    Example:
        >>> import daft
        >>> from daft.functions import mean_horizontal
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("mean", mean_horizontal("a", "b"))
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
        raise ValueError("mean_horizontal requires at least one expression")
    return list_(*exprs).list.mean()


def min_horizontal(*exprs: Expression | str) -> Expression:
    """Find the minimum value horizontally across columns.

    Args:
        exprs: The columns to find the minimum of.

    Example:
        >>> import daft
        >>> from daft.functions import min_horizontal
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("min", min_horizontal("a", "b"))
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
        raise ValueError("min_horizontal requires at least one expression")
    return list_(*exprs).list.min()


def max_horizontal(*exprs: Expression | str) -> Expression:
    """Find the maximum value horizontally across columns.

    Args:
        exprs: The columns to find the maximum of.

    Example:
        >>> import daft
        >>> from daft.functions import max_horizontal
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("max", max_horizontal("a", "b"))
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
        raise ValueError("max_horizontal requires at least one expression")
    return list_(*exprs).list.max()
