from __future__ import annotations

import daft.daft as native
from daft.expressions import Expression


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
