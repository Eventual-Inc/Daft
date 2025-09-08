"""List Functions."""

from __future__ import annotations

from daft.expressions import Expression


def value_counts(expr: Expression) -> Expression:
    """Counts the occurrences of each distinct value in the list.

    Returns:
        Expression: A Map<X, UInt64> expression where the keys are distinct elements from the
                    original list of type X, and the values are UInt64 counts representing
                    the number of times each element appears in the list.

    Note:
        This function does not work for nested types. For example, it will not produce a map
        with lists as keys.

    Examples:
        >>> import daft
        >>> from daft.functions import value_counts
        >>> df = daft.from_pydict({"letters": [["a", "b", "a"], ["b", "c", "b", "c"]]})
        >>> df.with_column("value_counts", value_counts(df["letters"])).collect()
        ╭──────────────┬───────────────────╮
        │ letters      ┆ value_counts      │
        │ ---          ┆ ---               │
        │ List[Utf8]   ┆ Map[Utf8: UInt64] │
        ╞══════════════╪═══════════════════╡
        │ [a, b, a]    ┆ [{key: a,         │
        │              ┆ value: 2,         │
        │              ┆ }, {key: …        │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [b, c, b, c] ┆ [{key: b,         │
        │              ┆ value: 2,         │
        │              ┆ }, {key: …        │
        ╰──────────────┴───────────────────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)
    """
    return Expression._call_builtin_scalar_fn("list_value_counts", expr)


def chunk(expr: Expression, size: int) -> Expression:
    """Splits each list into chunks of the given size.

    Args:
        expr: Expression to chunk
        size: size of chunks to split the list into. Must be greater than 0
    Returns:
        Expression: an expression with lists of fixed size lists of the type of the list values
    """
    return Expression._call_builtin_scalar_fn("list_chunk", expr, size)
