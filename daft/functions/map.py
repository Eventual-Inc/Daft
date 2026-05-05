"""Map Functions."""

from __future__ import annotations

from daft.expressions import Expression


def to_map(*pairs: tuple[Expression, Expression]) -> Expression:
    """Constructs a map column from key-value expression pairs.

    Args:
        pairs: Tuples of (key, value) expressions. All keys must share the same type
            and all values must share the same type.

    Returns:
        An expression for a map column.

    Examples:
        >>> import daft
        >>> from daft.functions import to_map
        >>>
        >>> df = daft.from_pydict({"k1": ["a", "b"], "v1": [1, 2], "k2": ["c", "d"], "v2": [3, 4]})
        >>> df.select(to_map((df["k1"], df["v1"]), (df["k2"], df["v2"])).alias("my_map")).show()
        ╭──────────────────────────────────╮
        │ my_map                           │
        │ ---                              │
        │ Map[String, Int64]               │
        ╞══════════════════════════════════╡
        │ [{key: a, value: 1},             │
        │ {key: c, value: 3}]              │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [{key: b, value: 2},             │
        │ {key: d, value: 4}]              │
        ╰──────────────────────────────────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)
    """
    if not pairs:
        raise ValueError("to_map requires at least one (key, value) pair")

    flat_args = []
    for pair in pairs:
        if not isinstance(pair, tuple) or len(pair) != 2:
            raise ValueError("Each argument to to_map must be a tuple of (key, value) expressions")
        key, value = pair
        flat_args.append(Expression._to_expression(key))
        flat_args.append(Expression._to_expression(value))

    return Expression._call_builtin_scalar_fn("map", *flat_args)
