from __future__ import annotations

import daft.daft as native
from daft.expressions import Expression, col, lit


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
