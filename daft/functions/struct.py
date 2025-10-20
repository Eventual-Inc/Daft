"""Struct Functions."""

from __future__ import annotations

from daft.expressions import Expression


def unnest(expr: Expression) -> Expression:
    """Flatten the fields of a struct expression into columns in a DataFrame.

    Examples:
        >>> import daft
        >>> df = daft.from_pydict(
        ...     {
        ...         "struct": [
        ...             {"x": 1, "y": 2},
        ...             {"x": 3, "y": 4},
        ...         ]
        ...     }
        ... )
        >>> unnested_df = df.select(unnest(df["struct"]))
        >>> unnested_df.show()
        ╭───────┬───────╮
        │ x     ┆ y     │
        │ ---   ┆ ---   │
        │ Int64 ┆ Int64 │
        ╞═══════╪═══════╡
        │ 1     ┆ 2     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 4     │
        ╰───────┴───────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)
    """
    return expr.get("*")


def to_struct(*fields: Expression, **named_fields: Expression) -> Expression:
    """Constructs a struct from the input expressions.

    Args:
        fields: Expressions to be set as struct fields, using the expression name as the field name.
        named_fields: Expressions to be set as struct fields, using the keyword arg as the field name.

    Returns:
        An expression for a struct column with the input columns as its fields.

    Examples:
        >>> import daft
        >>> from daft.functions import to_struct
        >>>
        >>> df = daft.from_pydict({"a": ["a", "b", "c"], "b": [1, 2, 3]})
        >>> df.select(to_struct(df["a"], b2=df["b"] * 2)).show()
        ╭──────────────────────────────╮
        │ struct                       │
        │ ---                          │
        │ Struct[a: String, b2: Int64] │
        ╞══════════════════════════════╡
        │ {a: a,                       │
        │ b2: 2,                       │
        │ }                            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ {a: b,                       │
        │ b2: 4,                       │
        │ }                            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ {a: c,                       │
        │ b2: 6,                       │
        │ }                            │
        ╰──────────────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    all_fields = list(fields) + [Expression._to_expression(field).alias(name) for name, field in named_fields.items()]
    return Expression._call_builtin_scalar_fn("struct", *all_fields)
