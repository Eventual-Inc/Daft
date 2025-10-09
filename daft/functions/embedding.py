"""Embedding Functions."""

from __future__ import annotations

from daft.expressions import Expression


def cosine_distance(left_embedding: Expression, right_embedding: Expression) -> Expression:
    """Compute the cosine distance between two embeddings.

    Args:
        left_embedding (FixedSizeList or Embedding Expression): The left embedding
        right_embedding (FixedSizeList or Embedding Expression): The right embedding

    Returns:
        Expression (Float64 Expression): an expression with the cosine distance between the two embeddings.

    Examples:
        >>> import daft
        >>> from daft.functions import cosine_distance
        >>>
        >>> df = daft.from_pydict({"e1": [[1, 2, 3], [1, 2, 3]], "e2": [[1, 2, 3], [-1, -2, -3]]})
        >>> dtype = daft.DataType.fixed_size_list(daft.DataType.float32(), 3)
        >>> df = df.with_column("dist", cosine_distance(df["e1"].cast(dtype), df["e2"].cast(dtype)))
        >>> df.show()
        ╭─────────────┬──────────────┬─────────╮
        │ e1          ┆ e2           ┆ dist    │
        │ ---         ┆ ---          ┆ ---     │
        │ List[Int64] ┆ List[Int64]  ┆ Float64 │
        ╞═════════════╪══════════════╪═════════╡
        │ [1, 2, 3]   ┆ [1, 2, 3]    ┆ 0       │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ [1, 2, 3]   ┆ [-1, -2, -3] ┆ 2       │
        ╰─────────────┴──────────────┴─────────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)

    """
    return Expression._call_builtin_scalar_fn("cosine_distance", left_embedding, right_embedding)
