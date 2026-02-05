"""Distance functions for vector inputs."""

from __future__ import annotations

from daft.expressions import Expression


def cosine_distance(left: Expression, right: Expression) -> Expression:
    """Compute the cosine distance between two embeddings.

    Args:
        left (FixedSizeList or Embedding Expression): The left vector
        right (FixedSizeList or Embedding Expression): The right vector

    Returns:
        Expression (Float64 Expression): an expression with the cosine distance between the two vectors.

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
    return Expression._call_builtin_scalar_fn("cosine_distance", left, right)


def dot_product(left: Expression, right: Expression) -> Expression:
    """Compute the dot product between two embeddings.

    Args:
        left (FixedSizeList or Embedding Expression): The left vector
        right (FixedSizeList or Embedding Expression): The right vector

    Returns:
        Expression (Float64 Expression): the dot product between the two vectors.
    """
    return Expression._call_builtin_scalar_fn("dot_product", left, right)


def euclidean_distance(left: Expression, right: Expression) -> Expression:
    """Compute the Euclidean distance between two embeddings.

    Args:
        left (FixedSizeList or Embedding Expression): The left vector
        right (FixedSizeList or Embedding Expression): The right vector

    Returns:
        Expression (Float64 Expression): the Euclidean distance between the two vectors.
    """
    return Expression._call_builtin_scalar_fn("euclidean_distance", left, right)
