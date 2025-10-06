"""Embedding Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from daft.expressions import Expression

if TYPE_CHECKING:
    from daft.expressions import EmbeddingExpr, FloatExpr


def cosine_distance(left: EmbeddingExpr, right: EmbeddingExpr) -> FloatExpr:
    """Compute the cosine distance between two embeddings.

    Args:
        left: `Embedding` expression.
        right: `Embedding` expression.

    Returns:
        Expression: a `Float64` Expression with the cosine distance between the two embeddings.

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
