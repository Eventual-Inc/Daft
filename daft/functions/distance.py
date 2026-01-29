"""Distance functions for vector inputs."""

from __future__ import annotations

from daft.expressions import Expression


def cosine_distance(left: Expression, right: Expression) -> Expression:
    """Compute the cosine distance between two embeddings.

    Args:
        left (FixedSizeList or Embedding Expression): The left vector
        right (FixedSizeList or Embedding Expression): The right vector

    Returns:
        Expression (Float64 Expression): the cosine distance between the two vectors.
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
