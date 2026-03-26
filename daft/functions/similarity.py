"""Similarity functions for vector inputs."""

from __future__ import annotations

from daft.expressions import Expression


def cosine_similarity(left: Expression, right: Expression) -> Expression:
    """Compute the cosine similarity between two embeddings.

    Args:
        left (FixedSizeList or Embedding Expression): The left vector
        right (FixedSizeList or Embedding Expression): The right vector

    Returns:
        Expression (Float64 Expression): the cosine similarity between the two vectors.
    """
    return Expression._call_builtin_scalar_fn("cosine_similarity", left, right)


def pearson_correlation(left: Expression, right: Expression) -> Expression:
    """Compute the Pearson correlation between two embeddings.

    Args:
        left (FixedSizeList or Embedding Expression): The left vector
        right (FixedSizeList or Embedding Expression): The right vector

    Returns:
        Expression (Float64 Expression): the Pearson correlation between the two vectors.
    """
    return Expression._call_builtin_scalar_fn("pearson_correlation", left, right)


def jaccard_similarity(left: Expression, right: Expression) -> Expression:
    """Compute the Jaccard similarity between two embeddings.

    The Jaccard similarity is computed by treating non-zero elements as set
    membership and comparing intersection over union.

    Args:
        left (FixedSizeList or Embedding Expression): The left vector
        right (FixedSizeList or Embedding Expression): The right vector

    Returns:
        Expression (Float64 Expression): the Jaccard similarity between the two vectors.
    """
    return Expression._call_builtin_scalar_fn("jaccard_similarity", left, right)
