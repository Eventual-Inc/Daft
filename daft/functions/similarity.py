"""Similarity functions for vector inputs."""

from __future__ import annotations

from daft.expressions import Expression


def hamming_distance(left: Expression, right: Expression) -> Expression:
    """Compute the Hamming distance (number of differing bits) between two hash fingerprints.

    Counts the number of differing bits (popcount of XOR).
    Supports integer inputs (e.g., UInt64 from simhash) and FixedSizeBinary
    inputs (e.g., from image_hash).

    Args:
        left (Integer or FixedSizeBinary Expression): The left fingerprint.
        right (Integer or FixedSizeBinary Expression): The right fingerprint (must match left's type).

    Returns:
        Expression (UInt32 Expression): Number of differing bits.
    """
    return Expression._call_builtin_scalar_fn("hamming_distance", left, right)


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
